package com.apple.springboot.controller;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.service.DataIngestionService;
import com.apple.springboot.service.EnrichmentPipelineService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api")
@Tag(name = "Data Extraction", description = "Data extraction, cleansing, enrichment, and storage API endpoints")
public class DataExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

    private final DataIngestionService dataIngestionService;
    private final EnrichmentPipelineService enrichmentPipelineService;

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;

    // List of statuses from DataIngestionService that indicate a fatal error before enrichment stage,
    // or that processing should stop before enrichment.
    private static final List<String> PRE_ENRICHMENT_TERMINAL_STATUSES = Arrays.asList(
            "S3_FILE_NOT_FOUND_OR_EMPTY", "INVALID_S3_URI", "S3_DOWNLOAD_FAILED",
            "CLASSPATH_FILE_NOT_FOUND", "EMPTY_PAYLOAD", "SOURCE_EMPTY_PAYLOAD",
            "INVALID_S3_URI", "S3_DOWNLOAD_FAILED", "CLASSPATH_FILE_NOT_FOUND",
            "EMPTY_CONTENT_LOADED", // Added from DataIngestionService
            "CLASSPATH_READ_ERROR", // Added from DataIngestionService
            "JSON_PARSE_ERROR", "EXTRACTION_ERROR", "EXTRACTION_FAILED",
            "CLEANSING_SERIALIZATION_ERROR", "ERROR_SERIALIZING_ITEMS",
            "FILE_PROCESSING_ERROR", "FILE_ERROR"
    );

    @Autowired
    public DataExtractionController(DataIngestionService dataIngestionService,
                                    EnrichmentPipelineService enrichmentPipelineService, CleansedDataStoreRepository cleansedDataStoreRepository, ObjectMapper objectMapper) {
        this.dataIngestionService = dataIngestionService;
        this.enrichmentPipelineService = enrichmentPipelineService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
    }

    @Operation(
            summary = "Extract, cleanse, enrich and store data from an uploaded JSON file",
            description = "Uploads a JSON file, cleanses it, and triggers enrichment. " +
                    "Enrichment is processed asynchronously in the background."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Request accepted - enrichment processing initiated in background"),
            @ApiResponse(responseCode = "400", description = "Bad request - file is empty or invalid"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/extract-cleanse-enrich-and-store")
    public ResponseEntity<String> extractCleanseEnrichAndStore(
            @Parameter(description = "The JSON file to upload.", required = true)
            @RequestParam("file") MultipartFile file) {

        if (file.isEmpty()) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("File is empty");
        }

        String sourceIdentifier = deriveSourceIdentifier(file);
        logger.info("Received POST request to process file '{}' with assigned sourceIdentifier: {}", file.getOriginalFilename(), sourceIdentifier);

        try {
            String content = new String(file.getBytes());
            CleansedDataStore cleansedDataEntry = dataIngestionService.ingestAndCleanseJsonPayload(content, sourceIdentifier);

            if (cleansedDataEntry != null && cleansedDataEntry.getId() != null) {
                ObjectNode responseJson = objectMapper.createObjectNode();
                responseJson.put("cleansedDataStoreId", cleansedDataEntry.getId().toString());
                responseJson.put("status", cleansedDataEntry.getStatus());

                // Trigger enrichment in a new thread
                handleIngestionAndTriggerEnrichment(cleansedDataEntry, sourceIdentifier);

                return ResponseEntity.status(HttpStatus.ACCEPTED).body(responseJson.toString());
            } else {
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Failed to process file.");
            }

        } catch (IOException e) {
            logger.error("Error reading uploaded file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error reading file");
        }
    }

    private String deriveSourceIdentifier(MultipartFile file) {
        String originalFilename = file.getOriginalFilename();
        String sanitizedFilename = (originalFilename != null) ? originalFilename.trim() : "";
        if (!sanitizedFilename.isEmpty()) {
            sanitizedFilename = sanitizedFilename.replace("\\", "/");
            int lastSlash = sanitizedFilename.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < sanitizedFilename.length() - 1) {
                sanitizedFilename = sanitizedFilename.substring(lastSlash + 1);
            }
        }
        if (sanitizedFilename.isEmpty()) {
            sanitizedFilename = "File-upload-" + UUID.randomUUID();
        }
        return "file-upload:" + sanitizedFilename;
    }

    @Operation(
            summary = "Ingest JSON payload",
            description = "Ingests and processes a JSON payload directly. " +
                    "The payload is cleansed and enrichment is triggered asynchronously in the background."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Request accepted - enrichment processing initiated in background"),
            @ApiResponse(responseCode = "200", description = "Source processed successfully but no content extracted for enrichment"),
            @ApiResponse(responseCode = "400", description = "Bad request - JSON payload is empty or invalid"),
            @ApiResponse(responseCode = "422", description = "Unprocessable entity - ingestion/cleansing failed"),
            @ApiResponse(responseCode = "417", description = "Expectation failed - cannot proceed to enrichment"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/ingest-json-payload")
    public ResponseEntity<String> ingestJsonPayload(
            @Parameter(description = "JSON payload to ingest and process", required = true)
            @RequestBody String jsonPayload) {
        String sourceIdentifier = "api-payload-" + UUID.randomUUID().toString();
        logger.info("Received POST request to process JSON payload. Assigned sourceIdentifier: {}", sourceIdentifier);
        CleansedDataStore cleansedDataEntry = null;

        try {
            if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
                logger.warn("Received empty JSON payload for identifier: {}", sourceIdentifier);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("JSON payload cannot be empty.");
            }

            cleansedDataEntry = dataIngestionService.ingestAndCleanseJsonPayload(jsonPayload, sourceIdentifier);
            return handleIngestionAndTriggerEnrichment(cleansedDataEntry, sourceIdentifier);

        } catch (IllegalArgumentException e) {
            logger.error("Invalid argument processing JSON payload for identifier: {}. Error: {}", sourceIdentifier, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body("Invalid argument for payload " + sourceIdentifier + ": " + e.getMessage());
        } catch (Exception e) {
            logger.error("Error processing JSON payload for identifier: {}. Error: {}", sourceIdentifier, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error processing JSON payload "+ sourceIdentifier + ": " + e.getMessage());
        }
    }
    @Operation(
            summary = "Get cleansed data status",
            description = "Retrieves the status of a cleansed data entry by its ID. " +
                    "Returns the status string or 'NOT_FOUND' if the ID doesn't exist."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Status retrieved successfully",
                    content = @Content(mediaType = "text/plain", schema = @Schema(type = "string"))),
            @ApiResponse(responseCode = "200", description = "Status not found", content = @Content)
    })
    @GetMapping("/cleansed-data-status/{id}")
    public String getStatus(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {
        return cleansedDataStoreRepository.findById(id)
                .map(store -> normalizeStatus(store.getStatus()))
                .orElse("NOT_FOUND");
    }

    private ResponseEntity<String> handleIngestionAndTriggerEnrichment(CleansedDataStore cleansedDataEntry, String identifierForLog) {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            String statusMsg = (cleansedDataEntry != null && cleansedDataEntry.getStatus() != null) ?
                    cleansedDataEntry.getStatus() : "Ingestion service returned null or ID-less CleansedDataStore.";
            logger.error("Data ingestion/cleansing failed for identifier: {}. Status: {}", identifierForLog, statusMsg);
            return ResponseEntity.status(HttpStatus.UNPROCESSABLE_ENTITY)
                    .body("Failed to process source: " + identifierForLog + ". Reason: " + statusMsg);
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        String currentStatus = cleansedDataEntry.getStatus();
        logger.info("Ingestion/cleansing complete for identifier: {}. CleansedDataStore ID: {}, Status: {}",
                identifierForLog, cleansedDataStoreId, currentStatus);

        if (currentStatus != null && PRE_ENRICHMENT_TERMINAL_STATUSES.stream().anyMatch(s -> s.equalsIgnoreCase(currentStatus))) {
            logger.warn("Ingestion/cleansing for {} ended with status: {}. No enrichment will be triggered. Details: {}", identifierForLog, currentStatus, cleansedDataEntry.getCleansingErrors());
            HttpStatus httpStatus = (Arrays.asList("S3_FILE_NOT_FOUND_OR_EMPTY", "CLASSPATH_FILE_NOT_FOUND", "EMPTY_PAYLOAD", "SOURCE_EMPTY_PAYLOAD", "EMPTY_CONTENT_LOADED").contains(currentStatus.toUpperCase()))
                    ? HttpStatus.NOT_FOUND
                    : HttpStatus.UNPROCESSABLE_ENTITY;
            return ResponseEntity.status(httpStatus)
                    .body("Problem with input source or cleansing for: " + identifierForLog + ". Status: " + currentStatus + ". Details: " + cleansedDataEntry.getCleansingErrors());
        }

        if ("NO_CONTENT_EXTRACTED".equalsIgnoreCase(currentStatus) || "PROCESSED_EMPTY_ITEMS".equalsIgnoreCase(currentStatus)) {
            logger.info("Processing for {} completed with status: {}. No content for enrichment. CleansedDataID: {}", identifierForLog, currentStatus, cleansedDataStoreId);
            return ResponseEntity.ok("Source processed. No content extracted for enrichment. CleansedDataID: " + cleansedDataStoreId + ", Status: " + currentStatus);
        }

        if (!"CLEANSED_PENDING_ENRICHMENT".equalsIgnoreCase(currentStatus)) {
            logger.warn("Ingestion/cleansing for {} completed with status: '{}', which is not 'CLEANSED_PENDING_ENRICHMENT'. No enrichment will be triggered. CleansedDataID: {}. Errors: {}",
                    identifierForLog, currentStatus, cleansedDataStoreId, cleansedDataEntry.getCleansingErrors());
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body("Data ingestion/cleansing for " + identifierForLog + " resulted in status '" + currentStatus + "', cannot proceed to enrichment.");
        }

        logger.info("Proceeding to enrichment for CleansedDataStore ID: {} from identifier: {}", cleansedDataStoreId, identifierForLog);

        final CleansedDataStore finalCleansedDataEntry = cleansedDataEntry;
        new Thread(() -> {
            try {
                logger.info("Initiating asynchronous enrichment for CleansedDataStore ID: {}", finalCleansedDataEntry.getId());
                enrichmentPipelineService.enrichAndStore(finalCleansedDataEntry);
            } catch (Exception e) {
                logger.error("Asynchronous enrichment failed for CleansedDataStore ID: {}. Error: {}", finalCleansedDataEntry.getId(), e.getMessage(), e);
            }
        }).start();

        String successMessage = String.format("Request for %s accepted. CleansedDataID: %s. Enrichment processing initiated in background. Current status: %s",
                identifierForLog, cleansedDataStoreId.toString(), normalizeStatus(currentStatus));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(successMessage);
    }

    private String normalizeStatus(String status) {
        if (status == null) {
            return null;
        }
        if ("PARTIALLY_ENRICHED".equalsIgnoreCase(status)) {
            return "ENRICHMENT COMPLETE";
        }
        return status;
    }
}
