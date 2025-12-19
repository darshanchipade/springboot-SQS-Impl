package com.apple.springboot.controller;

import com.apple.springboot.dto.CleansedItemRow;
import com.apple.springboot.dto.CleansedItemsResponse;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.service.DataIngestionService;
import com.apple.springboot.service.EnrichmentPipelineService;
import com.apple.springboot.service.EnrichmentReadService;
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
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Tag(name = "Data Extraction", description = "Data extraction, cleansing, enrichment, and storage API endpoints")
public class DataExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

    private final DataIngestionService dataIngestionService;
    private final EnrichmentPipelineService enrichmentPipelineService;
    private final EnrichmentReadService enrichmentReadService;

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
                                    EnrichmentPipelineService enrichmentPipelineService,
                                    EnrichmentReadService enrichmentReadService,
                                    CleansedDataStoreRepository cleansedDataStoreRepository,
                                    ObjectMapper objectMapper) {
        this.dataIngestionService = dataIngestionService;
        this.enrichmentPipelineService = enrichmentPipelineService;
        this.enrichmentReadService = enrichmentReadService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.objectMapper = objectMapper;
    }
    @GetMapping("/hello")
    public String extractCleanseEnrichAndStore(){

        return "Hello there";
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
            return handleCleansingOutcome(cleansedDataEntry, sourceIdentifier);
        } catch (IOException e) {
            logger.error("Error reading uploaded file", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Error reading file");
        }
    }
    private String deriveSourceIdentifier(MultipartFile file) {
        // Get the original filename, e.g., "internal-425-Test-1-US.json"
        String originalFilename = file.getOriginalFilename();
        String sanitizedFilename = (originalFilename != null) ? originalFilename.trim() : "";
        if (!sanitizedFilename.isEmpty()) {
            sanitizedFilename = sanitizedFilename.replace("\\", "/");
            int lastSlash = sanitizedFilename.lastIndexOf('/');
            if (lastSlash >= 0 && lastSlash < sanitizedFilename.length() - 1) {
                sanitizedFilename = sanitizedFilename.substring(lastSlash + 1);
            }
        }
        // If for some reason the filename is empty, fall back to a UUID
        if (sanitizedFilename.isEmpty()) {
            sanitizedFilename = "File-upload-" + UUID.randomUUID();
        }
        // Prepend "file-upload:" to the filename
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
            return handleCleansingOutcome(cleansedDataEntry, sourceIdentifier);

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

    @Operation(
            summary = "Fetch cleansed context snapshot",
            description = "Returns metadata and cached cleansed items for the provided cleansed data ID.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Context returned successfully"),
            @ApiResponse(responseCode = "404", description = "Cleansed record not found")
    })
    @GetMapping("/cleansed-context/{id}")
    public ResponseEntity<?> getCleansedContext(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {
        return enrichmentReadService
                .loadCleansedContext(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("CleansedDataStore not found for id " + id));
    }

    @Operation(
            summary = "Get cleansed items (original vs cleansed values)",
            description = "Returns normalized rows containing field name, original value, and cleansed value."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Items retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Cleansed data record not found")
    })
    @GetMapping("/cleansed-items/{id}")
    public ResponseEntity<?> getCleansedItems(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {

        Optional<CleansedDataStore> storeOpt = cleansedDataStoreRepository.findById(id);
        if (storeOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "CleansedDataStore not found for id " + id));
        }

        CleansedDataStore store = storeOpt.get();
        List<CleansedItemRow> rows = Optional.ofNullable(store.getCleansedItems())
                .orElse(List.of())
                .stream()
                .filter(Objects::nonNull)
                .map(new ItemRowMapper())
                .toList();

        return ResponseEntity.ok(new CleansedItemsResponse(rows));
    }

    @Operation(
            summary = "Get cleansed items stats (counts + duplicates)",
            description = "Returns counts for total extracted items, unique (sourcePath,field) pairs, and duplicate breakdown for a cleansedDataStoreId."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Stats retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Cleansed data record not found")
    })
    @GetMapping("/cleansed-items/{id}/stats")
    public ResponseEntity<?> getCleansedItemsStats(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {

        Optional<CleansedDataStore> storeOpt = cleansedDataStoreRepository.findById(id);
        if (storeOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "CleansedDataStore not found for id " + id));
        }

        CleansedDataStore store = storeOpt.get();
        List<Map<String, Object>> items = Optional.ofNullable(store.getCleansedItems()).orElse(List.of());
        int total = items.size();

        Map<String, Long> counts = items.stream()
                .filter(Objects::nonNull)
                .map(item -> {
                    Object up = item.get("usagePath");
                    Object sp = item.get("sourcePath");
                    Object fn = item.get("originalFieldName");
                    String base = (up instanceof String u && !u.isBlank())
                            ? u
                            : (sp instanceof String s ? s : null);
                    if (base == null || !(fn instanceof String)) {
                        return null;
                    }
                    return base + "::" + fn;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(Function.identity(), LinkedHashMap::new, Collectors.counting()));

        long uniqueKeys = counts.size();
        long duplicateKeys = counts.values().stream().filter(v -> v > 1).count();
        long duplicateItemsBeyondFirst = counts.values().stream().filter(v -> v > 1).mapToLong(v -> v - 1).sum();

        long deltaItems = items.stream()
                .filter(Objects::nonNull)
                .filter(m -> {
                    Object d = m.get("delta");
                    if (d instanceof Boolean b) return b;
                    if (d instanceof String s) return Boolean.parseBoolean(s);
                    return false;
                })
                .count();

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("cleansedDataStoreId", id.toString());
        body.put("sourceUri", store.getSourceUri());
        body.put("version", store.getVersion());
        body.put("totalExtractedItems", total);
        body.put("uniqueOccurrenceFieldPairs", uniqueKeys);
        body.put("duplicateKeys", duplicateKeys);
        body.put("duplicateItemsBeyondFirst", duplicateItemsBeyondFirst);
        body.put("deltaItems", deltaItems);
        body.put("expectedMaxEnrichedRowsForThisRun", uniqueKeys);

        return ResponseEntity.ok(body);
    }

    private static class ItemRowMapper implements Function<Map<String, Object>, CleansedItemRow> {
        @Override
        public CleansedItemRow apply(Map<String, Object> item) {
            CleansedItemRow row = new CleansedItemRow();
            String field = pickString(item.get("originalFieldName"));
            if (field == null) {
                field = pickString(item.get("itemType"));
            }
            if (field == null) {
                field = "Unknown field";
            }
            row.setField(field);

            row.setOriginal(pickString(item.get("originalValue")));

            String cleansed = pickString(item.get("cleansedValue"));
            if (cleansed == null) {
                cleansed = pickString(item.get("cleansedContent"));
            }
            row.setCleansed(cleansed);
            return row;
        }

        private String pickField(Map<String, Object> item) {
            Object field = item.get("field");
            if (field instanceof String && !((String) field).isBlank()) {
                return (String) field;
            }
            Object itemType = item.get("itemType");
            if (itemType instanceof String) return (String) itemType;
            return "Unknown field";
        }

        private String pickString(Object value) {
            return value instanceof String ? (String) value : null;
        }
    }

    @Operation(
            summary = "Resume ingestion from an existing cleansed record",
            description = "Replays the cleansing pipeline for an existing CleansedDataStore entry using the "
                    + "original stored payload, allowing downstream steps to continue without re-uploading JSON."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Resume request accepted"),
            @ApiResponse(responseCode = "404", description = "Cleansed record not found"),
            @ApiResponse(responseCode = "409", description = "Cleansed record cannot be replayed"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/ingestion/resume/{id}")
    public ResponseEntity<String> resumeIngestionFromSnapshot(
            @Parameter(description = "UUID of the cleansed data entry to replay", required = true)
            @PathVariable("id") UUID cleansedDataStoreId) {
        logger.info("Received request to resume ingestion pipeline for CleansedDataStore {}", cleansedDataStoreId);
        try {
            CleansedDataStore cleansedDataEntry = dataIngestionService.resumeFromExistingCleansedId(cleansedDataStoreId);
            return handleCleansingOutcome(cleansedDataEntry, "resume-" + cleansedDataStoreId);
        } catch (IllegalArgumentException e) {
            logger.warn("Unable to resume ingestion for {}: {}", cleansedDataStoreId, e.getMessage());
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("CleansedDataStore not found for id " + cleansedDataStoreId);
        } catch (IllegalStateException e) {
            logger.error("CleansedDataStore {} cannot be resumed: {}", cleansedDataStoreId, e.getMessage());
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Cannot resume cleansed record " + cleansedDataStoreId + ": " + e.getMessage());
        } catch (Exception e) {
            logger.error("Unexpected error while resuming CleansedDataStore {}: {}", cleansedDataStoreId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error resuming cleansed record " + cleansedDataStoreId + ": " + e.getMessage());
        }
    }
    @Operation(
            summary = "Fetch enrichment result",
            description = "Returns enriched content elements and metrics for the provided cleansed data ID.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Enrichment result returned successfully"),
            @ApiResponse(responseCode = "404", description = "Enrichment result not found")
    })
    @GetMapping("/enrichment/result/{id}")
    public ResponseEntity<?> getEnrichmentResult(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {
        return enrichmentReadService
                .loadEnrichmentResult(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body("Enrichment result not found for id " + id));
    }


    @Operation(
            summary = "Trigger enrichment for an existing cleansed data record",
            description = "Starts enrichment asynchronously for the provided cleansedDataStoreId. "
                    + "Only records in CLEANSED_PENDING_ENRICHMENT status can be processed."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Enrichment accepted"),
            @ApiResponse(responseCode = "404", description = "Cleansed data record not found"),
            @ApiResponse(responseCode = "409", description = "Record not ready for enrichment"),
            @ApiResponse(responseCode = "500", description = "Enrichment trigger failed")
    })
    @PostMapping("/enrichment/start/{id}")
    public ResponseEntity<String> startEnrichment(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable("id") UUID cleansedDataStoreId) {

        CleansedDataStore cleansedDataEntry = cleansedDataStoreRepository.findById(cleansedDataStoreId)
                .orElse(null);

        if (cleansedDataEntry == null) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body("CleansedDataStore not found for id " + cleansedDataStoreId);
        }

        if (!"CLEANSED_PENDING_ENRICHMENT".equalsIgnoreCase(cleansedDataEntry.getStatus())) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("CleansedDataStore " + cleansedDataStoreId + " is not awaiting enrichment. Current status: " + cleansedDataEntry.getStatus());
        }

        return triggerEnrichmentAsync(cleansedDataEntry, "manual-trigger");
    }

    private ResponseEntity<String> handleCleansingOutcome(CleansedDataStore cleansedDataEntry, String identifierForLog) {
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
            return buildJsonResponse(HttpStatus.OK, cleansedDataStoreId, currentStatus,
                    "Source processed. No content extracted for enrichment.");
        }

        if (!"CLEANSED_PENDING_ENRICHMENT".equalsIgnoreCase(currentStatus)) {
            logger.warn("Ingestion/cleansing for {} completed with status: '{}', which is not 'CLEANSED_PENDING_ENRICHMENT'. No enrichment will be triggered. CleansedDataID: {}. Errors: {}",
                    identifierForLog, currentStatus, cleansedDataStoreId, cleansedDataEntry.getCleansingErrors());
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body("Data ingestion/cleansing for " + identifierForLog + " resulted in status '" + currentStatus + "', cannot proceed to enrichment.");
        }

        logger.info("Cleansing complete for identifier: {}. CleansedDataStore ID: {} is awaiting enrichment trigger.", identifierForLog, cleansedDataStoreId);
        return buildJsonResponse(HttpStatus.ACCEPTED, cleansedDataStoreId, currentStatus,
                "Cleansing finished. Use POST /api/enrichment/start/" + cleansedDataStoreId + " to trigger enrichment.");
    }

    private ResponseEntity<String> triggerEnrichmentAsync(CleansedDataStore cleansedDataEntry, String triggerSource) {
        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Proceeding to enrichment for CleansedDataStore ID: {} via {}", cleansedDataStoreId, triggerSource);
        final CleansedDataStore finalCleansedDataEntry = cleansedDataEntry;
        new Thread(() -> {
            try {
                logger.info("Initiating asynchronous enrichment for CleansedDataStore ID: {}", finalCleansedDataEntry.getId());
                enrichmentPipelineService.enrichAndStore(finalCleansedDataEntry);
            } catch (Exception e) {
                logger.error("Asynchronous enrichment failed for CleansedDataStore ID: {}. Error: {}", finalCleansedDataEntry.getId(), e.getMessage(), e);
            }
        }).start();

        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body("Enrichment started for CleansedDataStore " + cleansedDataStoreId + ". Triggered by " + triggerSource + ".");
    }

    private ResponseEntity<String> buildJsonResponse(HttpStatus status, UUID cleansedDataStoreId, String pipelineStatus, String message) {
        ObjectNode node = objectMapper.createObjectNode();
        if (cleansedDataStoreId != null) {
            node.put("cleansedDataStoreId", cleansedDataStoreId.toString());
        }
        if (pipelineStatus != null) {
            node.put("status", normalizeStatus(pipelineStatus));
        }
        if (message != null) {
            node.put("message", message);
        }
        return ResponseEntity.status(status).body(node.toString());
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
