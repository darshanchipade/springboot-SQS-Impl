package com.apple.springboot.controller;

import com.apple.springboot.dto.CleansedContextMetadata;
import com.apple.springboot.dto.CleansedContextResponse;
import com.apple.springboot.dto.CleansedItemRow;
import com.apple.springboot.dto.CleansedItemsResponse;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.RawDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
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
import java.time.OffsetDateTime;
import java.util.*;

@RestController
@RequestMapping("/api")
@Tag(name = "Data Extraction", description = "Data extraction, cleansing, enrichment, and storage API endpoints")
public class DataExtractionController {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

    private final DataIngestionService dataIngestionService;
    private final EnrichmentPipelineService enrichmentPipelineService;

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final RawDataStoreRepository rawDataStoreRepository;
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
                                    CleansedDataStoreRepository cleansedDataStoreRepository,
                                    RawDataStoreRepository rawDataStoreRepository,
                                    ObjectMapper objectMapper) {
        this.dataIngestionService = dataIngestionService;
        this.enrichmentPipelineService = enrichmentPipelineService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.rawDataStoreRepository = rawDataStoreRepository;
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
            summary = "Get cleansed context metadata",
            description = "Returns metadata and context snapshot for a cleansed data entry."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Context retrieved successfully"),
            @ApiResponse(responseCode = "404", description = "Cleansed data record not found")
    })
    @GetMapping("/cleansed-context/{id}")
    public ResponseEntity<?> getCleansedContext(
            @Parameter(description = "UUID of the cleansed data entry", required = true)
            @PathVariable UUID id) {

        Optional<CleansedDataStore> storeOpt = cleansedDataStoreRepository.findById(id);
        if (storeOpt.isEmpty()) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "CleansedDataStore not found for id " + id));
        }

        CleansedDataStore store = storeOpt.get();
        CleansedContextMetadata metadata = buildContextMetadata(store);
        CleansedContextResponse response = new CleansedContextResponse(
                store.getId(),
                normalizeStatus(store.getStatus()),
                store.getCleansedAt(),
                metadata,
                store.getContext(),
                store.getCleansedItems() != null ? store.getCleansedItems().size() : 0
        );

        return ResponseEntity.ok(response);
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
            return buildJsonResponse(HttpStatus.OK, cleansedDataEntry, currentStatus,
                    "Source processed. No content extracted for enrichment.");
        }

        if (!"CLEANSED_PENDING_ENRICHMENT".equalsIgnoreCase(currentStatus)) {
            logger.warn("Ingestion/cleansing for {} completed with status: '{}', which is not 'CLEANSED_PENDING_ENRICHMENT'. No enrichment will be triggered. CleansedDataID: {}. Errors: {}",
                    identifierForLog, currentStatus, cleansedDataStoreId, cleansedDataEntry.getCleansingErrors());
            return ResponseEntity.status(HttpStatus.EXPECTATION_FAILED)
                    .body("Data ingestion/cleansing for " + identifierForLog + " resulted in status '" + currentStatus + "', cannot proceed to enrichment.");
        }

        logger.info("Cleansing complete for identifier: {}. CleansedDataStore ID: {} is awaiting enrichment trigger.", identifierForLog, cleansedDataStoreId);
        return buildJsonResponse(HttpStatus.ACCEPTED, cleansedDataEntry, currentStatus,
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

    private ResponseEntity<String> buildJsonResponse(HttpStatus status, CleansedDataStore cleansedDataEntry, String pipelineStatus, String message) {
        ObjectNode node = objectMapper.createObjectNode();
        if (cleansedDataEntry != null) {
            if (cleansedDataEntry.getId() != null) {
                node.put("cleansedDataStoreId", cleansedDataEntry.getId().toString());
            }
            if (cleansedDataEntry.getCleansedItems() != null) {
                node.set("cleansedItems", objectMapper.valueToTree(cleansedDataEntry.getCleansedItems()));
            }
            if (cleansedDataEntry.getCleansingErrors() != null && !cleansedDataEntry.getCleansingErrors().isEmpty()) {
                node.set("cleansingErrors", objectMapper.valueToTree(cleansedDataEntry.getCleansingErrors()));
            }
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

    private CleansedContextMetadata buildContextMetadata(CleansedDataStore store) {
        RawDataStore raw = null;
        if (store.getRawDataId() != null) {
            raw = rawDataStoreRepository.findById(store.getRawDataId()).orElse(null);
        }

        String sourceUri = store.getSourceUri();
        String fileName = deriveFileName(sourceUri);
        String sourceType = inferSourceType(sourceUri);
        String sourceLabel = describeSourceLabel(sourceType);
        String sourceIdentifier = deriveSourceIdentifier(sourceUri);

        long sizeInBytes = 0;
        OffsetDateTime uploadedAt = store.getCleansedAt();
        if (raw != null) {
            if (raw.getRawContentBinary() != null) {
                sizeInBytes = raw.getRawContentBinary().length;
            } else if (raw.getRawContentText() != null) {
                sizeInBytes = raw.getRawContentText().length();
            }
            if (raw.getReceivedAt() != null) {
                uploadedAt = raw.getReceivedAt();
            }
        }

        return new CleansedContextMetadata(
                store.getRawDataId(),
                sourceUri,
                sourceLabel,
                fileName,
                sizeInBytes,
                uploadedAt,
                sourceType,
                sourceIdentifier
        );
    }

    private String deriveFileName(String sourceUri) {
        if (sourceUri == null || sourceUri.isBlank()) {
            return "Unknown dataset";
        }
        String normalized = sourceUri.replace("\\", "/");
        int lastSlash = normalized.lastIndexOf('/');
        if (lastSlash >= 0 && lastSlash < normalized.length() - 1) {
            return normalized.substring(lastSlash + 1);
        }
        return normalized;
    }

    private String deriveSourceIdentifier(String sourceUri) {
        if (sourceUri == null) {
            return "unknown";
        }
        return sourceUri.trim();
    }

    private String inferSourceType(String sourceUri) {
        if (sourceUri == null) {
            return "unknown";
        }
        String lower = sourceUri.toLowerCase(Locale.ROOT);
        if (lower.startsWith("s3://")) {
            return "s3";
        }
        if (lower.startsWith("classpath:")) {
            return "classpath";
        }
        if (lower.startsWith("file-upload:") || lower.startsWith("local:")) {
            return "file";
        }
        if (lower.startsWith("api-payload")) {
            return "api";
        }
        return "unknown";
    }

    private String describeSourceLabel(String type) {
        return switch (type) {
            case "s3" -> "S3 source";
            case "classpath" -> "Classpath resource";
            case "file" -> "Local upload";
            case "api" -> "API payload";
            default -> "Unknown source";
        };
    }

    private class ItemRowMapper implements java.util.function.Function<Map<String, Object>, CleansedItemRow> {

        private final List<String> FIELD_KEYS = List.of(
                "originalFieldName",
                "fieldName",
                "field",
                "label",
                "key",
                "itemType",
                "name"
        );

        private final List<String> ORIGINAL_KEYS = List.of(
                "originalValue",
                "rawValue",
                "sourceValue",
                "before",
                "input",
                "valueBefore",
                "value",
                "copy",
                "text",
                "content"
        );

        private final List<String> CLEANSED_KEYS = List.of(
                "cleansedValue",
                "cleanedValue",
                "normalizedValue",
                "after",
                "output",
                "valueAfter",
                "value",
                "cleansedContent",
                "cleansedCopy",
                "text"
        );

        @Override
        public CleansedItemRow apply(Map<String, Object> item) {
            String field = findString(item, FIELD_KEYS);
            if (field == null || field.isBlank()) {
                field = "Field";
            }
            String finalField = field;
            String itemId = Optional.ofNullable(item.get("id"))
                    .map(Object::toString)
                    .orElseGet(() -> finalField + "-" + UUID.randomUUID());

            String original = findValueAcrossSources(item, ORIGINAL_KEYS, field);
            String cleansed = findValueAcrossSources(item, CLEANSED_KEYS, field);

            return new CleansedItemRow(
                    itemId,
                    field,
                    original,
                    cleansed
            );
        }

        private String findValueAcrossSources(Map<String, Object> item, List<String> keys, String forbiddenValue) {
            String direct = findString(item, keys, forbiddenValue);
            if (direct != null) {
                return direct;
            }
            Object contextObj = item.get("context");
            if (contextObj instanceof Map<?, ?> context) {
                @SuppressWarnings("unchecked")
                Map<String, Object> contextMap = (Map<String, Object>) context;
                String fromContext = findString(contextMap, keys, forbiddenValue);
                if (fromContext != null) {
                    return fromContext;
                }
                Object facetsObj = contextMap.get("facets");
                if (facetsObj instanceof Map<?, ?> facets) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> facetsMap = (Map<String, Object>) facets;
                    String fromFacets = findString(facetsMap, keys, forbiddenValue);
                    if (fromFacets != null) {
                        return fromFacets;
                    }
                }
            }
            Object facetsObj = item.get("facets");
            if (facetsObj instanceof Map<?, ?> facets) {
                @SuppressWarnings("unchecked")
                Map<String, Object> facetsMap = (Map<String, Object>) facets;
                return findString(facetsMap, keys, forbiddenValue);
            }
            return null;
        }

        private String findString(Map<String, Object> source, List<String> keys) {
            return findString(source, keys, null);
        }

        private String findString(Map<String, Object> source, List<String> keys, String forbiddenValue) {
            for (String key : keys) {
                if (key == null) continue;
                Object candidate = source.get(key);
                if (candidate instanceof String str && !str.isBlank()) {
                    if (forbiddenValue == null || !str.trim().equalsIgnoreCase(forbiddenValue.trim())) {
                        return str;
                    }
                }
            }
            return null;
        }
    }
}
