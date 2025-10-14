    package com.apple.springboot.controller;

    import com.apple.springboot.model.CleansedDataStore;
    import com.apple.springboot.repository.CleansedDataStoreRepository;
    import com.apple.springboot.service.DataIngestionService;
    import com.apple.springboot.service.EnrichmentPipelineService;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;
    import org.springframework.beans.factory.annotation.Autowired;
    import org.springframework.http.HttpStatus;
    import org.springframework.http.ResponseEntity;
    import org.springframework.web.bind.annotation.*;

    import java.io.IOException;
    import java.io.UncheckedIOException;
    import java.util.Arrays;
    import java.util.List;
    import java.util.UUID;

    @RestController
    @RequestMapping("/api")
    public class DataExtractionController {

        private static final Logger logger = LoggerFactory.getLogger(DataExtractionController.class);

        private final DataIngestionService dataIngestionService;
        private final EnrichmentPipelineService enrichmentPipelineService;

        private final CleansedDataStoreRepository cleansedDataStoreRepository;

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
                                        EnrichmentPipelineService enrichmentPipelineService, CleansedDataStoreRepository cleansedDataStoreRepository) {
            this.dataIngestionService = dataIngestionService;
            this.enrichmentPipelineService = enrichmentPipelineService;
            this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        }

        @GetMapping("/extract-cleanse-enrich-and-store")
        public ResponseEntity<String> extractCleanseEnrichAndStore(
                @RequestParam(name = "sourceUri", required = false) String sourceUriParam) {

            String identifierForServiceCall;
            String identifierForLog;

            if (sourceUriParam != null && !sourceUriParam.trim().isEmpty()) {
                identifierForServiceCall = sourceUriParam.trim();
                identifierForLog = identifierForServiceCall;
            } else {
                identifierForServiceCall = null; // DataIngestionService.ingestAndCleanseSingleFile() will use default
                identifierForLog = "default configured path";
            }

            logger.info("Received GET request to process from source: {}", identifierForLog);
            CleansedDataStore cleansedDataEntry = null;

            try {
                if (identifierForServiceCall != null) {
                    cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile(identifierForServiceCall);
                } else {
                    cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile();
                }
                // The ingestAndCleanseSingleFile method now returns CleansedDataStore directly or throws an exception.
                return handleIngestionAndTriggerEnrichment(cleansedDataEntry, identifierForLog);

            } catch (IOException | UncheckedIOException e) {
                logger.error("File I/O error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body("File I/O error for " + identifierForLog + ": " + e.getMessage());
            } catch (IllegalArgumentException e) {
                logger.error("Invalid argument for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                        .body("Invalid input for " + identifierForLog + ": " + e.getMessage());
            } catch (Exception e) {
                logger.error("Unexpected error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
                return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                        .body("Unexpected error for " + identifierForLog + ": " + e.getMessage());
            }
        }

    //        public void extractCleanseEnrichAndStore(
    //            @RequestParam(name = "sourceUri", required = false) String sourceUriParam) {
    //
    //        String identifierForServiceCall;
    //        String identifierForLog;
    //
    //        if (sourceUriParam != null && !sourceUriParam.trim().isEmpty()) {
    //            identifierForServiceCall = sourceUriParam.trim();
    //            identifierForLog = identifierForServiceCall;
    //        } else {
    //            identifierForServiceCall = null; // DataIngestionService.ingestAndCleanseSingleFile() will use default
    //            identifierForLog = "default configured path";
    //        }
    //
    //        logger.info("Received GET request to process from source: {}", identifierForLog);
    //        CleansedDataStore cleansedDataEntry = null;
    //
    //        try {
    //            if (identifierForServiceCall != null) {
    //                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile(identifierForServiceCall);
    //            } else {
    //                cleansedDataEntry = dataIngestionService.ingestAndCleanseSingleFile();
    //            }
    //            // The ingestAndCleanseSingleFile method now returns CleansedDataStore directly or throws an exception.
    //            return ;
    //
    //        } catch (UncheckedIOException e) {
    //            logger.error("File I/O error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
    //            return ;
    //        } catch (IllegalArgumentException e) {
    //            logger.error("Invalid argument for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
    //            return ;
    //        } catch (Exception e) {
    //            logger.error("Unexpected error for identifier: {}. Error: {}", identifierForLog, e.getMessage(), e);
    //            return ;
    //        }
    //    }

        @PostMapping("/ingest-json-payload")
        public ResponseEntity<String> ingestJsonPayload(@RequestBody String jsonPayload) {
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
        @GetMapping("/cleansed-data-status/{id}")
        public String getStatus(@PathVariable UUID id) {
            return cleansedDataStoreRepository.findById(id)
                    .map(CleansedDataStore::getStatus)
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
                        .body("Data ingestion/cleansing for " + identifierForLog + " resulted in status '" + currentStatus + "', cannot proceed to enrichment. Details: " + cleansedDataEntry.getCleansingErrors());
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
                    identifierForLog, cleansedDataStoreId.toString(), currentStatus);
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(successMessage);
        }
    }