package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.micrometer.common.lang.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

    private final RawDataStoreRepository rawDataStoreRepository;

    private ContentHashingService contentHashingService;

    // Treat these as extractable content fields
    private static final Set<String> CONTENT_FIELD_KEYS = Set.of("copy", "disclaimers", "text", "url");
    private static final Set<String> ICON_NODE_KEYS = Set.of("icon");
    private static final Set<String> ICON_META_KEYS = Set.of("_path", "_uri_path");
    private static final Set<String> IMAGE_META_KEYS = Set.of(
            "_path", "_model", "_id", "_uri1x_path", "_uri2x_path", "_uri_path"
    );
    private static final Pattern LOCALE_PATTERN = Pattern.compile("(?<=/)([a-z]{2})[-_]([A-Z]{2})(?=/|$)");
    private static final String USAGE_REF_DELIM = " ::ref:: ";
    private static final Map<String, String> EVENT_KEYWORDS = Map.of(
            "valentine", "Valentine day",
            "father's day", "Fathers day",
            "tax", "Tax day",
            "christmas", "Christmas",
            "mothers","Mothers day"
    );

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ObjectMapper objectMapper;
    private final ResourceLoader resourceLoader;
    private final String jsonFilePath;
    private final S3StorageService s3StorageService;
    private final String defaultS3BucketName;
    private final ContentHashRepository contentHashRepository;
    private final ContextUpdateService contextUpdateService;
    private final Set<String> excludedItemTypes;

    // Configurable behavior flags
    @Value("${app.ingestion.keep-blank-after-cleanse:true}")
    private boolean keepBlankAfterCleanse;

    // If true, bypass change filter and return all extracted items
    @Value("${app.ingestion.return-all-items:false}")
    private boolean returnAllItems;

    // If true, include contextHash in change detection
    @Value("${app.ingestion.consider-context-change:false}")
    private boolean considerContextChange;

    // If false, fall back to (sourcePath,itemType) when usagePath not found
    @Value("${app.ingestion.strict-usage-path:false}")
    private boolean strictUsagePath;

    // If true, log debug counters for found vs kept
    @Value("${app.ingestion.debug-counters:true}")
    private boolean debugCountersEnabled;

    // Copy cleansing patterns
    private static final Pattern NBSP_PATTERN = Pattern.compile("\\{%nbsp%\\}");
    private static final Pattern SOSUMI_PATTERN = Pattern.compile("\\{%sosumi type=\"[^\"]+\" metadata=\"\\d+\"%\\}");
    private static final Pattern BR_PATTERN = Pattern.compile("\\{%br%\\}");
    private static final Pattern URL_PATTERN = Pattern.compile(":\\s*\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)");
    // private static final Pattern WJ_PATTERN = Pattern.compile("\\(\\{%wj%\\}\\)");
    private static final Pattern NESTED_URL_PATTERN = Pattern.compile(":\\[\\s*:\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)\\]\\(\\{%wj%\\}\\)");
    private static final Pattern METADATA_PATTERN = Pattern.compile("\\{% metadata=\"\\d+\" %\\}");
    private static final String USAGE_JSON_DELIM = " ::json:: ";

    /**
     * Internal representation of an extracted content candidate before cleansing.
     * We intentionally defer cleansing so that on "delta" uploads we only cleanse the changed items.
     */
    private static class CandidateItem {
        final String sourcePath;
        final String itemType;
        final String usagePath;
        final String originalFieldName;
        final String model;
        final boolean skipEnrichment;
        final String originalValue;
        final String rawContentHash;
        final String contextFingerprintHash;
        final Envelope envelope;
        final Facets facets;
        final boolean isAnalytics;

        CandidateItem(String sourcePath,
                      String itemType,
                      String usagePath,
                      String originalFieldName,
                      String model,
                      boolean skipEnrichment,
                      String originalValue,
                      String rawContentHash,
                      String contextFingerprintHash,
                      Envelope envelope,
                      Facets facets,
                      boolean isAnalytics) {
            this.sourcePath = sourcePath;
            this.itemType = itemType;
            this.usagePath = usagePath;
            this.originalFieldName = originalFieldName;
            this.model = model;
            this.skipEnrichment = skipEnrichment;
            this.originalValue = originalValue;
            this.rawContentHash = rawContentHash;
            this.contextFingerprintHash = contextFingerprintHash;
            this.envelope = envelope;
            this.facets = facets;
            this.isAnalytics = isAnalytics;
        }
    }


    /**
     * Constructs the service with required repositories and config values.
     */
    public DataIngestionService(RawDataStoreRepository rawDataStoreRepository,
                                CleansedDataStoreRepository cleansedDataStoreRepository,
                                ContentHashRepository contentHashRepository,
                                ObjectMapper objectMapper,
                                ResourceLoader resourceLoader,
                                @Value("${app.json.file.path}") String jsonFilePath,
                                S3StorageService s3StorageService,
                                @Value("${app.s3.bucket-name}") String defaultS3BucketName,
                                ContextUpdateService contextUpdateService,
                                @Value("${app.ingestion.excluded-item-types:}") String excludedItemTypesProperty) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.contentHashRepository = contentHashRepository;
        this.contextUpdateService = contextUpdateService;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.jsonFilePath = jsonFilePath;
        this.s3StorageService = s3StorageService;
        this.defaultS3BucketName = defaultS3BucketName;
        this.excludedItemTypes = parseExcludedItemTypes(excludedItemTypesProperty);
    }


    private static class S3ObjectDetails {
        final String bucketName;
        final String fileKey;
        S3ObjectDetails(String bucketName, String fileKey) {
            this.bucketName = bucketName;
            this.fileKey = fileKey;
        }
    }
    private S3ObjectDetails parseS3Uri(String s3Uri) throws IllegalArgumentException {
        if (s3Uri == null || !s3Uri.startsWith("s3://")) {
            throw new IllegalArgumentException("Invalid S3 URI format: Must start with s3://. Received: " + s3Uri);
        }
        String pathPart = s3Uri.substring("s3://".length());
        if (pathPart.startsWith("///")) {
            String key = pathPart.substring(2);
            if (key.startsWith("/")) key = key.substring(1);
            if (key.isEmpty()) throw new IllegalArgumentException("Invalid S3 URI: Key is empty for default bucket URI " + s3Uri);
            return new S3ObjectDetails(defaultS3BucketName, key);
        } else {
            int firstSlashIndex = pathPart.indexOf('/');
            if (firstSlashIndex == -1 || firstSlashIndex == 0 || firstSlashIndex == pathPart.length() - 1) {
                throw new IllegalArgumentException("Invalid S3 URI format. Expected s3://bucket/key or s3:///key. Received: " + s3Uri);
            }
            String bucket = pathPart.substring(0, firstSlashIndex);
            String key = pathPart.substring(firstSlashIndex + 1);
            if (key.isEmpty()) throw new IllegalArgumentException("Invalid S3 URI: Key is empty for bucket '" + bucket + "' in URI " + s3Uri);
            return new S3ObjectDetails(bucket, key);
        }
    }

    /**
     * Loads JSON from configured path and processes it.
     * Handles both S3 and classpath loading strategies.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile() throws JsonProcessingException {
        try {
            return ingestAndCleanseSingleFile(this.jsonFilePath);
        } catch (IOException | RuntimeException e) {
            logger.error("Error processing default jsonFilePath '{}': {}. Creating error record.", this.jsonFilePath, e.getMessage(), e);
            String sourceUri;
            if (!this.jsonFilePath.startsWith("s3://") && !this.jsonFilePath.startsWith("classpath:")) {
                sourceUri = "classpath:" + this.jsonFilePath;
            } else {
                sourceUri = this.jsonFilePath;
            }
            String finalSourceUri = sourceUri;
            RawDataStore rawData = rawDataStoreRepository.findBySourceUri(finalSourceUri).orElseGet(() -> {
                RawDataStore newRawData = new RawDataStore();
                newRawData.setSourceUri(finalSourceUri);
                return newRawData;
            });
            if(rawData.getReceivedAt() == null) rawData.setReceivedAt(OffsetDateTime.now());
            rawData.setStatus("FILE_PROCESSING_ERROR");
            rawData.setRawContentText("Error processing file: " + e.getMessage());
            rawDataStoreRepository.save(rawData);
            return createAndSaveErrorCleansedDataStore(rawData, "FILE_ERROR", "ERROR FROM FILE","FileProcessingError: " + e.getMessage());
        }
    }

    /**
     * Handles ingestion for a specific identifier (s3:// or classpath:).
     * Performs validation, raw storage, deduplication, and cleansing.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseSingleFile(String identifier) throws IOException {
        logger.info("Starting ingestion and cleansing for identifier: {}", identifier);
        String rawJsonContent;
        String sourceUriForDb = identifier;
        RawDataStore rawDataStore = new RawDataStore();
        rawDataStore.setSourceUri(sourceUriForDb);
        rawDataStore.setReceivedAt(OffsetDateTime.now());

        if (identifier.startsWith("s3://")) {
            logger.info("Identifier is an S3 URI: {}", sourceUriForDb);
            try {
                S3ObjectDetails s3Details = parseS3Uri(sourceUriForDb);
                rawJsonContent = s3StorageService.downloadFileContent(s3Details.bucketName, s3Details.fileKey);
                //setting up source content type
                if (s3Details.fileKey.endsWith(".json")) {
                    rawDataStore.setSourceContentType("application/json");
                } else {
                    rawDataStore.setSourceContentType("application/octet-stream");
                }

                try {
                    JsonNode rootNode = objectMapper.readTree(rawJsonContent);
                    ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_model");
                    ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_path");
                    ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("copy");
                    rawDataStore.setSourceMetadata(objectMapper.writeValueAsString(rootNode));
                } catch (JsonProcessingException e) {
                    logger.error("Error processing JSON payload to extract metadata", e);
                }
                if (rawJsonContent == null) {
                    logger.warn("File not found or content is null from S3 URI: {}.", sourceUriForDb);
                    rawDataStore.setStatus("S3_FILE_NOT_FOUND_OR_EMPTY");
                    rawDataStoreRepository.save(rawDataStore);
                    return createAndSaveErrorCleansedDataStore(rawDataStore, "S3_FILE_NOT_FOUND_OR_EMPTY", "S3 ERROR", "S3Error: File not found or content was null at " + sourceUriForDb);
                }
                logger.info("Successfully downloaded content from S3 URI: {}", sourceUriForDb);
                rawDataStore.setStatus("S3_CONTENT_RECEIVED");
            } catch (IllegalArgumentException e) {
                logger.error("Invalid S3 URI format for identifier: '{}'. Error: {}", identifier, e.getMessage());
                rawDataStore.setStatus("INVALID_S3_URI");
                rawDataStore.setRawContentText("Invalid S3 URI: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "INVALID_S3_URI","INVALID S3", "InvalidS3URI: " + e.getMessage());
            } catch (Exception e) {
                logger.error("Failed to download S3 content for URI: '{}'. Error: {}", sourceUriForDb, e.getMessage(), e);
                rawDataStore.setStatus("S3_DOWNLOAD_FAILED");
                rawDataStore.setRawContentText("Error fetching S3 content: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "S3_DOWNLOAD_FAILED", "S3ERROR","S3DownloadError: " + e.getMessage());
            }
        } else {
            sourceUriForDb = identifier.startsWith("classpath:") ? identifier : "classpath:" + identifier;
            rawDataStore.setSourceUri(sourceUriForDb);
            rawDataStore.setSourceContentType("application/json");
            logger.info("Identifier is a classpath resource: {}", sourceUriForDb);
            Resource resource = resourceLoader.getResource(sourceUriForDb);
            if (!resource.exists()) {
                logger.error("Classpath resource not found: {}", sourceUriForDb);
                rawDataStore.setStatus("CLASSPATH_FILE_NOT_FOUND");
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "CLASSPATH_FILE_NOT_FOUND", "FILE NOT FOUND","ClasspathError: File not found at " + sourceUriForDb);
            }
            try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
                rawJsonContent = FileCopyUtils.copyToString(reader);
            } catch (IOException e) {
                logger.error("Failed to read raw JSON file from classpath: {}", sourceUriForDb, e);
                rawDataStore.setStatus("CLASSPATH_READ_ERROR");
                rawDataStore.setRawContentText("Error reading classpath file: " + e.getMessage());
                rawDataStoreRepository.save(rawDataStore);
                return createAndSaveErrorCleansedDataStore(rawDataStore, "CLASSPATH_READ_ERROR", "READ ERROR","IOError: " + e.getMessage());
            }
            try{
                JsonNode rootNode = objectMapper.readTree(rawJsonContent);
                ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_model");
                ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("_path");
                ((com.fasterxml.jackson.databind.node.ObjectNode) rootNode).remove("copy");
                rawDataStore.setSourceMetadata(objectMapper.writeValueAsString(rootNode));
            } catch (JsonProcessingException e) {
                logger.error("Error processing JSON payload to extract metadata", e);
            }
            rawDataStore.setStatus("CLASSPATH_CONTENT_RECEIVED");
        }

        if (rawJsonContent == null || rawJsonContent.trim().isEmpty()) {
            logger.warn("Raw JSON content from {} is effectively empty after loading.", sourceUriForDb);
            rawDataStore.setRawContentText(rawJsonContent);
            rawDataStore.setStatus("EMPTY_CONTENT_LOADED");
            RawDataStore savedForEmpty = rawDataStoreRepository.save(rawDataStore);
            return createAndSaveErrorCleansedDataStore(savedForEmpty, "EMPTY_CONTENT_LOADED","Error" ,"ContentError: Loaded content was empty.");
        }
        String contextJson = null;
        try {
            Resource contextResource = resourceLoader.getResource("classpath:context-config.json");
            if (contextResource.exists()) {
                try (Reader reader = new InputStreamReader(contextResource.getInputStream(), StandardCharsets.UTF_8)) {
                    contextJson = FileCopyUtils.copyToString(reader);
                }
            }
        } catch (IOException e) {
            logger.warn("Could not read context-config.json, continuing without it.", e);
        }
        String contentHash = calculateContentHash(rawJsonContent, contextJson);
        Optional<RawDataStore> existingRawDataOpt = rawDataStoreRepository.findBySourceUriAndContentHash(sourceUriForDb, contentHash);

        if (existingRawDataOpt.isPresent()) {
            RawDataStore existingRawData = existingRawDataOpt.get();
            logger.info("Duplicate content detected for source: {}. Using existing raw_data_id: {}", sourceUriForDb, existingRawData.getId());
            Optional<CleansedDataStore> existingCleansedData = cleansedDataStoreRepository.findByRawDataId(existingRawData.getId());
            if(existingCleansedData.isPresent()){
                logger.info("Found existing cleansed data for raw_data_id: {}. Skipping processing.", existingRawData.getId());
                return existingCleansedData.get();
            } else {
                logger.info("No existing cleansed data for raw_data_id: {}. Proceeding with processing.", existingRawData.getId());
                return processLoadedContent(rawJsonContent, existingRawData);
            }
        }

        rawDataStore.setRawContentText(rawJsonContent);
        rawDataStore.setRawContentBinary(rawJsonContent.getBytes(StandardCharsets.UTF_8));
        rawDataStore.setContentHash(contentHash);

        // Versioning logic
        Optional<RawDataStore> latestVersionOpt = rawDataStoreRepository.findTopBySourceUriOrderByVersionDesc(sourceUriForDb);
        if (!latestVersionOpt.isEmpty()) {
            RawDataStore latestVersion = latestVersionOpt.get();
            if (latestVersion.getLatest()) {
                latestVersion.setLatest(false);
                rawDataStoreRepository.save(latestVersion);
            }
            rawDataStore.setVersion(latestVersion.getVersion() + 1);
        } else {
            rawDataStore.setVersion(1);
        }

        RawDataStore savedRawDataStore = rawDataStoreRepository.save(rawDataStore);
        logger.info("Processed raw data with ID: {} for source: {} with status: {}", savedRawDataStore.getId(), sourceUriForDb, savedRawDataStore.getStatus());

        return processLoadedContent(rawJsonContent, savedRawDataStore);
    }
    @Transactional
    public CleansedDataStore ingestAndCleanseJsonPayload(String jsonPayload, String sourceIdentifier) throws JsonProcessingException {
        return ingestAndCleanseJsonPayload(jsonPayload, sourceIdentifier, null);
    }

    /**
     * Ingest a JSON payload and optionally override delta filtering.
     *
     * @param returnAllOverride when true, returns all items (full run) even if unchanged; when false, forces delta-only;
     *                          when null, uses configured app.ingestion.return-all-items behavior.
     */
    @Transactional
    public CleansedDataStore ingestAndCleanseJsonPayload(String jsonPayload,
                                                        String sourceIdentifier,
                                                        @Nullable Boolean returnAllOverride) throws JsonProcessingException {
        RawDataStore rawDataStore = findOrCreateRawDataStore(jsonPayload, sourceIdentifier);
        if (rawDataStore == null) {
            return null;
        }
        return processLoadedContent(jsonPayload, rawDataStore, null, returnAllOverride);
    }

    /**
     * Replays the cleansing pipeline for an existing CleansedDataStore record without re-uploading
     * the original payload. This allows downstream steps (cleansing/enrichment) to continue to use
     * the original source metadata (e.g. file-upload/S3 identifiers) instead of creating a new API
     * payload entry.
     *
     * @param cleansedDataStoreId existing CleansedDataStore identifier.
     * @return the refreshed CleansedDataStore entry after reprocessing.
     */
    @Transactional
    public CleansedDataStore resumeFromExistingCleansedId(UUID cleansedDataStoreId) throws JsonProcessingException {
        if (cleansedDataStoreId == null) {
            throw new IllegalArgumentException("CleansedDataStore id must be provided to resume processing.");
        }

        CleansedDataStore existingCleansed = cleansedDataStoreRepository.findById(cleansedDataStoreId)
                .orElseThrow(() -> new IllegalArgumentException("No CleansedDataStore found for id " + cleansedDataStoreId));

        UUID rawDataId = existingCleansed.getRawDataId();
        if (rawDataId == null) {
            throw new IllegalStateException("CleansedDataStore " + cleansedDataStoreId + " is missing a rawDataId reference.");
        }

        RawDataStore rawDataStore = rawDataStoreRepository.findById(rawDataId)
                .orElseThrow(() -> new IllegalStateException("RawDataStore " + rawDataId + " referenced by cleansed record " + cleansedDataStoreId + " was not found."));

        String rawJsonContent = rawDataStore.getRawContentText();
        if ((rawJsonContent == null || rawJsonContent.isBlank()) && rawDataStore.getRawContentBinary() != null) {
            rawJsonContent = new String(rawDataStore.getRawContentBinary(), StandardCharsets.UTF_8);
        }

        if (rawJsonContent == null || rawJsonContent.isBlank()) {
            throw new IllegalStateException("RawDataStore " + rawDataId + " does not contain stored JSON content to resume processing.");
        }

        logger.info("Resuming ingestion pipeline for CleansedDataStore {} using RawDataStore {}", cleansedDataStoreId, rawDataId);
        //return processLoadedContent(rawJsonContent, rawDataStore);
        return processLoadedContent(rawJsonContent, rawDataStore, existingCleansed, null);
    }

    private CleansedDataStore refreshExistingCleansedRecord(
            CleansedDataStore existing,
            List<Map<String, Object>> items,
            RawDataStore rawData) {

        existing.setCleansedItems(items);
        existing.setCleansedAt(OffsetDateTime.now());
        existing.setStatus("CLEANSED_PENDING_ENRICHMENT");
        existing.setVersion(rawData.getVersion());
        rawData.setStatus("CLEANSING_COMPLETE");
        rawDataStoreRepository.save(rawData);
        return cleansedDataStoreRepository.save(existing);
    }
    private RawDataStore findOrCreateRawDataStore(String jsonPayload, String sourceIdentifier) {
        if (jsonPayload == null || jsonPayload.trim().isEmpty()) {
            throw new IllegalArgumentException("JSON payload cannot be null or empty for sourceIdentifier " + sourceIdentifier);
        }

        String newContentHash = calculateContentHash(jsonPayload, null);

        Optional<RawDataStore> existingMatchingContent =
                rawDataStoreRepository.findBySourceUriAndContentHash(sourceIdentifier, newContentHash);
        if (existingMatchingContent.isPresent()) {
            RawDataStore existingRawData = existingMatchingContent.get();
            logger.info("Ingested content for sourceIdentifier '{}' matches existing raw data ID {}. Skipping new version.",
                    sourceIdentifier, existingRawData.getId());
            backfillPayloadColumnsIfMissing(existingRawData, jsonPayload);
            return rawDataStoreRepository.save(existingRawData);
        }

        RawDataStore rawDataStore = new RawDataStore();
        rawDataStore.setSourceUri(sourceIdentifier);
        rawDataStore.setReceivedAt(OffsetDateTime.now());
        rawDataStore.setStatus(resolveUploadStatus(sourceIdentifier));
        rawDataStore.setContentHash(newContentHash);
        rawDataStore.setLatest(true);
        populatePayloadColumns(rawDataStore, jsonPayload);

        Optional<RawDataStore> latestVersionOpt = rawDataStoreRepository.findTopBySourceUriOrderByVersionDesc(sourceIdentifier);
        if (latestVersionOpt.isPresent()) {
            RawDataStore latestVersion = latestVersionOpt.get();
            int nextVersion = Optional.ofNullable(latestVersion.getVersion()).orElse(0) + 1;
            rawDataStore.setVersion(nextVersion);
            if (Boolean.TRUE.equals(latestVersion.isLatest())) {
                latestVersion.setLatest(false);
                rawDataStoreRepository.save(latestVersion);
            }
        } else {
            rawDataStore.setVersion(1);
        }

        logger.info("No matching content found for sourceIdentifier {}. Persisting version {}.", sourceIdentifier, rawDataStore.getVersion());
        return rawDataStoreRepository.save(rawDataStore);
    }

    private void populatePayloadColumns(RawDataStore rawDataStore, String jsonPayload) {
        rawDataStore.setRawContentText(jsonPayload);
        rawDataStore.setRawContentBinary(jsonPayload.getBytes(StandardCharsets.UTF_8));
        rawDataStore.setSourceContentType("application/json");
        rawDataStore.setSourceMetadata(extractSourceMetadata(jsonPayload));
    }

    private void backfillPayloadColumnsIfMissing(RawDataStore rawDataStore, String jsonPayload) {
        if (rawDataStore.getRawContentText() == null) {
            rawDataStore.setRawContentText(jsonPayload);
        }
        if (rawDataStore.getRawContentBinary() == null) {
            rawDataStore.setRawContentBinary(jsonPayload.getBytes(StandardCharsets.UTF_8));
        }
        if (rawDataStore.getSourceContentType() == null) {
            rawDataStore.setSourceContentType("application/json");
        }
        if (rawDataStore.getSourceMetadata() == null) {
            rawDataStore.setSourceMetadata(extractSourceMetadata(jsonPayload));
        }
    }

    private String resolveUploadStatus(String sourceIdentifier) {
        if (sourceIdentifier == null) {
            return "API_PAYLOAD_RECEIVED";
        }
        if (sourceIdentifier.startsWith("file-upload:")) {
            return "FILE_UPLOAD_RECEIVED";
        }
        if (sourceIdentifier.startsWith("classpath:")) {
            return "CLASSPATH_CONTENT_RECEIVED";
        }
        return "API_PAYLOAD_RECEIVED";
    }

    private String extractSourceMetadata(String jsonPayload) {
        if (jsonPayload == null) {
            return null;
        }
        try {
            JsonNode rootNode = objectMapper.readTree(jsonPayload);
            if (rootNode instanceof ObjectNode objectNode) {
                objectNode.remove("_model");
                objectNode.remove("_path");
                objectNode.remove("copy");
                return objectMapper.writeValueAsString(objectNode);
            }
            return objectMapper.writeValueAsString(rootNode);
        } catch (JsonProcessingException e) {
            logger.error("Error processing JSON payload to extract metadata", e);
            return null;
        }
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent,
                                                   RawDataStore rawDataStore) throws JsonProcessingException {
        return processLoadedContent(rawJsonContent, rawDataStore, null, null);
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent,
                                                   RawDataStore rawDataStore,
                                                   @Nullable CleansedDataStore existingCleansed) throws JsonProcessingException {
        return processLoadedContent(rawJsonContent, rawDataStore, existingCleansed, null);
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent,
                                                   RawDataStore rawDataStore,
                                                   @Nullable CleansedDataStore existingCleansed,
                                                   @Nullable Boolean returnAllOverride) throws JsonProcessingException {
        try {
            JsonNode rootNode = objectMapper.readTree(rawJsonContent);
            List<CandidateItem> allExtractedItems = new ArrayList<>();
            Envelope rootEnvelope = new Envelope();
            rootEnvelope.setSourcePath(rawDataStore.getSourceUri());
            rootEnvelope.setUsagePath(rawDataStore.getSourceUri());
            rootEnvelope.setProvenance(new HashMap<>());

            IngestionCounters counters = new IngestionCounters();
            findAndExtractRecursive(rootNode, "#", "#", toUsageKey(rootEnvelope.getSourcePath(), "#"), rootEnvelope, new Facets(), allExtractedItems, counters);

            boolean shouldReturnAll = returnAllOverride != null ? returnAllOverride : returnAllItems;
            List<Map<String, Object>> itemsToProcess =
                    shouldReturnAll ? finalizeAllItems(allExtractedItems, counters) : filterForChangedItems(allExtractedItems, counters);

            if (debugCountersEnabled) {
                long keptPreFilterCopy = counters.copyKept;
                long keptPreFilterAnalytics = counters.analyticsKept;
                long keptPostFilterCopy =
                        itemsToProcess.stream().filter(i -> !isAnalyticsItem(i)).count();
                long keptPostFilterAnalytics =
                        itemsToProcess.stream().filter(this::isAnalyticsItem).count();
                logger.info("Counters: copy found={}, kept(after-cleanse)={}, kept(after-filter)={}; analytics found={}, kept(after-cleanse)={}, kept(after-filter)={}; blank-kept(copy)={}, blank-kept(analytics)={}",
                        counters.copyFound, keptPreFilterCopy, keptPostFilterCopy,
                        counters.analyticsFound, keptPreFilterAnalytics, keptPostFilterAnalytics,
                        counters.copyBlankKept, counters.analyticsBlankKept);
            }

            if (itemsToProcess.isEmpty()) {
                logger.info("No new or updated content to process for raw_data_id: {}", rawDataStore.getId());
                rawDataStore.setStatus("PROCESSED_NO_CHANGES");
                rawDataStoreRepository.save(rawDataStore);
                return existingCleansed != null
                        ? existingCleansed
                        : cleansedDataStoreRepository
                        .findTopByRawDataIdOrderByCleansedAtDesc(rawDataStore.getId())
                        .orElse(null);
            }

            return existingCleansed != null
                    ? refreshExistingCleansedRecord(existingCleansed, itemsToProcess, rawDataStore)
                    : createCleansedDataStore(itemsToProcess, rawDataStore);
        } catch (Exception e) {
            rawDataStore.setStatus("EXTRACTION_ERROR");
            rawDataStoreRepository.save(rawDataStore);
            if (existingCleansed != null) {
                throw e;
            }
            return createAndSaveErrorCleansedDataStore(
                    rawDataStore, "EXTRACTION_FAILED", "ExtractionError: " + e.getMessage(), "Failed");
        }
    }

    private List<Map<String, Object>> finalizeAllItems(List<CandidateItem> candidates, IngestionCounters counters) {
        List<Map<String, Object>> finalized = new ArrayList<>();
        for (CandidateItem candidate : candidates) {
            Map<String, Object> item = finalizeCandidate(candidate, counters);
            if (item == null) continue;
            finalized.add(item);
            upsertHashes(candidate, item, true);
        }
        return finalized;
    }

    private List<Map<String, Object>> filterForChangedItems(List<CandidateItem> candidates, IngestionCounters counters) {
        List<Map<String, Object>> changedItems = new ArrayList<>();
        for (CandidateItem candidate : candidates) {
            if (candidate.sourcePath == null || candidate.itemType == null) continue;

            Optional<ContentHash> existingHashOpt =
                    contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(candidate.sourcePath, candidate.itemType, candidate.usagePath);
            if (existingHashOpt.isEmpty() && !strictUsagePath) {
                existingHashOpt = contentHashRepository.findBySourcePathAndItemType(candidate.sourcePath, candidate.itemType);
                if (existingHashOpt.isPresent()) {
                    logger.debug("Change detection fallback matched by (sourcePath,itemType) without usagePath for {} :: {}", candidate.sourcePath, candidate.itemType);
                }
            }

            boolean rawChanged = existingHashOpt.isEmpty()
                    || existingHashOpt.get().getRawContentHash() == null
                    || !Objects.equals(existingHashOpt.get().getRawContentHash(), candidate.rawContentHash);

            boolean contextFingerprintChanged = considerContextChange && (existingHashOpt.isEmpty()
                    || existingHashOpt.get().getContextFingerprintHash() == null
                    || !Objects.equals(existingHashOpt.get().getContextFingerprintHash(), candidate.contextFingerprintHash));

            // If we can't safely decide (e.g., legacy rows without raw hashes), we treat as changed.
            boolean shouldProcess = rawChanged || contextFingerprintChanged;

            if (shouldProcess) {
                Map<String, Object> finalized = finalizeCandidate(candidate, counters);
                if (finalized != null) {
                    changedItems.add(finalized);
                    upsertHashes(candidate, finalized, true);
                } else {
                    // Still store raw hashes so next run can skip cleansing.
                    upsertHashes(candidate, null, false);
                }
            } else {
                // Unchanged: persist raw/context fingerprints (if missing) without re-cleansing.
                upsertHashes(candidate, null, false);
            }
        }
        return changedItems;
    }

    private void upsertHashes(CandidateItem candidate, @Nullable Map<String, Object> finalizedItem, boolean overwriteCleansedHashes) {
        Optional<ContentHash> existingHashOpt =
                contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(candidate.sourcePath, candidate.itemType, candidate.usagePath);
        if (existingHashOpt.isEmpty() && !strictUsagePath) {
            existingHashOpt = contentHashRepository.findBySourcePathAndItemType(candidate.sourcePath, candidate.itemType);
        }

        ContentHash hashToSave = existingHashOpt.orElse(new ContentHash(candidate.sourcePath, candidate.itemType, candidate.usagePath, "", null));

        // Always refresh raw + context fingerprint hashes
        hashToSave.setRawContentHash(candidate.rawContentHash);
        hashToSave.setContextFingerprintHash(candidate.contextFingerprintHash);

        if (finalizedItem != null && overwriteCleansedHashes) {
            String newContentHash = (String) finalizedItem.get("contentHash");
            String newContextHash = (String) finalizedItem.get("contextHash");
            if (newContentHash != null) {
                hashToSave.setContentHash(newContentHash);
            }
            hashToSave.setContextHash(newContextHash);
        }

        // contentHash column is non-nullable; if we created a new row without cleansing, ensure a value.
        if (hashToSave.getContentHash() == null || hashToSave.getContentHash().isBlank()) {
            // Prefer existing content hash if present; otherwise fall back to raw hash.
            hashToSave.setContentHash(existingHashOpt.map(ContentHash::getContentHash).orElse(candidate.rawContentHash));
        }
        contentHashRepository.save(hashToSave);
    }

    private CleansedDataStore createCleansedDataStore(List<Map<String, Object>> items, RawDataStore rawData) {
        CleansedDataStore cleansedDataStore = new CleansedDataStore();
        cleansedDataStore.setRawDataId(rawData.getId());
        cleansedDataStore.setSourceUri(rawData.getSourceUri());
        cleansedDataStore.setCleansedAt(OffsetDateTime.now());
        cleansedDataStore.setCleansedItems(items);
        cleansedDataStore.setVersion(rawData.getVersion());
        cleansedDataStore.setStatus("CLEANSED_PENDING_ENRICHMENT");
        rawData.setStatus("CLEANSING_COMPLETE");
        rawDataStoreRepository.save(rawData);
        return cleansedDataStoreRepository.save(cleansedDataStore);
    }

    private void findAndExtractRecursive(JsonNode currentNode,
                                         String parentFieldName,
                                         String jsonPath,
                                         String parentUsageKey,
                                         Envelope parentEnvelope,
                                         Facets parentFacets,
                                         List<CandidateItem> results,
                                         IngestionCounters counters) {
        if (currentNode.isObject()) {
            Envelope currentEnvelope = buildCurrentEnvelope(currentNode, parentEnvelope, jsonPath);
            Facets currentFacets = buildCurrentFacets(currentNode, parentFacets);
            String currentUsageKey = toUsageKey(currentEnvelope.getSourcePath(), jsonPath);

            // Section detection logic
            String modelName = currentEnvelope.getModel();
            if (modelName != null && modelName.endsWith("-section")) {
                String sectionPath = currentEnvelope.getSourcePath();
                currentFacets.put("sectionModel", modelName);
                currentFacets.put("sectionPath", sectionPath);

                if (sectionPath != null) {
                    String[] pathParts = sectionPath.split("/");
                    if (pathParts.length > 0) {
                        currentFacets.put("sectionKey", pathParts[pathParts.length - 1]);
                    }
                }
            }

            currentNode.fields().forEachRemaining(entry -> {
                String fieldKey = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                String childJsonPath = jsonPath + "/" + fieldKey;
                String fragmentUsageKey = currentUsageKey;
                String containerUsageKey = (parentUsageKey != null && !parentUsageKey.equals(fragmentUsageKey))
                        ? parentUsageKey
                        : null;
                String usagePath = (containerUsageKey != null)
                        ? containerUsageKey + USAGE_REF_DELIM + fragmentUsageKey
                        : fragmentUsageKey;

                if (CONTENT_FIELD_KEYS.contains(fieldKey)) {
                    if (fieldValue.isTextual()) {
                        currentEnvelope.setUsagePath(usagePath);
                        // If the key is "copy", use the parent's name. Otherwise, use the key itself.
                        String effectiveFieldName = fieldKey.equals("copy") ? parentFieldName : fieldKey;
                        processContentField(fieldValue.asText(), effectiveFieldName, currentEnvelope, currentFacets, results, counters, false);// copy object
                    } else if (fieldValue.isObject() && fieldValue.has("copy") && fieldValue.get("copy").isTextual()) {
                        Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope, childJsonPath);
                        contentEnv.setUsagePath(usagePath);
                        processContentField(fieldValue.get("copy").asText(), fieldKey, contentEnv, currentFacets, results, counters, false);

                        // text object
                    } else if (fieldValue.isObject() && fieldValue.has("text") && fieldValue.get("text").isTextual()) {
                        Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope, childJsonPath);
                        contentEnv.setUsagePath(usagePath);
                        processContentField(fieldValue.get("text").asText(), fieldKey, contentEnv, currentFacets, results, counters, false);

                        // url object (string value)
                    } else if (fieldValue.isObject() && fieldValue.has("url") && fieldValue.get("url").isTextual()) {
                        Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope, childJsonPath);
                        contentEnv.setUsagePath(usagePath);
                        processContentField(fieldValue.get("url").asText(), fieldKey, contentEnv, currentFacets, results, counters, false);
                    }
//                    else if (fieldValue.isObject() && fieldValue.has("copy") && fieldValue.get("copy").isTextual()) {
//                        currentEnvelope.setUsagePath(usagePath);
//                        // This is a nested content fragment. Use the outer envelope's field name (fieldKey).
//                        // If this object is under a URL, it would have been returned above. Here we are safe.
//                        processContentField(fieldValue.get("copy").asText(), fieldKey, currentEnvelope, currentFacets, results, counters, false);
//                    } else if (fieldValue.isObject() && fieldValue.has("text") && fieldValue.get("text").isTextual()) {
//                        currentEnvelope.setUsagePath(usagePath);
//                        processContentField(fieldValue.get("text").asText(), fieldKey, currentEnvelope, currentFacets, results, counters, false);
//                    } else if (fieldValue.isObject() && fieldValue.has("url") && fieldValue.get("url").isTextual()) {
//                        currentEnvelope.setUsagePath(usagePath);
//                        processContentField(fieldValue.get("url").asText(), fieldKey, currentEnvelope, currentFacets, results, counters, false);
                    //   }
                    else if (fieldValue.isArray()) {
                        if ("disclaimers".equals(fieldKey)) {
                            int groupIndex = 0;
                            for (JsonNode element : fieldValue) {
                                if (element.isObject() && element.has("items") && element.get("items").isArray()) {
                                    int itemIndex = 0;
                                    for (JsonNode item : element.get("items")) {
                                        if (item.isObject() && item.has("copy") && item.get("copy").isTextual()) {
                                            currentEnvelope.setUsagePath(usagePath);
                                            String uniqueFieldName = "disclaimer[" + groupIndex + "][" + itemIndex + "]";
                                            processContentField(item.get("copy").asText(), uniqueFieldName, currentEnvelope, currentFacets, results, counters, false);
                                        }
                                        itemIndex++;
                                    }
                                }
                                groupIndex++;
                            }
                        } else {
                            int idx = 0;
                            for (JsonNode element : fieldValue) {
                                if (element.isTextual()) {
                                    currentEnvelope.setUsagePath(usagePath);
                                    processContentField(element.asText(), fieldKey + "[" + idx + "]", currentEnvelope, currentFacets, results, counters, false);
                                } else if (element.isObject() && element.has("copy") && element.get("copy").isTextual()) {
                                    currentEnvelope.setUsagePath(usagePath);
                                    processContentField(element.get("copy").asText(), fieldKey + "[" + idx + "]", currentEnvelope, currentFacets, results, counters, false);
                                }
                                idx++;
                            }
                        }
                    } else {
                        currentEnvelope.setUsagePath(usagePath);
                        findAndExtractRecursive(fieldValue, fieldKey, childJsonPath, currentUsageKey, currentEnvelope, currentFacets, results, counters);
                    }
                } else if (fieldKey.toLowerCase().contains("analytics")) {
                    processAnalyticsNode(fieldValue, fieldKey, childJsonPath, currentEnvelope, currentFacets, results, counters);
                } else if (fieldValue.isObject() || fieldValue.isArray()) {
                    currentEnvelope.setUsagePath(usagePath);
                    findAndExtractRecursive(fieldValue, fieldKey, childJsonPath, currentUsageKey, currentEnvelope, currentFacets, results, counters);
                }
            });
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                JsonNode arrayElement = currentNode.get(i);
                String childJsonPath = jsonPath + "[" + i + "]";
                Facets newFacets = new Facets();
                newFacets.putAll(parentFacets);
                newFacets.put("sectionIndex", String.valueOf(i));
                // When recursing into an array, the parent field name is the one that pointed to the array
                findAndExtractRecursive(arrayElement, parentFieldName, childJsonPath, parentUsageKey, parentEnvelope, newFacets, results, counters);
            }
        }
    }


    private Envelope buildCurrentEnvelope(JsonNode currentNode, Envelope parentEnvelope, String jsonPath) {
        Envelope currentEnvelope = new Envelope();
        // Prefer explicit AEM _path; otherwise use the traversal jsonPath to keep items uniquely addressable.
        String path;
        if (currentNode != null && currentNode.has("_path") && currentNode.get("_path").isTextual()) {
            path = currentNode.get("_path").asText();
        } else if (jsonPath != null && !jsonPath.isBlank()) {
            path = jsonPath;
        } else {
            path = parentEnvelope != null ? parentEnvelope.getSourcePath() : null;
        }
        currentEnvelope.setSourcePath(path);
        currentEnvelope.setModel(currentNode.path("_model").asText(parentEnvelope.getModel()));
        currentEnvelope.setUsagePath(currentNode.path("_usagePath").asText(parentEnvelope.getUsagePath()));

        if (currentNode.has("_provenance")) {
            try {
                Map<String, String> provenanceMap = objectMapper.convertValue(currentNode.get("_provenance"), new com.fasterxml.jackson.core.type.TypeReference<Map<String, String>>() {});
                currentEnvelope.setProvenance(provenanceMap);
            } catch (IllegalArgumentException e) {
                logger.warn("Could not parse _provenance field as a Map for path: {}", path, e);
                currentEnvelope.setProvenance(parentEnvelope.getProvenance());
            }
        } else {
            currentEnvelope.setProvenance(parentEnvelope.getProvenance());
        }

        if (path != null) {
            Matcher matcher = LOCALE_PATTERN.matcher(path);
            //cover /en_US/, /en_US, /en-US/, and /en-US.
            if (matcher.find()) {
                String language = matcher.group(1);         // "en"
                String country  = matcher.group(2);         // "US"
                String locale   = language + "_" + country;
                currentEnvelope.setLocale(locale);
                currentEnvelope.setLanguage(language);
                currentEnvelope.setCountry(country);
            }
            List<String> pathSegments = Arrays.asList(path.split("/"));
            currentEnvelope.setPathHierarchy(pathSegments);
            if (!pathSegments.isEmpty()) {
                currentEnvelope.setSectionName(pathSegments.get(pathSegments.size() - 1));
            }
            currentEnvelope.setPathHierarchy(Arrays.asList(path.split("/")));
        }
        return currentEnvelope;
    }

    private Facets buildCurrentFacets(JsonNode currentNode, Facets parentFacets) {
        Facets currentFacets = new Facets();
        currentFacets.putAll(parentFacets);
        currentFacets.remove("copy"); // Remove generic copy if it exists
        currentFacets.remove("text"); // Avoid duplicating extracted text
        currentFacets.remove("url");  // Avoid duplicating extracted url text
        currentNode.fields().forEachRemaining(entry -> {
            JsonNode value = entry.getValue();
            String key = entry.getKey();
            if (value.isValueNode() && !key.startsWith("_")) {
                currentFacets.put(key, value.asText());
            } else if (ICON_NODE_KEYS.contains(key) && value.isObject()) {
                enrichFacetsWithIconProperties(value, key, currentFacets);
            } else if (isImageNodeKey(key) && value.isObject()) {
                enrichFacetsWithImageProperties(value, key, currentFacets);
            }
        });
        return currentFacets;
    }

    private void processContentField(String content, String fieldKey, Envelope envelope, Facets facets, List<CandidateItem> results, IngestionCounters counters, boolean isAnalytics) {
        boolean skipEnrichment = isExcluded(fieldKey);
        if (isAnalytics) counters.analyticsFound++; else counters.copyFound++;
        if (content == null) return;

        Envelope envCopy = copyEnvelope(envelope);
        Facets facetsCopy = new Facets(facets);

        String rawHash = calculateContentHash(content, null);
        String contextFingerprintHash = null;
        try {
            EnrichmentContext fingerprintContext = new EnrichmentContext(envCopy, facetsCopy);
            contextFingerprintHash = calculateContentHash(objectMapper.writeValueAsString(fingerprintContext), null);
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize context fingerprint for hashing", e);
        }

        results.add(new CandidateItem(
                envCopy.getSourcePath(),
                fieldKey,
                envCopy.getUsagePath(),
                fieldKey,
                envCopy.getModel(),
                skipEnrichment,
                content,
                rawHash,
                contextFingerprintHash,
                envCopy,
                facetsCopy,
                isAnalytics
        ));
    }

    private boolean isAnalyticsItem(CandidateItem item) {
        return item != null && item.isAnalytics;
    }

    private boolean isAnalyticsItem(Map<String, Object> item) {
        if (item == null) return false;
        Object type = item.get("itemType");
        return type instanceof String s && s.toLowerCase().contains("analytics");
    }

    private void processAnalyticsNode(JsonNode node,
                                      String fieldKey,
                                      String jsonPath,
                                      Envelope env,
                                      Facets facets,
                                      List<CandidateItem> results,
                                      IngestionCounters counters) {
        if (node == null || node.isNull()) return;

        if (node.isTextual() || node.isNumber() || node.isBoolean()) {
            processContentField(node.asText(), fieldKey, env, facets, results, counters, true);
            return;
        }

        if (node.isObject()) {
            Envelope analyticsEnvelope = buildCurrentEnvelope(node, env, jsonPath);
            JsonNode valueNode = node.get("value");
            if (valueNode != null && !valueNode.isNull()) {
                processContentField(valueNode.asText(), fieldKey, analyticsEnvelope, facets, results, counters, true);
            }
            for (String k : List.of("items","children","child")) {
                JsonNode arr = node.get(k);
                if (arr != null && arr.isArray()) {
                    int idx = 0;
                    for (JsonNode child : arr) {
                        processAnalyticsNode(child, fieldKey, jsonPath + "/" + k + "[" + idx + "]", analyticsEnvelope, facets, results, counters);
                        idx++;
                    }
                }
            }
            node.fields().forEachRemaining(e -> {
                if (!List.of("items","children","child","value").contains(e.getKey())) {
                    processAnalyticsNode(e.getValue(), fieldKey, jsonPath + "/" + e.getKey(), analyticsEnvelope, facets, results, counters);
                }
            });
            return;
        }

        if (node.isArray()) {
            int i = 0;
            for (JsonNode el : node) {
                if (el.isObject()) {
                    Envelope elEnv = buildCurrentEnvelope(el, env, jsonPath + "[" + i + "]");
                    String name = el.path("name").asText(null);
                    String val  = el.path("value").asText(null);
                    if (val != null && !val.isBlank()) {
                        String key = (name != null && !name.isBlank()) ? "analytics[" + name + "]" : "analytics[" + i + "]";
                        processContentField(val, key, elEnv, facets, results, counters, true);
                    }
                } else if (el.isTextual()) {
                    processContentField(el.asText(), "analytics[" + i + "]", env, facets, results, counters, true);
                }
                i++;
            }
        }
    }

    private String toUsageKey(String sourcePath, String jsonPath) {
        if (sourcePath == null || sourcePath.isBlank()) {
            return jsonPath;
        }
        if (jsonPath == null || jsonPath.isBlank()) {
            return sourcePath;
        }
        if (sourcePath.equals(jsonPath)) {
            return sourcePath;
        }
        return sourcePath + USAGE_JSON_DELIM + jsonPath;
    }

    private @Nullable Map<String, Object> finalizeCandidate(CandidateItem candidate, IngestionCounters counters) {
        if (candidate == null || candidate.originalValue == null) return null;

        String cleansedContent = cleanseCopyText(candidate.originalValue);

        boolean isBlankAfterCleanse = cleansedContent == null || cleansedContent.isBlank();
        boolean keep = cleansedContent != null && (!isBlankAfterCleanse || keepBlankAfterCleanse);
        if (!keep) {
            return null;
        }
        if (keep && isBlankAfterCleanse) {
            if (candidate.isAnalytics) counters.analyticsBlankKept++; else counters.copyBlankKept++;
        }

        // Enrichment facets derived from cleansed content should only be computed for items we actually process.
        Facets facets = new Facets(candidate.facets);
        facets.put("cleansedCopy", cleansedContent);
        String lowerCaseContent = cleansedContent.toLowerCase();
        for (Map.Entry<String, String> entry : EVENT_KEYWORDS.entrySet()) {
            if (lowerCaseContent.contains(entry.getKey())) {
                facets.put("eventType", entry.getValue());
                break;
            }
        }

        EnrichmentContext finalContext = new EnrichmentContext(candidate.envelope, facets);
        Map<String, Object> item = new HashMap<>();
        item.put("sourcePath", candidate.sourcePath);
        item.put("itemType", candidate.itemType);
        item.put("originalFieldName", candidate.originalFieldName);
        item.put("model", candidate.model);
        item.put("usagePath", candidate.usagePath);
        item.put("cleansedContent", cleansedContent);
        item.put("contentHash", calculateContentHash(cleansedContent, null));
        item.put("skipEnrichment", candidate.skipEnrichment);
        item.put("originalValue", candidate.originalValue);
        item.put("cleansedValue", cleansedContent);
        try {
            item.put("context", objectMapper.convertValue(finalContext, new com.fasterxml.jackson.core.type.TypeReference<>() {}));
            item.put("contextHash", calculateContentHash(objectMapper.writeValueAsString(finalContext), null));
        } catch (JsonProcessingException e) {
            logger.error("Failed to process context for hashing", e);
        }

        if (candidate.isAnalytics) counters.analyticsKept++; else counters.copyKept++;
        return item;
    }

    private Envelope copyEnvelope(Envelope src) {
        if (src == null) return null;
        Envelope copy = new Envelope();
        copy.setUsagePath(src.getUsagePath());
        copy.setSourcePath(src.getSourcePath());
        copy.setModel(src.getModel());
        copy.setLocale(src.getLocale());
        copy.setLanguage(src.getLanguage());
        copy.setCountry(src.getCountry());
        copy.setSectionName(src.getSectionName());
        if (src.getPathHierarchy() != null) {
            copy.setPathHierarchy(new ArrayList<>(src.getPathHierarchy()));
        }
        if (src.getProvenance() != null) {
            copy.setProvenance(new HashMap<>(src.getProvenance()));
        }
        return copy;
    }

    private String calculateContentHash(String content, String context) {
        if (content == null) return null; // Allow hashing of empty strings to differentiate from null
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(content.getBytes(StandardCharsets.UTF_8));
            if (context != null && !context.isEmpty()) {
                digest.update(context.getBytes(StandardCharsets.UTF_8));
            }
            byte[] encodedhash = digest.digest();
            return bytesToHex(encodedhash);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not found", e);
        }
    }

    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

    private CleansedDataStore createAndSaveErrorCleansedDataStore(RawDataStore rawDataStore, String cleansedStatus, String errorMessage, String specificErrorMessage) {
        CleansedDataStore errorCleansedData = new CleansedDataStore();
        if (rawDataStore != null) {
            errorCleansedData.setRawDataId(rawDataStore.getId());
            errorCleansedData.setSourceUri(rawDataStore.getSourceUri());
        }
        errorCleansedData.setCleansedItems(Collections.emptyList());
        errorCleansedData.setStatus(cleansedStatus);
        errorCleansedData.setCleansingErrors(Map.of("error", errorMessage));
        errorCleansedData.setCleansedAt(OffsetDateTime.now());
        return cleansedDataStoreRepository.save(errorCleansedData);
    }

    private static String cleanseCopyText(String text) {
        if (text == null) return null;
        String cleansed = text;
        // Targeted replacements based on requested patterns
        cleansed = NBSP_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = BR_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = SOSUMI_PATTERN.matcher(cleansed).replaceAll(" ");
        // Remove nested URL macro patterns first to avoid partial leftovers
        cleansed = NESTED_URL_PATTERN.matcher(cleansed).replaceAll(" ");
        // Then remove regular URL macro patterns
        cleansed = URL_PATTERN.matcher(cleansed).replaceAll(" ");
        // Remove metadata macro
        cleansed = METADATA_PATTERN.matcher(cleansed).replaceAll(" ");

        // Strip HTML tags
        cleansed = cleansed.replaceAll("<[^>]+?>", " ");
        // Normalize unicode NBSP if present
        cleansed = cleansed.replace('\u00A0', ' ');
        // Collapse whitespace
        cleansed = cleansed.replaceAll("\\s+", " ").trim();
        return cleansed;
    }

    private boolean isImageNodeKey(String key) {
        if (key == null) return false;
        String lower = key.toLowerCase();
        return lower.contains("image") || lower.contains("backgroundimage");
    }

    private void enrichFacetsWithIconProperties(JsonNode iconNode, String prefix, Facets targetFacets) {
        if (iconNode == null || iconNode.isNull()) return;
        iconNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            if (key.startsWith("_") && !ICON_META_KEYS.contains(key)) return;
            JsonNode value = entry.getValue();
            String facetKey = (prefix == null || prefix.isBlank()) ? key : prefix + "." + key;
            if (value.isValueNode()) {
                targetFacets.put(facetKey, value.asText());
            } else if (value.isObject()) {
                enrichFacetsWithIconProperties(value, facetKey, targetFacets);
            }
        });
    }

    private void enrichFacetsWithImageProperties(JsonNode imageNode, String prefix, Facets targetFacets) {
        if (imageNode == null || imageNode.isNull()) return;
        imageNode.fields().forEachRemaining(entry -> {
            String key = entry.getKey();
            if (key.startsWith("_") && !IMAGE_META_KEYS.contains(key)) return;
            JsonNode value = entry.getValue();
            String facetKey = (prefix == null || prefix.isBlank()) ? key : prefix + "." + key;
            if (value.isValueNode()) {
                targetFacets.put(facetKey, value.asText());
            } else if (value.isObject()) {
                enrichFacetsWithImageProperties(value, facetKey, targetFacets);
            } else if (value.isArray()) {
                int idx = 0;
                for (JsonNode element : value) {
                    String arrayKey = facetKey + "[" + idx + "]";
                    if (element.isValueNode()) {
                        targetFacets.put(arrayKey, element.asText());
                    } else if (element.isObject()) {
                        enrichFacetsWithImageProperties(element, arrayKey, targetFacets);
                    }
                    idx++;
                }
            }
        });
    }

    private Set<String> parseExcludedItemTypes(String property) {
        if (property == null || property.isBlank()) {
            return Set.of();
        }
        return Arrays.stream(property.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
    }

    private boolean isExcluded(String fieldKey) {
        if (fieldKey == null || excludedItemTypes.isEmpty()) {
            return false;
        }
        String lower = fieldKey.toLowerCase();
        return excludedItemTypes.stream().anyMatch(lower::startsWith);
    }

    private static class IngestionCounters {
        long copyFound = 0;
        long copyKept = 0;
        long analyticsFound = 0;
        long analyticsKept = 0;
        long copyBlankKept = 0;
        long analyticsBlankKept = 0;
    }
}