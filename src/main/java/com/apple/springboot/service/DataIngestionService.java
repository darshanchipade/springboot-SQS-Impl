package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
@Service
public class DataIngestionService {

    private static final Logger logger = LoggerFactory.getLogger(DataIngestionService.class);

    private final RawDataStoreRepository rawDataStoreRepository;

    private ContentHashingService contentHashingService;

    // Treat these as extractable content fields
    private static final Set<String> CONTENT_FIELD_KEYS = Set.of("copy", "disclaimers", "text", "url");
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
                                ContextUpdateService contextUpdateService) {
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.contentHashRepository = contentHashRepository;
        this.contextUpdateService = contextUpdateService;
        this.objectMapper = objectMapper;
        this.resourceLoader = resourceLoader;
        this.jsonFilePath = jsonFilePath;
        this.s3StorageService = s3StorageService;
        this.defaultS3BucketName = defaultS3BucketName;
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
    public CleansedDataStore ingestAndCleanseJsonPayload(String jsonPayload, String sourceIdentifier) {
        RawDataStore rawDataStore = findOrCreateRawDataStore(jsonPayload, sourceIdentifier);
        if (rawDataStore == null) {
            return null;
        }
        return processLoadedContent(jsonPayload, rawDataStore);
    }

    private RawDataStore findOrCreateRawDataStore(String jsonPayload, String sourceIdentifier) {
        String newContentHash = calculateContentHash(jsonPayload, null);
        Optional<RawDataStore> latestVersionOpt = rawDataStoreRepository.findTopBySourceUriOrderByVersionDesc(sourceIdentifier);

        if (latestVersionOpt.isPresent()) {
            RawDataStore rawDataStore = latestVersionOpt.get();
            if (Objects.equals(rawDataStore.getContentHash(), newContentHash)) {
                logger.info("Ingested content for sourceIdentifier '{}' has not changed. Skipping processing.", sourceIdentifier);
                return null;
            }
            logger.info("Found existing RawDataStore with ID {}. Content has changed, updating record.", rawDataStore.getId());
            rawDataStore.setReceivedAt(OffsetDateTime.now());
            rawDataStore.setRawContentText(jsonPayload);
            rawDataStore.setContentHash(newContentHash);
            return rawDataStoreRepository.save(rawDataStore);
        } else {
            RawDataStore rawDataStore = new RawDataStore();
            rawDataStore.setSourceUri(sourceIdentifier);
            rawDataStore.setVersion(1);
            rawDataStore.setReceivedAt(OffsetDateTime.now());
            rawDataStore.setRawContentText(jsonPayload);
            rawDataStore.setContentHash(newContentHash);
            rawDataStore.setStatus("API_PAYLOAD_RECEIVED");
            logger.info("No existing RawDataStore found for sourceIdentifier {}. Creating a new one.", sourceIdentifier);
            return rawDataStoreRepository.save(rawDataStore);
        }
    }

    private CleansedDataStore processLoadedContent(String rawJsonContent, RawDataStore associatedRawDataStore) {
        try {
            JsonNode rootNode = objectMapper.readTree(rawJsonContent);
            List<Map<String, Object>> allExtractedItems = new ArrayList<>();

            Envelope rootEnvelope = new Envelope();
            rootEnvelope.setSourcePath(associatedRawDataStore.getSourceUri());
            rootEnvelope.setUsagePath(associatedRawDataStore.getSourceUri());
            rootEnvelope.setProvenance(new HashMap<>());

            IngestionCounters counters = new IngestionCounters();
            findAndExtractRecursive(rootNode, "#", rootEnvelope, new Facets(), allExtractedItems, counters);

            List<Map<String, Object>> itemsToProcess = returnAllItems ? allExtractedItems : filterForChangedItems(allExtractedItems);

            if (debugCountersEnabled) {
                long keptPreFilterCopy = counters.copyKept;
                long keptPreFilterAnalytics = counters.analyticsKept;
                long keptPostFilterCopy = itemsToProcess.stream().filter(i -> !isAnalyticsItem(i)).count();
                long keptPostFilterAnalytics = itemsToProcess.stream().filter(this::isAnalyticsItem).count();
                logger.info("Counters: copy found={}, kept(after-cleanse)={}, kept(after-filter)={}; analytics found={}, kept(after-cleanse)={}, kept(after-filter)={}; blank-kept(copy)={}, blank-kept(analytics)={}",
                        counters.copyFound, keptPreFilterCopy, keptPostFilterCopy,
                        counters.analyticsFound, keptPreFilterAnalytics, keptPostFilterAnalytics,
                        counters.copyBlankKept, counters.analyticsBlankKept);
            }

            if (itemsToProcess.isEmpty()) {
                logger.info("No new or updated content to process for raw_data_id: {}", associatedRawDataStore.getId());
                associatedRawDataStore.setStatus("PROCESSED_NO_CHANGES");
                rawDataStoreRepository.save(associatedRawDataStore);
                return cleansedDataStoreRepository.findTopByRawDataIdOrderByCleansedAtDesc(associatedRawDataStore.getId()).orElse(null);
            }

            return createCleansedDataStore(itemsToProcess, associatedRawDataStore);

        } catch (Exception e) {
            logger.error("Error during content processing for raw data ID: {}. Error: {}", associatedRawDataStore.getId(), e.getMessage(), e);
            associatedRawDataStore.setStatus("EXTRACTION_ERROR");
            rawDataStoreRepository.save(associatedRawDataStore);
            return createAndSaveErrorCleansedDataStore(associatedRawDataStore, "EXTRACTION_FAILED", "ExtractionError: " + e.getMessage(),"Failed");
        }
    }

    private List<Map<String, Object>> filterForChangedItems(List<Map<String, Object>> allItems) {
        List<Map<String, Object>> changedItems = new ArrayList<>();
        for (Map<String, Object> item : allItems) {
            String sourcePath = (String) item.get("sourcePath");
            String itemType = (String) item.get("itemType");
            String usagePath = (String) item.get("usagePath");
            String newContentHash = (String) item.get("contentHash");
            String newContextHash = (String) item.get("contextHash");

            if (sourcePath == null || itemType == null) continue;

            boolean hasUsagePath = usagePath != null && !usagePath.isBlank();
            Optional<ContentHash> existingHashOpt = hasUsagePath
                    ? contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(sourcePath, itemType, usagePath)
                    : Optional.empty();
            // Only fallback to (sourcePath,itemType) when usagePath is missing AND fallback allowed
            if (!hasUsagePath && !strictUsagePath && existingHashOpt.isEmpty()) {
                existingHashOpt = contentHashRepository.findBySourcePathAndItemType(sourcePath, itemType);
                if (existingHashOpt.isPresent()) {
                    logger.debug("Change detection fallback matched by (sourcePath,itemType) (usagePath missing) for {} :: {}", sourcePath, itemType);
                }
            }

            boolean contentChanged = existingHashOpt.isEmpty()
                    || !Objects.equals(existingHashOpt.get().getContentHash(), newContentHash);
            boolean contextChanged = considerContextChange && (existingHashOpt.isEmpty()
                    || !Objects.equals(existingHashOpt.get().getContextHash(), newContextHash));

            if (existingHashOpt.isEmpty() || contentChanged || contextChanged) {
                changedItems.add(item);
            }
            // Always persist latest observed hashes for this key (respect usagePath when provided)
            ContentHash hashToSave = existingHashOpt.orElse(new ContentHash(sourcePath, itemType, usagePath, null, null));
            hashToSave.setContentHash(newContentHash);
            hashToSave.setContextHash(newContextHash);
            contentHashRepository.save(hashToSave);
        }
        return changedItems;
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

    private void findAndExtractRecursive(JsonNode currentNode, String parentFieldName, Envelope parentEnvelope, Facets parentFacets, List<Map<String, Object>> results, IngestionCounters counters) {
        if (currentNode.isObject()) {
            Envelope currentEnvelope = buildCurrentEnvelope(currentNode, parentEnvelope);
            Facets currentFacets = buildCurrentFacets(currentNode, parentFacets);

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
                String fragmentPath = currentEnvelope.getSourcePath();
                String containerPath = (parentEnvelope != null
                        && parentEnvelope.getSourcePath() != null
                        && !parentEnvelope.getSourcePath().equals(fragmentPath))
                        ? parentEnvelope.getSourcePath()
                        : null;
                String usagePath = (containerPath != null)
                        ? containerPath + USAGE_REF_DELIM + fragmentPath
                        : fragmentPath;

                if (CONTENT_FIELD_KEYS.contains(fieldKey)) {
                    if (fieldValue.isTextual()) {
                        currentEnvelope.setUsagePath(usagePath);
                        // If the key is "copy", use the parent's name. Otherwise, use the key itself.
                        String effectiveFieldName = fieldKey.equals("copy") ? parentFieldName : fieldKey;
                        processContentField(fieldValue.asText(), effectiveFieldName, currentEnvelope, currentFacets, results, counters, false);// copy object
                } else if (fieldValue.isObject() && fieldValue.has("copy") && fieldValue.get("copy").isTextual()) {
                    Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope);
                    contentEnv.setUsagePath(usagePath);
                    processContentField(fieldValue.get("copy").asText(), fieldKey, contentEnv, currentFacets, results, counters, false);

            // text object
                } else if (fieldValue.isObject() && fieldValue.has("text") && fieldValue.get("text").isTextual()) {
                    Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope);
                    contentEnv.setUsagePath(usagePath);
                    processContentField(fieldValue.get("text").asText(), fieldKey, contentEnv, currentFacets, results, counters, false);

                // url object (string value)
                } else if (fieldValue.isObject() && fieldValue.has("url") && fieldValue.get("url").isTextual()) {
                    Envelope contentEnv = buildCurrentEnvelope(fieldValue, currentEnvelope);
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
                        findAndExtractRecursive(fieldValue, fieldKey, currentEnvelope, currentFacets, results, counters);
                    }
                } else if (fieldKey.toLowerCase().contains("analytics")) {
                    processAnalyticsNode(fieldValue, fieldKey, currentEnvelope, currentFacets, results, counters);
                } else if (fieldValue.isObject() || fieldValue.isArray()) {
                    currentEnvelope.setUsagePath(usagePath);
                    findAndExtractRecursive(fieldValue, fieldKey, currentEnvelope, currentFacets, results, counters);
                }
            });
        } else if (currentNode.isArray()) {
            for (int i = 0; i < currentNode.size(); i++) {
                JsonNode arrayElement = currentNode.get(i);
                Facets newFacets = new Facets();
                newFacets.putAll(parentFacets);
                newFacets.put("sectionIndex", String.valueOf(i));
                // When recursing into an array, the parent field name is the one that pointed to the array
                findAndExtractRecursive(arrayElement, parentFieldName, parentEnvelope, newFacets, results, counters);
            }
        }
    }


    private Envelope buildCurrentEnvelope(JsonNode currentNode, Envelope parentEnvelope) {
        Envelope currentEnvelope = new Envelope();
        String path = currentNode.has("_path") ? currentNode.get("_path").asText(parentEnvelope.getSourcePath()) : parentEnvelope.getSourcePath();
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
            if (entry.getValue().isValueNode() && !entry.getKey().startsWith("_")) {
                currentFacets.put(entry.getKey(), entry.getValue().asText());
            }
        });
        return currentFacets;
    }

    private void processContentField(String content, String fieldKey, Envelope envelope, Facets facets, List<Map<String, Object>> results, IngestionCounters counters, boolean isAnalytics) {
        String cleansedContent = cleanseCopyText(content);
        if (isAnalytics) counters.analyticsFound++; else counters.copyFound++;

        boolean isBlankAfterCleanse = cleansedContent == null || cleansedContent.isBlank();
        boolean keep = cleansedContent != null && (!isBlankAfterCleanse || keepBlankAfterCleanse);
        if (keep && isBlankAfterCleanse) {
            if (isAnalytics) counters.analyticsBlankKept++; else counters.copyBlankKept++;
        }

        if (keep) {
            //facets.putAll(facets);
            facets.put("cleansedCopy", cleansedContent);

            String lowerCaseContent = cleansedContent.toLowerCase();
            for (Map.Entry<String, String> entry : EVENT_KEYWORDS.entrySet()) {
                if (lowerCaseContent.contains(entry.getKey())) {
                    facets.put("eventType", entry.getValue());
                    break;
                }
            }
            EnrichmentContext finalContext = new EnrichmentContext(envelope, facets);
            Map<String, Object> item = new HashMap<>();
            item.put("sourcePath", envelope.getSourcePath());
            item.put("itemType", fieldKey);
            item.put("originalFieldName", fieldKey);
            item.put("model", envelope.getModel());
            item.put("usagePath", envelope.getUsagePath());
            item.put("cleansedContent", cleansedContent);
            item.put("contentHash", calculateContentHash(cleansedContent, null));
            try {
                item.put("context", objectMapper.convertValue(finalContext, new com.fasterxml.jackson.core.type.TypeReference<>() {}));
                // Ensure stable property and map ordering when hashing
                item.put("contextHash", calculateContentHash(objectMapper.writeValueAsString(finalContext), null));
            } catch (JsonProcessingException e) {
                logger.error("Failed to process context for hashing", e);
            }
            results.add(item);
            if (isAnalytics) counters.analyticsKept++; else counters.copyKept++;
        }
    }

    private boolean isAnalyticsItem(Map<String, Object> item) {
        String type = (String) item.get("itemType");
        return type != null && type.toLowerCase().contains("analytics");
    }

    private void processAnalyticsNode(JsonNode node, String fieldKey, Envelope env, Facets facets,
                                      List<Map<String, Object>> results, IngestionCounters counters) {
        if (node == null || node.isNull()) return;

        if (node.isTextual() || node.isNumber() || node.isBoolean()) {
            processContentField(node.asText(), fieldKey, env, facets, results, counters, true);
            return;
        }

        if (node.isObject()) {
            Envelope analyticsEnvelope = buildCurrentEnvelope(node, env);
            JsonNode valueNode = node.get("value");
            if (valueNode != null && !valueNode.isNull()) {
                processContentField(valueNode.asText(), fieldKey, analyticsEnvelope, facets, results, counters, true);
            }
            for (String k : List.of("items","children","child")) {
                JsonNode arr = node.get(k);
                if (arr != null && arr.isArray()) {
                    for (JsonNode child : arr) processAnalyticsNode(child, fieldKey, analyticsEnvelope, facets, results, counters);
                }
            }
            node.fields().forEachRemaining(e -> {
                if (!List.of("items","children","child","value").contains(e.getKey())) {
                    processAnalyticsNode(e.getValue(), fieldKey, analyticsEnvelope, facets, results, counters);
                }
            });
            return;
        }

        if (node.isArray()) {
            int i = 0;
            for (JsonNode el : node) {
                if (el.isObject()) {
                    Envelope elEnv = buildCurrentEnvelope(el, env);
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

    private static class IngestionCounters {
        long copyFound = 0;
        long copyKept = 0;
        long analyticsFound = 0;
        long analyticsKept = 0;
        long copyBlankKept = 0;
        long analyticsBlankKept = 0;
    }
}