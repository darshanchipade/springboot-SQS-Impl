package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class EnrichmentPipelineService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPipelineService.class);

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ObjectMapper objectMapper;
    private final SqsService sqsService;
    private final EnrichmentCompletionService completionService;
    private final ContentHashRepository contentHashRepository;
    private final boolean strictUsagePath;
    private final boolean considerContextChange;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     ObjectMapper objectMapper,
                                     SqsService sqsService,
                                     EnrichmentCompletionService completionService,
                                     ContentHashRepository contentHashRepository,
                                     @Value("${app.ingestion.strict-usage-path:false}") boolean strictUsagePath,
                                     @Value("${app.ingestion.consider-context-change:false}") boolean considerContextChange) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.sqsService = sqsService;
        this.completionService = completionService;
        this.contentHashRepository = contentHashRepository;
        this.strictUsagePath = strictUsagePath;
        this.considerContextChange = considerContextChange;
    }

    @Transactional
    public void enrichAndStore(CleansedDataStore cleansedDataEntry) throws JsonProcessingException {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            logger.warn("Received null or no ID CleansedDataStore entry for enrichment. Skipping...");
            return;
        }

        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        logger.info("Starting enrichment process for CleansedDataStore ID: {}", cleansedDataStoreId);

        if (!"CLEANSED_PENDING_ENRICHMENT".equals(cleansedDataEntry.getStatus())) {
            logger.info("CleansedDataStore ID: {} is not in 'CLEANSED_PENDING_ENRICHMENT' state (current: {}). Skipping enrichment.",
                    cleansedDataStoreId, cleansedDataEntry.getStatus());
            return;
        }

        cleansedDataEntry.setStatus("ENRICHMENT_IN_PROGRESS");
        cleansedDataStoreRepository.save(cleansedDataEntry);

        List<Map<String, Object>> maps = cleansedDataEntry.getCleansedItems();
        if (maps == null || maps.isEmpty()) {
            logger.info("No items found in cleansed_items for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        // Convert and deduplicate by (sourcePath, originalFieldName)
        List<CleansedItemDetail> rawItems = convertMapsToCleansedItemDetails(maps);
        List<CleansedItemDetail> itemsToEnrich = rawItems.stream()
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                it -> it.sourcePath + "::" + it.originalFieldName,
                                it -> it,
                                (a, b) -> a,
                                LinkedHashMap::new
                        ),
                        m -> new ArrayList<>(m.values())
                ));

        // Filter to only items that need enrichment (text changed)
        List<CleansedItemDetail> itemsToQueue = itemsToEnrich.stream()
                .filter(this::shouldQueueItem)
                .collect(Collectors.toList());
        int skipped = itemsToEnrich.size() - itemsToQueue.size();
        if (skipped > 0) {
            logger.info("Skipped {} unchanged items for CleansedDataStore ID: {} based on existing enrichment metadata.", skipped, cleansedDataEntry.getId());
        }

        // Track completion on the ACTUAL number queued
        completionService.startTracking(cleansedDataStoreId, itemsToQueue.size());

        // Queue items
        for (CleansedItemDetail itemDetail : itemsToQueue) {
            EnrichmentMessage message = new EnrichmentMessage(itemDetail, cleansedDataStoreId);
            sqsService.sendMessage(message);
        }

        cleansedDataEntry.setStatus("ENRICHMENT_QUEUED");
        logger.info("{} items were queued for enrichment for CleansedDataStore ID: {}", itemsToQueue.size(), cleansedDataEntry.getId());
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished queuing enrichment tasks for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), cleansedDataEntry.getStatus());
    }

    private List<CleansedItemDetail> convertMapsToCleansedItemDetails(List<Map<String, Object>> maps) {
        return maps.stream()
                .map(map -> {
                    try {
                        String sourcePath = (String) map.get("sourcePath");
                        String originalFieldName = (String) map.get("originalFieldName");
                        String cleansedContent = (String) map.get("cleansedContent");
                        String model = (String) map.get("model");
                        EnrichmentContext context = objectMapper.convertValue(map.get("context"), EnrichmentContext.class);
                        String usagePath = (String) map.get("usagePath");
                        String contentHash = (String) map.get("contentHash");
                        String contextHash = (String) map.get("contextHash");
                        return new CleansedItemDetail(sourcePath, originalFieldName, cleansedContent, model, context, usagePath, contentHash, contextHash);
                    } catch (Exception e) {
                        logger.warn("Could not convert map to CleansedItemDetail object. Skipping item. Map: {}, Error: {}", map, e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private boolean shouldQueueItem(CleansedItemDetail itemDetail) {
        if (itemDetail == null) {
            return false;
        }

        // If we already have enriched content with the same cleansed text, skip immediately
        if (enrichedContentElementRepository
                .existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(
                        itemDetail.sourcePath, itemDetail.originalFieldName, itemDetail.cleansedContent)) {
            return false;
        }

        // Fallback to content-hash-based change detection to guard against upstream misses
        Optional<com.apple.springboot.model.ContentHash> existingHash = Optional.empty();
        if (itemDetail.usagePath != null) {
            existingHash = contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(
                    itemDetail.sourcePath, itemDetail.originalFieldName, itemDetail.usagePath);
        }
        if (existingHash.isEmpty() && !strictUsagePath) {
            existingHash = contentHashRepository.findBySourcePathAndItemType(
                    itemDetail.sourcePath, itemDetail.originalFieldName);
        }

        if (existingHash.isEmpty()) {
            return true;
        }

        boolean contentChanged = itemDetail.contentHash == null
                || !Objects.equals(existingHash.get().getContentHash(), itemDetail.contentHash);

        boolean contextChanged = considerContextChange && itemDetail.contextHash != null
                && !Objects.equals(existingHash.get().getContextHash(), itemDetail.contextHash);

        return contentChanged || contextChanged;
    }
}