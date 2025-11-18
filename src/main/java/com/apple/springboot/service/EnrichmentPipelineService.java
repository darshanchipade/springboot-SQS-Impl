package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

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
    private final EnrichmentProcessor enrichmentProcessor;
    private final EnrichmentPersistenceService persistenceService;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     ObjectMapper objectMapper,
                                     SqsService sqsService,
                                     EnrichmentCompletionService completionService,
                                     EnrichmentProcessor enrichmentProcessor,
                                     EnrichmentPersistenceService persistenceService) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.sqsService = sqsService;
        this.completionService = completionService;
        this.enrichmentProcessor = enrichmentProcessor;
        this.persistenceService = persistenceService;
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
        logger.info("Converted {} cleansed entries into {} unique (sourcePath,field) pairs for CleansedDataStore ID {}.",
                maps.size(), rawItems.size(), cleansedDataStoreId);
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
        List<CleansedItemDetail> changedItems = itemsToEnrich.stream()
                .filter(itemDetail -> !enrichedContentElementRepository
                        .existsByCleansedDataIdAndItemSourcePathAndItemOriginalFieldNameAndCleansedText(
                                cleansedDataStoreId,
                                itemDetail.sourcePath,
                                itemDetail.originalFieldName,
                                itemDetail.cleansedContent))
                .collect(Collectors.toList());

        logger.info("After change-detection filtering, {} items remain for processing (CleansedDataStore ID {}).",
                changedItems.size(), cleansedDataStoreId);

        if (changedItems.isEmpty()) {
            logger.info("No items require enrichment for CleansedDataStore ID: {}. Marking as ENRICHED_NO_ITEMS_TO_PROCESS.", cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHED_NO_ITEMS_TO_PROCESS");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        List<CleansedItemDetail> skippedItems = changedItems.stream()
                .filter(detail -> detail.skipEnrichment)
                .collect(Collectors.toList());
        List<CleansedItemDetail> itemsToQueue = changedItems.stream()
                .filter(detail -> !detail.skipEnrichment)
                .collect(Collectors.toList());

        logger.info("Split {} items into {} to queue and {} to skip enrichment for CleansedDataStore ID {}.",
                changedItems.size(), itemsToQueue.size(), skippedItems.size(), cleansedDataStoreId);

        logger.info("Starting completion tracking for CleansedDataStore ID {} with {} total items (including skipped).",
                cleansedDataStoreId, changedItems.size());
        completionService.startTracking(cleansedDataStoreId, changedItems.size());

        for (CleansedItemDetail skipped : skippedItems) {
            handleSkippedItem(skipped, cleansedDataEntry);
        }

        if (itemsToQueue.isEmpty()) {
            logger.info("All {} items were skipped for enrichment for CleansedDataStore ID {}.", changedItems.size(), cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHMENT_SKIPPED");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            return;
        }

        logger.info("Queuing {} items for enrichment after skipping {} for CleansedDataStore ID {}.",
                itemsToQueue.size(), skippedItems.size(), cleansedDataStoreId);

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

    private void handleSkippedItem(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry) {
        try {
            persistenceService.saveSkippedEnrichedElement(itemDetail, cleansedDataEntry, "ENRICHMENT_SKIPPED");
        } catch (Exception e) {
            logger.error("Failed to persist skipped enrichment item for {}::{}: {}", itemDetail.sourcePath, itemDetail.originalFieldName, e.getMessage(), e);
        }

        try {
            boolean complete = completionService.itemCompleted(cleansedDataEntry.getId());
            if (complete) {
                logger.info("All items complete for {} after processing skipped entries. Running finalization.", cleansedDataEntry.getId());
                enrichmentProcessor.runFinalizationSteps(cleansedDataEntry);
            }
        } catch (Exception ex) {
            logger.error("Completion tracking failed for {} while handling skipped items: {}", cleansedDataEntry.getId(), ex.getMessage(), ex);
        }
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
                        boolean skipEnrichment = extractSkipFlag(map.get("skipEnrichment"));
                        return new CleansedItemDetail(sourcePath, originalFieldName, cleansedContent, model, context, skipEnrichment);
                    } catch (Exception e) {
                        logger.warn("Could not convert map to CleansedItemDetail object. Skipping item. Map: {}, Error: {}", map, e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private boolean extractSkipFlag(Object flag) {
        if (flag instanceof Boolean b) {
            return b;
        }
        if (flag instanceof String s) {
            return Boolean.parseBoolean(s);
        }
        return false;
    }
}