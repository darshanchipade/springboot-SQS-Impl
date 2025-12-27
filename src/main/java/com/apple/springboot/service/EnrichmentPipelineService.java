package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.CleansedItemDetail;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.CompletableFuture;

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
    private final EnrichmentProgressService progressService;
    private final ContentHashingService contentHashingService;
    private final boolean useSqs;

    public EnrichmentPipelineService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                     EnrichedContentElementRepository enrichedContentElementRepository,
                                     ObjectMapper objectMapper,
                                     SqsService sqsService,
                                     EnrichmentCompletionService completionService,
                                     EnrichmentProcessor enrichmentProcessor,
                                     EnrichmentPersistenceService persistenceService,
                                     EnrichmentProgressService progressService,
                                     ContentHashingService contentHashingService,
                                     @Value("${app.enrichment.use-sqs:false}") boolean useSqs) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.sqsService = sqsService;
        this.completionService = completionService;
        this.enrichmentProcessor = enrichmentProcessor;
        this.persistenceService = persistenceService;
        this.progressService = progressService;
        this.contentHashingService = contentHashingService;
        this.useSqs = useSqs;
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

        // Filter to only items that need enrichment (text changed).
        // Priority order:
        // 1) If this cleansedDataId already has enriched elements, compare against those (supports "edit one item and re-run").
        // IMPORTANT: Compare against the previous version for this same sourceUri to avoid false
        // negatives from global "exists" checks (which can match other sources/versions).
        List<CleansedItemDetail> changedItems;
        List<com.apple.springboot.model.EnrichedContentElement> currentElements =
                enrichedContentElementRepository.findAllByCleansedDataId(cleansedDataStoreId);
        if (currentElements != null && !currentElements.isEmpty()) {
            Map<String, String> currentHashByKey = currentElements.stream()
                    .filter(e -> e.getItemSourcePath() != null && e.getItemOriginalFieldName() != null)
                    .collect(Collectors.toMap(
                            e -> e.getItemSourcePath() + "::" + e.getItemOriginalFieldName(),
                            com.apple.springboot.model.EnrichedContentElement::getContentHash,
                            (a, b) -> a
                    ));
            Map<String, String> currentTextByKey = currentElements.stream()
                    .filter(e -> e.getItemSourcePath() != null && e.getItemOriginalFieldName() != null)
                    .collect(Collectors.toMap(
                            e -> e.getItemSourcePath() + "::" + e.getItemOriginalFieldName(),
                            com.apple.springboot.model.EnrichedContentElement::getCleansedText,
                            (a, b) -> a
                    ));
            changedItems = itemsToEnrich.stream()
                    .filter(itemDetail -> {
                        String key = itemDetail.sourcePath + "::" + itemDetail.originalFieldName;
                        String baselineHash = currentHashByKey.get(key);
                        String currentHash = contentHashingService.hash(itemDetail.cleansedContent);
                        if (baselineHash != null && currentHash != null) {
                            return !Objects.equals(baselineHash, currentHash);
                        }
                        String baselineText = currentTextByKey.get(key);
                        return baselineText == null || !Objects.equals(baselineText, itemDetail.cleansedContent);
                    })
                    .collect(Collectors.toList());
        } else {
        Integer currentVersion = cleansedDataEntry.getVersion();
        Integer previousVersion = (currentVersion != null && currentVersion > 1) ? currentVersion - 1 : null;
        if (previousVersion != null && cleansedDataEntry.getSourceUri() != null) {
            List<com.apple.springboot.model.EnrichedContentElement> previousElements =
                    enrichedContentElementRepository.findAllBySourceUriAndVersion(cleansedDataEntry.getSourceUri(), previousVersion);
            Map<String, String> previousHashByKey = previousElements.stream()
                    .filter(e -> e.getItemSourcePath() != null && e.getItemOriginalFieldName() != null)
                    .collect(Collectors.toMap(
                            e -> e.getItemSourcePath() + "::" + e.getItemOriginalFieldName(),
                            com.apple.springboot.model.EnrichedContentElement::getContentHash,
                            (a, b) -> a
                    ));
            Map<String, String> previousTextByKey = previousElements.stream()
                    .filter(e -> e.getItemSourcePath() != null && e.getItemOriginalFieldName() != null)
                    .collect(Collectors.toMap(
                            e -> e.getItemSourcePath() + "::" + e.getItemOriginalFieldName(),
                            com.apple.springboot.model.EnrichedContentElement::getCleansedText,
                            (a, b) -> a
                    ));
            changedItems = itemsToEnrich.stream()
                    .filter(itemDetail -> {
                        String key = itemDetail.sourcePath + "::" + itemDetail.originalFieldName;
                        String currentHash = contentHashingService.hash(itemDetail.cleansedContent);
                        String prevHash = previousHashByKey.get(key);
                        if (prevHash != null && currentHash != null) {
                            return !Objects.equals(prevHash, currentHash);
                        }
                        // Back-compat: older rows won’t have hashes yet
                        String prev = previousTextByKey.get(key);
                        return prev == null || !Objects.equals(prev, itemDetail.cleansedContent);
                    })
                    .collect(Collectors.toList());
        } else {
            // First run (or missing version info): fall back to existence check.
            // Prefer scoped-by-sourceUri when available to avoid cross-source false negatives.
            changedItems = itemsToEnrich.stream()
                    .filter(itemDetail -> {
                        String currentHash = contentHashingService.hash(itemDetail.cleansedContent);
                        String sourceUri = cleansedDataEntry.getSourceUri();
                        if (sourceUri != null && currentHash != null) {
                            return !enrichedContentElementRepository
                                    .existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndContentHash(
                                            sourceUri,
                                            itemDetail.sourcePath,
                                            itemDetail.originalFieldName,
                                            currentHash);
                        }
                        if (currentHash != null) {
                            return !enrichedContentElementRepository
                                    .existsByItemSourcePathAndItemOriginalFieldNameAndContentHash(
                                            itemDetail.sourcePath,
                                            itemDetail.originalFieldName,
                                            currentHash);
                        }
                        if (sourceUri != null) {
                            return !enrichedContentElementRepository
                                    .existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndCleansedText(
                                            sourceUri,
                                            itemDetail.sourcePath,
                                            itemDetail.originalFieldName,
                                            itemDetail.cleansedContent);
                        }
                        return !enrichedContentElementRepository
                                .existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(
                                        itemDetail.sourcePath,
                                        itemDetail.originalFieldName,
                                        itemDetail.cleansedContent);
                    })
                    .collect(Collectors.toList());
        }
        }

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
        progressService.startTracking(cleansedDataStoreId, changedItems.size());

        if (useSqs) {
            logger.info("Starting completion tracking for CleansedDataStore ID {} with {} total items (including skipped).",
                    cleansedDataStoreId, changedItems.size());
            completionService.startTracking(cleansedDataStoreId, changedItems.size());
        }

        for (CleansedItemDetail skipped : skippedItems) {
            handleSkippedItem(skipped, cleansedDataEntry, useSqs);
        }

        if (itemsToQueue.isEmpty()) {
            logger.info("All {} items were skipped for enrichment for CleansedDataStore ID {}.", changedItems.size(), cleansedDataStoreId);
            cleansedDataEntry.setStatus("ENRICHMENT_SKIPPED");
            cleansedDataStoreRepository.save(cleansedDataEntry);
            // Even when everything is skipped (e.g. analytics excluded), we still need to run the
            // finalization steps (consolidation + embedding markers + final status) so downstream
            // reads don't end up with empty consolidated tables.
            try {
                CleansedDataStore latestForFinalization =
                        cleansedDataStoreRepository.findById(cleansedDataStoreId).orElse(cleansedDataEntry);
                enrichmentProcessor.runFinalizationSteps(latestForFinalization);
            } catch (Exception e) {
                logger.error("Finalization failed for CleansedDataStore ID {} after skipping all items: {}", cleansedDataStoreId, e.getMessage(), e);
            }
            return;
        }

        logger.info("Queuing {} items for enrichment after skipping {} for CleansedDataStore ID {}.",
                itemsToQueue.size(), skippedItems.size(), cleansedDataStoreId);

        List<EnrichmentMessage> messages = itemsToQueue.stream()
                .map(itemDetail -> new EnrichmentMessage(itemDetail, cleansedDataStoreId))
                .collect(Collectors.toList());

        cleansedDataEntry.setStatus("ENRICHMENT_QUEUED");
        cleansedDataStoreRepository.save(cleansedDataEntry);

        if (useSqs) {
            if (!messages.isEmpty()) {
                sqsService.sendMessages(messages);
            }
        } else {
            logger.info("SQS disabled; running enrichment inline for {} items.", messages.size());
            for (EnrichmentMessage message : messages) {
                try {
                    enrichmentProcessor.processInline(message.getCleansedItemDetail(), cleansedDataEntry);
                } catch (Exception ex) {
                    logger.error("Inline enrichment failed for {}::{} - {}", message.getCleansedItemDetail().sourcePath, message.getCleansedItemDetail().originalFieldName, ex.getMessage(), ex);
                }
            }
            scheduleInlineFinalization(cleansedDataEntry);
            return;
        }

        logger.info("{} items were dispatched for enrichment for CleansedDataStore ID: {}", itemsToQueue.size(), cleansedDataEntry.getId());
    }

    private void scheduleInlineFinalization(CleansedDataStore cleansedDataEntry) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            runInlineFinalizationImmediately(cleansedDataEntry);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                runInlineFinalizationImmediately(cleansedDataEntry);
            }
        });
    }

    private void runInlineFinalizationImmediately(CleansedDataStore cleansedDataEntry) {
        if (cleansedDataEntry == null || cleansedDataEntry.getId() == null) {
            return;
        }
        UUID cleansedDataStoreId = cleansedDataEntry.getId();
        CompletableFuture.runAsync(() -> {
            try {
                CleansedDataStore latest = cleansedDataStoreRepository.findById(cleansedDataStoreId).orElse(null);
                if (latest == null) {
                    logger.warn("Skipping inline finalization because CleansedDataStore ID {} no longer exists.", cleansedDataStoreId);
                    return;
                }
                enrichmentProcessor.finalizeInline(latest);
                logger.info("Inline finalization complete for CleansedDataStore ID: {}", cleansedDataStoreId);
            } catch (Exception e) {
                logger.error("Inline finalization failed for CleansedDataStore ID: {}", cleansedDataStoreId, e);
            }
        });
    }

    private void handleSkippedItem(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry, boolean trackCompletion) {
        try {
            persistenceService.saveSkippedEnrichedElement(itemDetail, cleansedDataEntry, "ENRICHMENT_SKIPPED");
        } catch (Exception e) {
            logger.error("Failed to persist skipped enrichment item for {}::{}: {}", itemDetail.sourcePath, itemDetail.originalFieldName, e.getMessage(), e);
        }

        progressService.increment(cleansedDataEntry.getId(), itemDetail.originalFieldName + " (skipped)");

        if (!trackCompletion) {
            return;
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