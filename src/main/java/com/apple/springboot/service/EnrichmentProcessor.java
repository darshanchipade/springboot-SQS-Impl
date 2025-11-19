package com.apple.springboot.service;

import com.apple.springboot.model.*;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class EnrichmentProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentProcessor.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final TextChunkingService textChunkingService;
    private final ContentChunkRepository contentChunkRepository;
    private final RateLimiter bedrockRateLimiter;
    private final RateLimiter chatRateLimiter;
    private final RateLimiter embedRateLimiter;
    private final EnrichmentPersistenceService persistenceService;
    private final AIResponseValidator aiResponseValidator;
    private final ObjectMapper objectMapper;
    private final EnrichmentCompletionService completionService;
    @Value("${app.enrichment.computeItemVector:false}")
    private boolean computeItemVector;

    @SuppressWarnings("UnstableApiUsage")
    public EnrichmentProcessor(BedrockEnrichmentService bedrockEnrichmentService,
                               CleansedDataStoreRepository cleansedDataStoreRepository,
                               EnrichedContentElementRepository enrichedContentElementRepository,
                               ConsolidatedSectionService consolidatedSectionService,
                               TextChunkingService textChunkingService,
                               ContentChunkRepository contentChunkRepository,
                               RateLimiter bedrockRateLimiter,
                               @Qualifier("chatRateLimiter") RateLimiter chatRateLimiter,
                               @Qualifier("embedRateLimiter") RateLimiter embedRateLimiter,
                               EnrichmentPersistenceService persistenceService,
                               AIResponseValidator aiResponseValidator,
                               ObjectMapper objectMapper,
                               EnrichmentCompletionService completionService) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.consolidatedSectionService = consolidatedSectionService;
        this.textChunkingService = textChunkingService;
        this.contentChunkRepository = contentChunkRepository;
        this.bedrockRateLimiter = bedrockRateLimiter;
        this.chatRateLimiter = chatRateLimiter;
        this.embedRateLimiter = embedRateLimiter;
        this.persistenceService = persistenceService;
        this.aiResponseValidator = aiResponseValidator;
        this.objectMapper = objectMapper;
        this.completionService = completionService;
    }

    public void process(EnrichmentMessage message) {
        boolean shouldRecordCompletion = true;
        throttleFor(bedrockRateLimiter, chatRateLimiter);

        CleansedItemDetail itemDetail = message.getCleansedItemDetail();
        UUID cleansedDataStoreId = message.getCleansedDataStoreId();

        CleansedDataStore cleansedDataEntry = cleansedDataStoreRepository.findById(cleansedDataStoreId)
                .orElse(null);

        if (cleansedDataEntry == null) {
            logger.error("Could not find CleansedDataStore with ID: {}. Skipping message but marking as completed to avoid blocking finalization.", cleansedDataStoreId);
            completionService.itemCompleted(cleansedDataStoreId);
            return;
        }

        try {
            // Proceed without per-run idempotency skip; queuing logic prevents duplicates
            Map<String, String> itemContent = new HashMap<>();
            itemContent.put("cleansedContent", itemDetail.cleansedContent);
            JsonNode itemJson = objectMapper.valueToTree(itemContent);

            Map<String, Object> result = bedrockEnrichmentService.enrichItem(itemJson, itemDetail.context);

            if (result.containsKey("error")) {
                String msg = "Bedrock enrichment failed: " + result.get("error");
                persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", msg);
            } else {
                Map<String, Object> ctx = objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<>() {
                });
                ctx.put("fullContextId", itemDetail.sourcePath + "::" + itemDetail.originalFieldName);
                ctx.put("sourcePath", itemDetail.sourcePath);
                Map<String, Object> prov = new HashMap<>();
                prov.put("modelId", bedrockEnrichmentService.getConfiguredModelId());
                ctx.put("provenance", prov);
                result.put("context", ctx);

                if (!aiResponseValidator.isValid(result)) {
                    persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_VALIDATION_FAILED", "Invalid AI response");
                } else {
                    persistenceService.saveEnrichedElement(itemDetail, cleansedDataEntry, result, "ENRICHED");
                }
            }
        } catch (ThrottledException te) {
            shouldRecordCompletion = false;
            logger.warn("Bedrock throttled for item {}::{}, re-queueing message.", itemDetail.sourcePath, itemDetail.originalFieldName);
            throw te;
        } catch (Exception e) {
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_UNEXPECTED", e.getMessage());
        } finally {
            if (!shouldRecordCompletion) {
                logger.debug("Skipping completion bookkeeping for {} due to throttling retry.", cleansedDataEntry.getId());
            } else {
                try {
                    boolean allDone = completionService.itemCompleted(cleansedDataEntry.getId());
                    if (allDone) {
                        logger.info("All queued items complete for {}. Running finalization.", cleansedDataEntry.getId());
                        runFinalizationSteps(cleansedDataEntry);
                    } else if (!completionService.isTracking(cleansedDataEntry.getId())) {
                        logger.warn("Falling back to database completion check for CleansedDataStore ID {} (tracking missing).",
                                cleansedDataEntry.getId());
                        // Lost in-memory tracking (e.g., service restart). Fall back to DB counts.
                        checkCompletion(cleansedDataEntry);
                    } else {
                        int remaining = completionService.getRemainingCount(cleansedDataEntry.getId());
                        int expected = completionService.getExpectedCount(cleansedDataEntry.getId());
                        long processed = enrichedContentElementRepository.countByCleansedDataId(cleansedDataEntry.getId());
                        logger.debug("Completion tracker active for {}. {} items remaining per counter; {} expected; {} rows currently persisted.",
                                cleansedDataEntry.getId(),
                                remaining,
                                expected,
                                processed);
                        if (expected > 0 && processed >= expected && remaining > 0 && !isFinalStatus(cleansedDataEntry)) {
                            logger.warn("Detected processed={} >= expected={} while counter still reports {} remaining for {}. Forcing finalization.",
                                    processed, expected, remaining, cleansedDataEntry.getId());
                            if (completionService.forceComplete(cleansedDataEntry.getId())) {
                                runFinalizationSteps(cleansedDataEntry);
                            }
                        }
                    }
                } catch (Exception ex) {
                    logger.error("Completion tracking failed for {}: {}", cleansedDataEntry.getId(), ex.getMessage(), ex);
                }
            }
        }
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void runFinalizationSteps(CleansedDataStore cleansedDataEntry) {
        if (!acquireFinalizationLock(cleansedDataEntry)) {
            return;
        }
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        contentChunkRepository.deleteByCleansedDataId(cleansedDataEntry.getId());
        final int BATCH_SIZE = 200;
        List<ContentChunk> chunkBatch = new ArrayList<>();
        for (ConsolidatedEnrichedSection section : savedSections) {
            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
            for (String chunkText : chunks) {
                try {
                    // This call also needs to be rate-limited
                    throttleFor(bedrockRateLimiter, embedRateLimiter);
                    float[] vector = bedrockEnrichmentService.generateEmbedding(chunkText);
                    // Always create a new content chunk row for versioning
                    ContentChunk contentChunk = new ContentChunk();
                    contentChunk.setConsolidatedEnrichedSection(section);
                    contentChunk.setChunkText(chunkText);
                    contentChunk.setSourceField(section.getSourceUri());
                    contentChunk.setSectionPath(section.getSectionPath());
                    contentChunk.setVector(vector);
                    contentChunk.setCreatedAt(OffsetDateTime.now());
                    contentChunk.setCreatedBy("EnrichmentPipelineService");
                    chunkBatch.add(contentChunk);
                    if (chunkBatch.size() >= BATCH_SIZE) {
                        contentChunkRepository.saveAll(chunkBatch);
                        chunkBatch.clear();
                    }
                } catch (Exception e) {
                    logger.error("Error creating content chunk for item path {}: {}", section.getSectionPath(), e.getMessage(), e);
                }
            }
        }
        if (!chunkBatch.isEmpty()) {
            contentChunkRepository.saveAll(chunkBatch);
        }
        updateFinalCleansedDataStatus(cleansedDataEntry);
    }

    private boolean acquireFinalizationLock(CleansedDataStore cleansedDataEntry) {
        UUID id = cleansedDataEntry.getId();
        if (id == null) {
            return false;
        }
        List<String> eligibleStatuses = List.of("ENRICHMENT_QUEUED", "ENRICHMENT_SKIPPED");
        for (String expected : eligibleStatuses) {
            int updated = cleansedDataStoreRepository.updateStatusIfMatches(id, expected, "FINALIZING");
            if (updated == 1) {
                cleansedDataEntry.setStatus("FINALIZING");
                logger.info("Acquired finalization lock for CleansedDataStore ID {} (previous status {}).", id, expected);
                return true;
            }
        }
        if ("FINALIZING".equalsIgnoreCase(cleansedDataEntry.getStatus())) {
            return true;
        }
        logger.info("Another instance is finalizing CleansedDataStore ID {}. Current status: {}", id, cleansedDataEntry.getStatus());
        return false;
    }

    private void throttleFor(RateLimiter... limiters) {
        if (limiters == null) return;
        for (RateLimiter limiter : limiters) {
            if (limiter != null) {
                limiter.acquire();
            }
        }
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry) {
        long errorCount = enrichedContentElementRepository.countByCleansedDataIdAndStatusContaining(cleansedDataEntry.getId(), "ERROR");
        long successCount = enrichedContentElementRepository.countByCleansedDataIdAndStatus(cleansedDataEntry.getId(), "ENRICHED");
        long skippedCount = enrichedContentElementRepository.countByCleansedDataIdAndStatusContaining(cleansedDataEntry.getId(), "SKIPPED");

        String finalStatus;
        if (errorCount == 0) {
            if (successCount + skippedCount > 0) {
                finalStatus = "ENRICHED_COMPLETE";
            } else {
                finalStatus = "ENRICHMENT_FAILED";
            }
        } else if (successCount + skippedCount > 0) {
            finalStatus = "PARTIALLY_ENRICHED";
        } else {
            finalStatus = "ENRICHMENT_FAILED";
        }
        cleansedDataEntry.setStatus(finalStatus);
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }

    private void checkCompletion(CleansedDataStore cleansedDataEntry) {
        // expected = unique items by (sourcePath, originalFieldName)
        long expected = cleansedDataEntry.getCleansedItems().stream()
                .map(item -> ((String) item.get("sourcePath")) + "::" + ((String) item.get("originalFieldName")))
                .distinct()
                .count();

        long processedCount = enrichedContentElementRepository.countByCleansedDataId(cleansedDataEntry.getId());

        logger.info("Completion check for {}: processed={} expected={}", cleansedDataEntry.getId(), processedCount, expected);
        if (processedCount >= expected) {
            logger.info("All items for CleansedDataStore ID {} processed. Running finalization.", cleansedDataEntry.getId());
            runFinalizationSteps(cleansedDataEntry);
        }
    }

    private boolean isFinalStatus(CleansedDataStore cleansedDataEntry) {
        String status = cleansedDataEntry.getStatus();
        return status != null && (status.contains("ENRICHED_COMPLETE") || status.contains("PARTIALLY_ENRICHED"));
    }
}