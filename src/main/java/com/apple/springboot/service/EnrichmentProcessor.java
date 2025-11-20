package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.CleansedItemDetail;
import com.apple.springboot.model.EnrichmentMessage;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
    private final RateLimiter bedrockRateLimiter;
    private final RateLimiter chatRateLimiter;
    private final EnrichmentPersistenceService persistenceService;
    private final AIResponseValidator aiResponseValidator;
    private final ObjectMapper objectMapper;
    private final EnrichmentCompletionService completionService;
    private final EnrichmentProgressService progressService;
    private final EnrichmentFinalizationService finalizationService;
    @Value("${app.enrichment.computeItemVector:false}")
    private boolean computeItemVector;

    @SuppressWarnings("UnstableApiUsage")
    public EnrichmentProcessor(BedrockEnrichmentService bedrockEnrichmentService,
                               CleansedDataStoreRepository cleansedDataStoreRepository,
                               EnrichedContentElementRepository enrichedContentElementRepository,
                               RateLimiter bedrockRateLimiter,
                               @Qualifier("chatRateLimiter") RateLimiter chatRateLimiter,
                                 EnrichmentPersistenceService persistenceService,
                                 AIResponseValidator aiResponseValidator,
                                 ObjectMapper objectMapper,
                                 EnrichmentCompletionService completionService,
                                  EnrichmentProgressService progressService,
                                  EnrichmentFinalizationService finalizationService) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.bedrockRateLimiter = bedrockRateLimiter;
        this.chatRateLimiter = chatRateLimiter;
        this.persistenceService = persistenceService;
        this.aiResponseValidator = aiResponseValidator;
        this.objectMapper = objectMapper;
        this.completionService = completionService;
        this.progressService = progressService;
        this.finalizationService = finalizationService;
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
            performEnrichment(itemDetail, cleansedDataEntry);
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
                progressService.increment(cleansedDataEntry.getId(), itemDetail.originalFieldName);
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

    public void processInline(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry) {
        throttleFor(bedrockRateLimiter, chatRateLimiter);
        try {
            performEnrichment(itemDetail, cleansedDataEntry);
        } catch (ThrottledException te) {
            logger.warn("Inline enrichment throttled for {}::{}, marking as error: {}", itemDetail.sourcePath, itemDetail.originalFieldName, te.getMessage());
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", "Throttled during inline processing");
        } catch (Exception e) {
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_UNEXPECTED", e.getMessage());
        } finally {
            progressService.increment(cleansedDataEntry.getId(), itemDetail.originalFieldName);
        }
    }

    public void finalizeInline(CleansedDataStore cleansedDataEntry) {
        runFinalizationSteps(cleansedDataEntry);
    }

    public void runFinalizationSteps(CleansedDataStore cleansedDataEntry) {
        finalizationService.finalizeCleansedData(cleansedDataEntry);
    }

    private void throttleFor(RateLimiter... limiters) {
        if (limiters == null) return;
        for (RateLimiter limiter : limiters) {
            if (limiter != null) {
                limiter.acquire();
            }
        }
    }

    private void performEnrichment(CleansedItemDetail itemDetail, CleansedDataStore cleansedDataEntry) throws Exception {
        try {
            Map<String, String> itemContent = new HashMap<>();
            itemContent.put("cleansedContent", itemDetail.cleansedContent);
            JsonNode itemJson = objectMapper.valueToTree(itemContent);

            Map<String, Object> result = bedrockEnrichmentService.enrichItem(itemJson, itemDetail.context);

            if (result.containsKey("error")) {
                String msg = "Bedrock enrichment failed: " + result.get("error");
                persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", msg);
            } else {
                Map<String, Object> ctx = objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<>() {});
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
            throw te;
        } catch (Exception e) {
            throw e;
        }
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