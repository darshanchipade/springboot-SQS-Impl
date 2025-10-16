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
        this.persistenceService = persistenceService;
        this.aiResponseValidator = aiResponseValidator;
        this.objectMapper = objectMapper;
        this.completionService = completionService;
    }

    public void process(EnrichmentMessage message) {
        boolean terminal = false; // only true on success or non-throttle error
        bedrockRateLimiter.acquire();

        CleansedItemDetail itemDetail = message.getCleansedItemDetail();
        UUID cleansedDataStoreId = message.getCleansedDataStoreId();

        CleansedDataStore cleansedDataEntry = cleansedDataStoreRepository.findById(cleansedDataStoreId)
                .orElse(null);

        if (cleansedDataEntry == null) {
            logger.error("Could not find CleansedDataStore with ID: {}. Cannot process item.", cleansedDataStoreId);
            return;
        }

        try {
            // Idempotency: if this item already exists as ENRICHED for this CleansedDataStore, skip Bedrock call
            var existingForThisRun = enrichedContentElementRepository
                    .findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(cleansedDataEntry.getId(), itemDetail.sourcePath, itemDetail.originalFieldName);
            if (existingForThisRun.isPresent() && "ENRICHED".equalsIgnoreCase(existingForThisRun.get().getStatus())) {
                logger.info("Skipping enrichment for already ENRICHED item {}::{} for CleansedDataStore {}",
                        itemDetail.sourcePath, itemDetail.originalFieldName, cleansedDataEntry.getId());
                return;
            }
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
            // DO NOT rethrow; persist error so we complete and consolidate
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_ENRICHMENT_FAILED", "Throttled after retries");
        } catch (Exception e) {
            persistenceService.saveErrorEnrichedElement(itemDetail, cleansedDataEntry, "ERROR_UNEXPECTED", e.getMessage());
        } finally {
            checkCompletion(cleansedDataEntry); // triggers consolidation + chunking when all items are processed
        }
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void runFinalizationSteps(CleansedDataStore cleansedDataEntry) {
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        for (ConsolidatedEnrichedSection section : savedSections) {
            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
            for (String chunkText : chunks) {
                try {
                    // This call also needs to be rate-limited
                    bedrockRateLimiter.acquire();
                    float[] vector = bedrockEnrichmentService.generateEmbedding(chunkText);
                    // Upsert content_chunk: update vector if chunk exists, else create new
                    contentChunkRepository.findByConsolidatedEnrichedSectionAndChunkText(section, chunkText)
                            .ifPresentOrElse(existing -> {
                                existing.setVector(vector);
                                contentChunkRepository.save(existing);
                            }, () -> {
                                ContentChunk contentChunk = new ContentChunk();
                                contentChunk.setConsolidatedEnrichedSection(section);
                                contentChunk.setChunkText(chunkText);
                                contentChunk.setSourceField(section.getSourceUri());
                                contentChunk.setSectionPath(section.getSectionPath());
                                contentChunk.setVector(vector);
                                contentChunk.setCreatedAt(OffsetDateTime.now());
                                contentChunk.setCreatedBy("EnrichmentPipelineService");
                                contentChunkRepository.save(contentChunk);
                            });
                } catch (Exception e) {
                    logger.error("Error creating content chunk for item path {}: {}", section.getSectionPath(), e.getMessage(), e);
                }
            }
        }
        updateFinalCleansedDataStatus(cleansedDataEntry);
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry) {
        long errorCount = enrichedContentElementRepository.countByCleansedDataIdAndStatusContaining(cleansedDataEntry.getId(), "ERROR");
        long successCount = enrichedContentElementRepository.countByCleansedDataIdAndStatus(cleansedDataEntry.getId(), "ENRICHED");

        String finalStatus;
        if (errorCount == 0) {
            finalStatus = "ENRICHED_COMPLETE";
        } else if (successCount > 0 || errorCount > 0) {
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
}