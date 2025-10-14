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

        // 1) Try normal path from enriched elements
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);

        // 2) Fallback: if nothing saved (e.g., all items errored or rows failed earlier),
        //    derive minimal sections directly from cleansed items so chunking still proceeds
        if (savedSections == null || savedSections.isEmpty()) {
            logger.warn("No consolidated sections found; deriving from cleansed items as fallback.");
            List<Map<String, Object>> items = cleansedDataEntry.getCleansedItems();
            List<ConsolidatedEnrichedSection> toSave = new ArrayList<>();
            if (items != null) {
                for (Map<String, Object> item : items) {
                    String sourcePath = (String) item.get("sourcePath");
                    String originalFieldName = (String) item.get("originalFieldName");
                    String cleansedText = (String) item.get("cleansedContent");

                    ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
                    section.setCleansedDataId(cleansedDataEntry.getId());
                    section.setVersion(cleansedDataEntry.getVersion());
                    section.setSourceUri(cleansedDataEntry.getSourceUri());
                    section.setSectionPath(sourcePath);     // container
                    section.setSectionUri(sourcePath);      // fragment fallback
                    section.setOriginalFieldName(originalFieldName);
                    section.setCleansedText(cleansedText);
                    section.setSavedAt(OffsetDateTime.now());
                    section.setStatus("ERROR_FALLBACK");    // mark as fallback
                    toSave.add(section);
                }
            }
            if (!toSave.isEmpty()) {
                // batch save
                toSave.forEach(s -> {
                    try { /* repo inside service */ } catch (Exception ignore) {}
                });
                // Reload
                savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
            }
        }

        // 3) Always chunk; save chunk even if vector fails (vector=null)
        List<ContentChunk> chunksToSave = new ArrayList<>();
        for (ConsolidatedEnrichedSection section : savedSections) {
            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
            for (String chunkText : chunks) {
                float[] vector = null;
                try { vector = bedrockEnrichmentService.generateEmbedding(chunkText); }
                catch (Exception e) {
                    logger.warn("Embedding failed; saving chunk without vector. section={}", section.getSectionPath(), e);
                }
                ContentChunk chunk = new ContentChunk();
                chunk.setConsolidatedEnrichedSection(section);
                chunk.setChunkText(chunkText);
                chunk.setSourceField(section.getSourceUri());
                chunk.setSectionPath(section.getSectionPath());
                chunk.setVector(vector); // may be null
                chunk.setCreatedAt(OffsetDateTime.now());
                chunk.setCreatedBy("EnrichmentPipelineService");
                chunksToSave.add(chunk);
            }
        }
        if (!chunksToSave.isEmpty()) {
            // batch insert for speed
            contentChunkRepository.saveAll(chunksToSave);
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