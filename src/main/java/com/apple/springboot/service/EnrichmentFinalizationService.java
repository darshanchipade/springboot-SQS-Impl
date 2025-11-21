package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.google.common.util.concurrent.RateLimiter;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class EnrichmentFinalizationService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentFinalizationService.class);

    private final ConsolidatedSectionService consolidatedSectionService;
    private final ContentChunkRepository contentChunkRepository;
    private final TextChunkingService textChunkingService;
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EntityManager entityManager;
    private final EnrichmentProgressService progressService;
    private final RateLimiter bedrockRateLimiter;
    private final RateLimiter embedRateLimiter;

    public EnrichmentFinalizationService(ConsolidatedSectionService consolidatedSectionService,
                                         ContentChunkRepository contentChunkRepository,
                                         TextChunkingService textChunkingService,
                                         BedrockEnrichmentService bedrockEnrichmentService,
                                         CleansedDataStoreRepository cleansedDataStoreRepository,
                                         EntityManager entityManager,
                                         EnrichmentProgressService progressService,
                                         RateLimiter bedrockRateLimiter,
                                         @Qualifier("embedRateLimiter") RateLimiter embedRateLimiter) {
        this.consolidatedSectionService = consolidatedSectionService;
        this.contentChunkRepository = contentChunkRepository;
        this.textChunkingService = textChunkingService;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.entityManager = entityManager;
        this.progressService = progressService;
        this.bedrockRateLimiter = bedrockRateLimiter;
        this.embedRateLimiter = embedRateLimiter;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finalizeCleansedData(CleansedDataStore cleansedDataEntry, boolean allowInlineLockBypass) {
        if (!acquireFinalizationLock(cleansedDataEntry, allowInlineLockBypass)) {
            return;
        }
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);

        List<ConsolidatedEnrichedSection> savedSections = consolidatedSectionService.getSectionsFor(cleansedDataEntry);
        contentChunkRepository.deleteByCleansedDataId(cleansedDataEntry.getId());

        final int batchSize = 100;
        List<ContentChunk> chunkBatch = new ArrayList<>();
        for (ConsolidatedEnrichedSection section : savedSections) {
            List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
            for (String chunkText : chunks) {
                try {
                    throttleFor(bedrockRateLimiter, embedRateLimiter);
                    float[] vector = bedrockEnrichmentService.generateEmbedding(chunkText);
                    ContentChunk contentChunk = new ContentChunk();
                    contentChunk.setConsolidatedEnrichedSection(section);
                    contentChunk.setChunkText(chunkText);
                    contentChunk.setSourceField(section.getSourceUri());
                    contentChunk.setSectionPath(section.getSectionPath());
                    contentChunk.setVector(vector);
                    contentChunk.setCreatedAt(OffsetDateTime.now());
                    contentChunk.setCreatedBy("EnrichmentPipelineService");
                    chunkBatch.add(contentChunk);
                    if (chunkBatch.size() >= batchSize) {
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
        progressService.complete(cleansedDataEntry.getId());
    }

    private boolean acquireFinalizationLock(CleansedDataStore cleansedDataEntry, boolean allowInlineLockBypass) {
        UUID id = cleansedDataEntry.getId();
        if (id == null) {
            return false;
        }
        List<String> eligibleStatuses = List.of("ENRICHMENT_QUEUED", "ENRICHMENT_SKIPPED", "ENRICHMENT_IN_PROGRESS");
        for (String expected : eligibleStatuses) {
            int updated = cleansedDataStoreRepository.updateStatusIfMatches(id, expected, "FINALIZING");
            if (updated == 1) {
                cleansedDataEntry.setStatus("FINALIZING");
                logger.info("Acquired finalization lock for CleansedDataStore ID {} (previous status {}).", id, expected);
                return true;
            }
        }
        if (allowInlineLockBypass) {
            String currentStatus = cleansedDataEntry.getStatus();
            if (eligibleStatuses.contains(currentStatus)) {
                logger.info("Inline finalization detected for {} with local status '{}'. Forcing FINALIZING.", id, currentStatus);
                cleansedDataEntry.setStatus("FINALIZING");
                cleansedDataStoreRepository.save(cleansedDataEntry);
                return true;
            }
        }
        if ("FINALIZING".equalsIgnoreCase(cleansedDataEntry.getStatus())) {
            return true;
        }
        logger.info("Another instance is finalizing CleansedDataStore ID {}. Current status: {}", id, cleansedDataEntry.getStatus());
        return false;
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry) {
        entityManager.flush();
        @SuppressWarnings("unchecked")
        List<Object[]> statusCounts = entityManager.createNativeQuery(
                        "select upper(status) as status, count(*) as cnt " +
                                "from enriched_content_elements where cleansed_data_id = :id group by upper(status)")
                .setParameter("id", cleansedDataEntry.getId())
                .getResultList();

        long successCount = statusCounts.stream()
                .filter(row -> "ENRICHED".equals(row[0]))
                .mapToLong(row -> ((Number) row[1]).longValue())
                .sum();
        long skippedCount = statusCounts.stream()
                .filter(row -> row[0] instanceof String s && s.contains("SKIPPED"))
                .mapToLong(row -> ((Number) row[1]).longValue())
                .sum();
        long errorCount = statusCounts.stream()
                .filter(row -> row[0] instanceof String s && s.startsWith("ERROR"))
                .mapToLong(row -> ((Number) row[1]).longValue())
                .sum();
        logger.info("Status counts for {} -> success={}, skipped={}, error={}",
                cleansedDataEntry.getId(), successCount, skippedCount, errorCount);

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

    private void throttleFor(RateLimiter... limiters) {
        if (limiters == null) {
            return;
        }
        for (RateLimiter limiter : limiters) {
            if (limiter != null) {
                limiter.acquire();
            }
        }
    }
}
