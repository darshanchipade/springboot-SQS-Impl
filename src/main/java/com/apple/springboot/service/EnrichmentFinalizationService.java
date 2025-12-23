package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class EnrichmentFinalizationService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentFinalizationService.class);

    private final ConsolidatedSectionService consolidatedSectionService;
    private final ContentChunkRepository contentChunkRepository;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EntityManager entityManager;
    private final EnrichmentProgressService progressService;

    public EnrichmentFinalizationService(ConsolidatedSectionService consolidatedSectionService,
                                         ContentChunkRepository contentChunkRepository,
                                         CleansedDataStoreRepository cleansedDataStoreRepository,
                                         EntityManager entityManager,
                                         EnrichmentProgressService progressService) {
        this.consolidatedSectionService = consolidatedSectionService;
        this.contentChunkRepository = contentChunkRepository;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.entityManager = entityManager;
        this.progressService = progressService;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finalizeCleansedData(CleansedDataStore cleansedDataEntry, boolean allowInlineLockBypass) {
        if (!acquireFinalizationLock(cleansedDataEntry, allowInlineLockBypass)) {
            return;
        }
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        contentChunkRepository.deleteByCleansedDataId(cleansedDataEntry.getId());
        consolidatedSectionService.markSectionsPendingEmbedding(cleansedDataEntry.getId(), cleansedDataEntry.getVersion());
        long pendingSections = consolidatedSectionService.countSectionsMissingEmbeddings(cleansedDataEntry.getId());
        updateFinalCleansedDataStatus(cleansedDataEntry, pendingSections == 0);
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
        // Re-check the persisted status to avoid misleading logs from a stale entity instance.
        String persistedStatus = cleansedDataStoreRepository.findById(id)
                .map(CleansedDataStore::getStatus)
                .orElse(cleansedDataEntry.getStatus());
        if ("FINALIZING".equalsIgnoreCase(persistedStatus)) {
            cleansedDataEntry.setStatus(persistedStatus);
            return true;
        }
        logger.info("Could not acquire finalization lock for CleansedDataStore ID {}. Current status: {}", id, persistedStatus);
        return false;
    }

    private void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, boolean embeddingsReady) {
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
                finalStatus = embeddingsReady ? "ENRICHED_COMPLETE" : "PARTIALLY_ENRICHED";
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

    @Transactional
    public void markEmbeddingsComplete(UUID cleansedDataStoreId) {
        cleansedDataStoreRepository.findById(cleansedDataStoreId).ifPresent(cleansed -> {
            updateFinalCleansedDataStatus(cleansed, true);
            progressService.complete(cleansed.getId());
        });
    }
}
