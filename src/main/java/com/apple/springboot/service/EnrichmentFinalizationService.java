package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Service
public class EnrichmentFinalizationService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentFinalizationService.class);

    private final ConsolidatedSectionService consolidatedSectionService;
    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EntityManager entityManager;

    public EnrichmentFinalizationService(ConsolidatedSectionService consolidatedSectionService,
                                         CleansedDataStoreRepository cleansedDataStoreRepository,
                                         EntityManager entityManager) {
        this.consolidatedSectionService = consolidatedSectionService;
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void finalizeCleansedData(CleansedDataStore cleansedDataEntry, boolean allowInlineLockBypass) {
        if (!acquireFinalizationLock(cleansedDataEntry, allowInlineLockBypass)) {
            return;
        }
        logger.info("Running finalization steps for CleansedDataStore ID: {}", cleansedDataEntry.getId());
        consolidatedSectionService.saveFromCleansedEntry(cleansedDataEntry);
        cleansedDataEntry.setStatus("ENRICHMENT_PENDING_EMBEDDINGS");
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Queued CleansedDataStore ID: {} for embedding backfill.", cleansedDataEntry.getId());
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

    void updateFinalCleansedDataStatus(CleansedDataStore cleansedDataEntry, String finalStatus) {
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

        cleansedDataEntry.setStatus(finalStatus);
        cleansedDataStoreRepository.save(cleansedDataEntry);
        logger.info("Finished enrichment for CleansedDataStore ID: {}. Final status: {}", cleansedDataEntry.getId(), finalStatus);
    }
}
