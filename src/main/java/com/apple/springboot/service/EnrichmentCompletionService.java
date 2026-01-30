package com.apple.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class EnrichmentCompletionService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentCompletionService.class);

    private final ConcurrentHashMap<UUID, AtomicInteger> completionCounters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<UUID, Integer> expectedCounters = new ConcurrentHashMap<>();

    /**
     * Initializes the completion counter for a new enrichment job.
     * This should be called when the enrichment process for a CleansedDataStore begins.
     *
     * @param cleansedDataStoreId The ID of the CleansedDataStore being processed.
     * @param totalItems The total number of items to be enriched for this data store.
     */
    public void startTracking(UUID cleansedDataStoreId, int totalItems) {
        if (totalItems <= 0) {
            logger.warn("Not tracking completion for CleansedDataStore ID {} because it has no items.", cleansedDataStoreId);
            return;
        }
        completionCounters.put(cleansedDataStoreId, new AtomicInteger(totalItems));
        expectedCounters.put(cleansedDataStoreId, totalItems);
        logger.info("Started tracking completion for CleansedDataStore ID {}. Expecting {} items.", cleansedDataStoreId, totalItems);
    }

    /**
     * Records that an item has been processed and checks if the entire job is complete.
     *
     * @param cleansedDataStoreId The ID of the CleansedDataStore to which the item belongs.
     * @return true if this was the last item and the job is now complete, false otherwise.
     */
    public boolean itemCompleted(UUID cleansedDataStoreId) {
        AtomicInteger counter = completionCounters.get(cleansedDataStoreId);
        if (counter == null) {
            logger.warn("Received a completion signal for an untracked CleansedDataStore ID: {}. This may indicate a logic error.", cleansedDataStoreId);
            return false;
        }

        int remaining = counter.decrementAndGet();
        logger.debug("Item completed for {}. {} items remaining.", cleansedDataStoreId, remaining);

        if (remaining == 0) {
            logger.info("All items for CleansedDataStore ID {} have been processed.", cleansedDataStoreId);
            completionCounters.remove(cleansedDataStoreId); // Clean up the map
            expectedCounters.remove(cleansedDataStoreId);
            return true;
        }

        if (remaining < 0) {
            logger.warn("Completion counter for CleansedDataStore ID {} has gone below zero. This indicates a potential logic error.", cleansedDataStoreId);
        }

        return false;
    }

    /**
     * Checks whether completion tracking is active for a given cleansed record.
     */
    public boolean isTracking(UUID cleansedDataStoreId) {
        boolean tracking = completionCounters.containsKey(cleansedDataStoreId);
        if (!tracking) {
            logger.warn("Completion tracking absent for CleansedDataStore ID {} when queried.", cleansedDataStoreId);
        }
        return tracking;
    }

    /**
     * Returns the number of remaining items, or -1 if not tracked.
     */
    public int getRemainingCount(UUID cleansedDataStoreId) {
        AtomicInteger counter = completionCounters.get(cleansedDataStoreId);
        return counter != null ? counter.get() : -1;
    }

    /**
     * Returns the expected item count, or -1 if not tracked.
     */
    public int getExpectedCount(UUID cleansedDataStoreId) {
        return expectedCounters.getOrDefault(cleansedDataStoreId, -1);
    }

    /**
     * Forces completion tracking to end and returns whether it was active.
     */
    public boolean forceComplete(UUID cleansedDataStoreId) {
        AtomicInteger remaining = completionCounters.remove(cleansedDataStoreId);
        expectedCounters.remove(cleansedDataStoreId);
        if (remaining != null) {
            logger.warn("Force-completing tracking for CleansedDataStore ID {} with {} items still recorded as remaining.",
                    cleansedDataStoreId, remaining.get());
            return true;
        }
        return false;
    }
}