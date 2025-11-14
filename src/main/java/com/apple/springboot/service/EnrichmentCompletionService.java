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
            return true;
        }

        if (remaining < 0) {
            logger.warn("Completion counter for CleansedDataStore ID {} has gone below zero. This indicates a potential logic error.", cleansedDataStoreId);
        }

        return false;
    }
    public boolean isTracking(UUID cleansedDataStoreId) {
        return completionCounters.containsKey(cleansedDataStoreId);
    }
}