package com.apple.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class EnrichmentProgressService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentProgressService.class);

    private static class Progress {
        final int expected;
        final AtomicInteger processed = new AtomicInteger(0);

        /**
         * Creates a progress tracker with the expected item count.
         */
        Progress(int expected) {
            this.expected = expected;
        }
    }

    private final ConcurrentHashMap<UUID, Progress> progressMap = new ConcurrentHashMap<>();

    /**
     * Starts tracking progress for a cleansed record.
     */
    public void startTracking(UUID id, int expected) {
        if (id == null || expected <= 0) {
            return;
        }
        progressMap.put(id, new Progress(expected));
        logger.info("Enrichment progress tracking started for {} ({} total items).", id, expected);
    }

    /**
     * Increments the processed counter and logs remaining items.
     */
    public void increment(UUID id, String label) {
        if (id == null) {
            return;
        }
        Progress progress = progressMap.get(id);
        if (progress == null) {
            return;
        }
        int processed = progress.processed.incrementAndGet();
        int remaining = Math.max(progress.expected - processed, 0);
        logger.info("Enrichment progress for {}: processed {}/{} ({} remaining){}",
                id,
                processed,
                progress.expected,
                remaining,
                label != null ? " – " + label : "");
    }

    /**
     * Marks tracking complete and removes the entry from memory.
     */
    public void complete(UUID id) {
        if (id == null) {
            return;
        }
        Progress progress = progressMap.remove(id);
        if (progress != null) {
            logger.info("Enrichment progress completed for {} (processed {}/{}).",
                    id,
                    progress.processed.get(),
                    progress.expected);
        }
    }
}
