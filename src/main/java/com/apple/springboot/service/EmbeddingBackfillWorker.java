package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Periodically backfills missing embeddings by chunking consolidated sections and persisting vectors.
 */
@Service
public class EmbeddingBackfillWorker {

    private static final Logger logger = LoggerFactory.getLogger(EmbeddingBackfillWorker.class);

    private final ConsolidatedEnrichedSectionRepository sectionRepository;
    private final ContentChunkRepository contentChunkRepository;
    private final TextChunkingService textChunkingService;
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final EnrichmentFinalizationService finalizationService;
    private final ConsolidatedSectionService consolidatedSectionService;
    private final RateLimiter bedrockRateLimiter;
    private final RateLimiter embedRateLimiter;
    private final int batchSize;
    private final boolean workerEnabled;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Wires the worker with all collaborators and runtime configuration.
     */
    public EmbeddingBackfillWorker(ConsolidatedEnrichedSectionRepository sectionRepository,
                                   ContentChunkRepository contentChunkRepository,
                                   TextChunkingService textChunkingService,
                                   BedrockEnrichmentService bedrockEnrichmentService,
                                   EnrichmentFinalizationService finalizationService,
                                   ConsolidatedSectionService consolidatedSectionService,
                                   @Qualifier("bedrockRateLimiter") RateLimiter bedrockRateLimiter,
                                   @Qualifier("embedRateLimiter") RateLimiter embedRateLimiter,
                                   @Value("${app.embedding.worker.batch-size:5}") int batchSize,
                                   @Value("${app.embedding.worker.enabled:true}") boolean workerEnabled) {
        this.sectionRepository = sectionRepository;
        this.contentChunkRepository = contentChunkRepository;
        this.textChunkingService = textChunkingService;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.finalizationService = finalizationService;
        this.consolidatedSectionService = consolidatedSectionService;
        this.bedrockRateLimiter = bedrockRateLimiter;
        this.embedRateLimiter = embedRateLimiter;
        this.batchSize = Math.max(1, batchSize);
        this.workerEnabled = workerEnabled;
    }

    /**
     * Scheduled entry point that locks and processes sections needing embeddings.
     */
    @Scheduled(fixedDelayString = "${app.embedding.worker.poll-delay-ms:5000}")
    @Transactional
    public void pollPendingSections() {
        if (!workerEnabled) {
            return;
        }
        if (!running.compareAndSet(false, true)) {
            logger.debug("Embedding backfill already running; skipping tick.");
            return;
        }
        try {
            processBatch();
        } finally {
            running.set(false);
        }
    }

    /**
     * Dequeues a batch of sections, embeds them, and updates finalization status.
     */
    private void processBatch() {
        List<ConsolidatedEnrichedSection> sections = sectionRepository.lockSectionsMissingEmbeddings(batchSize);
        if (sections.isEmpty()) {
            logger.debug("No sections pending embeddings at this time.");
            return;
        }
        logger.info("Embedding worker picked up {} sections for processing.", sections.size());
        Map<UUID, List<ConsolidatedEnrichedSection>> sectionsByStore =
                sections.stream().collect(Collectors.groupingBy(ConsolidatedEnrichedSection::getCleansedDataId));

        for (ConsolidatedEnrichedSection section : sections) {
            boolean success = embedSection(section);
            if (success) {
                consolidatedSectionService.markSectionEmbedded(section.getId());
            } else {
                consolidatedSectionService.resetSectionToPending(section.getId());
            }
        }

        for (UUID cleansedId : sectionsByStore.keySet()) {
            long remaining = consolidatedSectionService.countSectionsMissingEmbeddings(cleansedId);
            if (remaining == 0) {
                finalizationService.markEmbeddingsComplete(cleansedId);
            }
        }
    }

    /**
     * Embeds a single section by chunking text, generating vectors, and storing chunks.
     */
    private boolean embedSection(ConsolidatedEnrichedSection section) {
        List<String> chunks = textChunkingService.chunkIfNeeded(section.getCleansedText());
        if (chunks.isEmpty()) {
            logger.info("Section {} has no text to embed; marking as complete.", section.getId());
            return true;
        }
        List<ContentChunk> chunkBatch = new ArrayList<>(chunks.size());
        for (String chunkText : chunks) {
            try {
                throttleFor(bedrockRateLimiter, embedRateLimiter);
                float[] vector = bedrockEnrichmentService.generateEmbedding(chunkText);
                ContentChunk contentChunk = ContentChunk.builder()
                        .consolidatedEnrichedSection(section)
                        .chunkText(chunkText)
                        .sourceField(section.getSourceUri())
                        .sectionPath(section.getSectionPath())
                        .vector(vector)
                        .createdAt(OffsetDateTime.now())
                        .createdBy("EmbeddingBackfillWorker")
                        .build();
                chunkBatch.add(contentChunk);
            } catch (Exception e) {
                logger.error("Failed to embed text for section {}: {}", section.getId(), e.getMessage(), e);
                return false;
            }
        }
        try {
            contentChunkRepository.saveAll(chunkBatch);
            return true;
        } catch (Exception e) {
            logger.error("Failed to persist content chunks for section {}: {}", section.getId(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * Applies the configured rate limiters sequentially.
     */
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
