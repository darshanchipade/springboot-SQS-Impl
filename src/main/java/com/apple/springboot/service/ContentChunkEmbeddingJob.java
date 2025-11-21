package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentChunkRepository;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
public class ContentChunkEmbeddingJob {

    private static final Logger logger = LoggerFactory.getLogger(ContentChunkEmbeddingJob.class);

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final ContentChunkRepository contentChunkRepository;
    private final EnrichmentFinalizationService enrichmentFinalizationService;
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final TextChunkingService textChunkingService;
    private final EnrichmentProgressService progressService;
    private final RateLimiter bedrockRateLimiter;
    private final RateLimiter embedRateLimiter;
    private final ConsolidatedSectionService consolidatedSectionService;

    private final boolean embeddingsEnabled;
    private final int batchSize;

    public ContentChunkEmbeddingJob(CleansedDataStoreRepository cleansedDataStoreRepository,
                                    ContentChunkRepository contentChunkRepository,
                                    EnrichmentFinalizationService enrichmentFinalizationService,
                                    BedrockEnrichmentService bedrockEnrichmentService,
                                    Text individualTextChunk holds? needs consolidation?
