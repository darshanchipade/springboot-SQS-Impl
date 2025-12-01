package com.apple.springboot.service;

import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.transaction.Transactional;
import java.util.Map;

/**
 * Provides utilities for synchronizing context metadata between enriched elements and consolidated sections.
 */
@Service
public class ContextUpdateService {

    private static final Logger logger = LoggerFactory.getLogger(ContextUpdateService.class);

    private final EnrichedContentElementRepository enrichedRepo;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;

    /**
     * Creates the service with the repositories that can be updated in tandem.
     */
    public ContextUpdateService(EnrichedContentElementRepository enrichedRepo,
                                ConsolidatedEnrichedSectionRepository consolidatedRepo) {
        this.enrichedRepo = enrichedRepo;
        this.consolidatedRepo = consolidatedRepo;
    }

//    @Transactional
//    public void updateContext(String sourcePath, String itemType, Map<String, Object> context) {
//        enrichedRepo.findByItemSourcePathAndItemOriginalFieldName(sourcePath, itemType).ifPresent(enrichedContentElement -> {
//            enrichedContentElement.setContext(context);
//            enrichedRepo.save(enrichedContentElement);
//            logger.info("Updated context for enriched content element with sourcePath: {} and itemType: {}", sourcePath, itemType);
//        });
//
//        consolidatedRepo.findBySectionPathAndOriginalFieldName(sourcePath, itemType).ifPresent(consolidatedEnrichedSection -> {
//            consolidatedEnrichedSection.setContext(context);
//            consolidatedRepo.save(consolidatedEnrichedSection);
//            logger.info("Updated context for consolidated enriched section with sourcePath: {} and itemType: {}", sourcePath, itemType);
//        });
//    }
}