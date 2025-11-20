package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

@Service
public class ConsolidatedSectionService {

    private static final Logger logger = LoggerFactory.getLogger(ConsolidatedSectionService.class);

    private final EnrichedContentElementRepository enrichedRepo;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final ContentHashRepository contentHashRepository;
    private final EntityManager entityManager;
    private static final String USAGE_REF_DELIM = " ::ref:: ";

    public ConsolidatedSectionService(EnrichedContentElementRepository enrichedRepo,
                                      ConsolidatedEnrichedSectionRepository consolidatedRepo,
                                      ContentHashRepository contentHashRepository,
                                      EntityManager entityManager) {
        this.enrichedRepo = enrichedRepo;
        this.consolidatedRepo = consolidatedRepo;
        this.contentHashRepository = contentHashRepository;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void saveFromCleansedEntry(CleansedDataStore cleansedData) {
        // Build an index of all usagePaths per (sourcePath, originalFieldName) from the cleansed items
        Map<String, Set<String>> usageIndex = buildUsageIndex(cleansedData);

        List<EnrichedContentElement> enrichedItems = enrichedRepo.findAllByCleansedDataId(cleansedData.getId());
        logger.info("Found {} enriched items for CleansedDataStore ID: {} to consolidate.", enrichedItems.size(), cleansedData.getId());
        List<Object[]> counts = entityManager.createNativeQuery(
                        "select status, count(*) from enriched_content_elements where cleansed_data_id = :id group by status")
                .setParameter("id", cleansedData.getId())
                .getResultList();
        logger.info("Debug: DB counts for {} -> {}", cleansedData.getId(), counts);

        for (EnrichedContentElement item : enrichedItems) {
            if (item.getItemSourcePath() == null || item.getCleansedText() == null) {
                logger.warn("Skipping enriched item ID {} due to null itemSourcePath or cleansedText.", item.getId());
                continue;
            }
            // Fan out per usagePath for this fragment
            String usageKey = usageKey(item.getItemSourcePath(), item.getItemOriginalFieldName());
            Set<String> usagePaths = usageIndex.getOrDefault(usageKey, Collections.singleton(extractUsagePath(item)));

            for (String usagePath : usagePaths) {
                String[] split = splitUsagePath(usagePath);
                String sectionPath = split[0];
                String sectionUri  = split[1];

                if (sectionPath == null) sectionPath = item.getItemSourcePath();
                if (sectionUri  == null) sectionUri  = item.getItemSourcePath();

                // Prevent duplicate insert within the same version
                boolean exists = consolidatedRepo.existsBySectionUriAndSectionPathAndOriginalFieldNameAndCleansedTextAndVersion(
                        sectionUri, sectionPath, item.getItemOriginalFieldName(), item.getCleansedText(), cleansedData.getVersion()
                );

                if (!exists) {
                    ConsolidatedEnrichedSection section = new ConsolidatedEnrichedSection();
                    section.setCleansedDataId(cleansedData.getId());
                    section.setVersion(cleansedData.getVersion());
                    section.setSourceUri(item.getSourceUri());
                    section.setSectionPath(sectionPath);
                    section.setSectionUri(sectionUri);
                    section.setOriginalFieldName(item.getItemOriginalFieldName());
                    section.setCleansedText(item.getCleansedText());

                    // Prefer hash keyed by usagePath; fall back to legacy two-key lookup if absent
                    contentHashRepository
                            .findBySourcePathAndItemTypeAndUsagePath(item.getItemSourcePath(), item.getItemOriginalFieldName(), usagePath)
                            .or(() -> contentHashRepository.findBySourcePathAndItemType(item.getItemSourcePath(), item.getItemOriginalFieldName()))
                            .ifPresent(contentHash -> section.setContentHash(contentHash.getContentHash()));

                    section.setSummary(item.getSummary());
                    section.setClassification(item.getClassification());
                    section.setKeywords(item.getKeywords());
                    section.setTags(item.getTags());
                    section.setSentiment(item.getSentiment());
                    section.setModelUsed(item.getBedrockModelUsed());
                    section.setEnrichmentMetadata(item.getEnrichmentMetadata());
                    section.setEnrichedAt(item.getEnrichedAt());
                    section.setContext(item.getContext());
                    section.setSavedAt(OffsetDateTime.now());
                    section.setStatus(item.getStatus());

                    consolidatedRepo.save(section);
                    logger.info("Saved new ConsolidatedEnrichedSection ID {} for usagePath '{}' from EnrichedContentElement ID {}", section.getId(), usagePath, item.getId());
                } else {
                    logger.info("ConsolidatedEnrichedSection already exists for (uri={}, path={}, field={}, ver={}); skipping insert.",
                            sectionUri, sectionPath, item.getItemOriginalFieldName(), cleansedData.getVersion());
                }
            }
        }
    }

    @Transactional(readOnly = true)
    public List<ConsolidatedEnrichedSection> getSectionsFor(CleansedDataStore cleansedData) {
        if (cleansedData == null || cleansedData.getId() == null) {
            return Collections.emptyList();
        }
        return consolidatedRepo.findAllByCleansedDataId(cleansedData.getId());
    }

    @SuppressWarnings("unchecked")
    private String extractUsagePath(EnrichedContentElement item) {
        Map<String, Object> ctx = item.getContext();
        if (ctx != null) {
            Object envObj = ctx.get("envelope");
            if (envObj instanceof Map<?, ?> env) {
                Object up = env.get("usagePath");
                if (up instanceof String s && !s.isBlank()) {
                    return s;
                }
            }
        }
        return item.getItemSourcePath();
    }

    private String[] splitUsagePath(String usagePath) {
        if (usagePath == null || usagePath.isBlank()) return new String[]{null, null};
        int idx = usagePath.indexOf(USAGE_REF_DELIM);
        if (idx < 0) return new String[]{usagePath, usagePath};
        String left = usagePath.substring(0, idx).trim();
        String right = usagePath.substring(idx + USAGE_REF_DELIM.length()).trim();
        return new String[]{left.isEmpty() ? null : left, right.isEmpty() ? null : right};
    }

    private Map<String, Set<String>> buildUsageIndex(CleansedDataStore cleansedData) {
        Map<String, Set<String>> index = new HashMap<>();
        if (cleansedData == null || cleansedData.getCleansedItems() == null) return index;
        for (Map<String, Object> item : cleansedData.getCleansedItems()) {
            try {
                String sourcePath = (String) item.get("sourcePath");
                String originalFieldName = (String) item.get("originalFieldName");
                String usagePath = (String) item.get("usagePath");
                if (sourcePath == null || originalFieldName == null || usagePath == null) continue;
                String key = usageKey(sourcePath, originalFieldName);
                index.computeIfAbsent(key, k -> new HashSet<>()).add(usagePath);
            } catch (ClassCastException ignored) { /* skip malformed entries */ }
        }
        return index;
    }

    private String usageKey(String sourcePath, String originalFieldName) {
        return sourcePath + "\u0001" + originalFieldName;
    }
}