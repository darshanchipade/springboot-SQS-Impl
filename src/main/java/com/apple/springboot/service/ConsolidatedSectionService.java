package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.EmbeddingStatus;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.*;

@Service
public class ConsolidatedSectionService {

    private static final Logger logger = LoggerFactory.getLogger(ConsolidatedSectionService.class);

    private final EnrichedContentElementRepository enrichedRepo;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final ContentHashRepository contentHashRepository;
    private static final String USAGE_REF_DELIM = " ::ref:: ";

    public ConsolidatedSectionService(EnrichedContentElementRepository enrichedRepo,
                                      ConsolidatedEnrichedSectionRepository consolidatedRepo,
                                      ContentHashRepository contentHashRepository) {
        this.enrichedRepo = enrichedRepo;
        this.consolidatedRepo = consolidatedRepo;
        this.contentHashRepository = contentHashRepository;
    }

    @Transactional
    public void saveFromCleansedEntry(CleansedDataStore cleansedData) {
        List<EnrichedContentElement> enrichedItems = enrichedRepo.findAllByCleansedDataId(cleansedData.getId());
        logger.info("Found {} enriched items for CleansedDataStore ID: {} to consolidate.", enrichedItems.size(), cleansedData.getId());

        // Build an index of all usagePaths per (sourcePath, originalFieldName). We prefer harvesting from
        // persisted `content_hashes` so consolidation remains correct even when `cleansed_items` only contains deltas.
        Map<String, Set<String>> usageIndex = buildUsageIndex(cleansedData, enrichedItems);

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

    private Map<String, Set<String>> buildUsageIndex(CleansedDataStore cleansedData,
                                                     List<EnrichedContentElement> enrichedItems) {
        Map<String, Set<String>> index = new HashMap<>();

        // 1) Always include whatever was stored on the cleansed record (could be full or delta).
        if (cleansedData != null && cleansedData.getCleansedItems() != null) {
            for (Map<String, Object> item : cleansedData.getCleansedItems()) {
                try {
                    String sourcePath = (String) item.get("sourcePath");
                    String originalFieldName = (String) item.get("originalFieldName");
                    String usagePath = (String) item.get("usagePath");
                    if (sourcePath == null || originalFieldName == null || usagePath == null) continue;
                    index.computeIfAbsent(usageKey(sourcePath, originalFieldName), k -> new HashSet<>()).add(usagePath);
                } catch (ClassCastException ignored) { /* skip malformed entries */ }
            }
        }

        // 2) Backfill the full fan-out from `content_hashes` for all enriched items (covers delta-cleansed runs).
        if (enrichedItems == null || enrichedItems.isEmpty()) {
            return index;
        }

        Set<String> seenPairs = new HashSet<>();
        for (EnrichedContentElement item : enrichedItems) {
            String sourcePath = item.getItemSourcePath();
            String fieldName = item.getItemOriginalFieldName();
            if (sourcePath == null || fieldName == null) {
                continue;
            }
            String pairKey = usageKey(sourcePath, fieldName);
            if (!seenPairs.add(pairKey)) {
                continue;
            }
            try {
                contentHashRepository.findAllBySourcePathAndItemType(sourcePath, fieldName).stream()
                        .map(ch -> ch.getUsagePath())
                        .filter(Objects::nonNull)
                        .filter(s -> !s.isBlank())
                        .forEach(up -> index.computeIfAbsent(pairKey, k -> new HashSet<>()).add(up));
            } catch (Exception e) {
                logger.debug("Unable to backfill usage paths from content_hashes for {}::{}, falling back to element context.",
                        sourcePath, fieldName, e);
            }
        }

        return index;
    }

    private String usageKey(String sourcePath, String originalFieldName) {
        return sourcePath + "\u0001" + originalFieldName;
    }

    @Transactional
    public void markSectionsPendingEmbedding(UUID cleansedDataId, Integer version) {
        consolidatedRepo.updateStatusForCleansedData(cleansedDataId, version, EmbeddingStatus.SECTION_PENDING);
        consolidatedRepo.updateBlankSectionsStatus(cleansedDataId, version, EmbeddingStatus.SECTION_EMBEDDED);
    }

    @Transactional
    public void markSectionEmbedded(UUID sectionId) {
        consolidatedRepo.updateStatusForSection(sectionId, EmbeddingStatus.SECTION_EMBEDDED);
    }

    @Transactional
    public void resetSectionToPending(UUID sectionId) {
        consolidatedRepo.updateStatusForSection(sectionId, EmbeddingStatus.SECTION_PENDING);
    }

    @Transactional(readOnly = true)
    public long countSectionsMissingEmbeddings(UUID cleansedDataId) {
        return consolidatedRepo.countSectionsMissingEmbeddings(cleansedDataId);
    }
}