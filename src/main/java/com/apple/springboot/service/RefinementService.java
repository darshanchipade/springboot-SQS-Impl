package com.apple.springboot.service;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.RefinementChip;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class RefinementService {

    @Autowired
    private VectorSearchService vectorSearchService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ConsolidatedEnrichedSectionRepository consolidatedRepo;

    private static final Pattern SECTION_KEY_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");
    private static final double SECTION_KEY_SCORE_WEIGHT = 0.2;

    /**
     * Generates refinement chips by analyzing semantically similar content chunks.
     */
    public List<RefinementChip> getRefinementChips(String query) throws IOException {
        // Perform a semantic search across a broader candidate set for chip coverage.
        Double threshold = null;
        int initialLimit = 50;
        List<ContentChunkWithDistance> initialChunks = vectorSearchService.search(query, null, initialLimit, null, null, null, threshold, null);

        if (initialChunks.isEmpty()) {
            return Collections.emptyList();
        }

        Map<RefinementChip, Double> chipScores = new HashMap<>();

        for (ContentChunkWithDistance chunkWithDistance : initialChunks) {
            double distance = chunkWithDistance.getDistance();
            double score = similarityFromDistance(distance);
            if (score <= 0) continue;


            ConsolidatedEnrichedSection section = chunkWithDistance.getContentChunk().getConsolidatedEnrichedSection();
            if (section == null) continue;

            String originalFieldName = section.getOriginalFieldName();
            if (StringUtils.hasText(originalFieldName)) {
                RefinementChip chip = new RefinementChip(originalFieldName.trim(), "sectionName", 0);
                chipScores.merge(chip, score, Double::sum);
            }

            // Extract Tags
            if (section.getTags() != null) {
                section.getTags().forEach(tag -> {
                    RefinementChip chip = new RefinementChip(tag, "Tag", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }
            // Extract Keywords
            if (section.getKeywords() != null) {
                section.getKeywords().forEach(keyword -> {
                    RefinementChip chip = new RefinementChip(keyword, "Keyword", 0);
                    chipScores.merge(chip, score, Double::sum);
                });
            }

            // Extract from nested context based on simplified requirements
            if (section.getContext() != null) {
                JsonNode contextNode = objectMapper.valueToTree(section.getContext());
                extractContextChips(contextNode.path("facets"), List.of("sectionKey", "sectionName", "eventType"), "facets", chipScores, score);
                extractContextChips(contextNode.path("envelope"), List.of("sectionName", "locale", "country"), "envelope", chipScores, score);
            }
        }

        List<ConsolidatedEnrichedSection> supplementalSections = loadSectionsForQuery(query);
        if (!supplementalSections.isEmpty()) {
            mergeChipsFromSections(supplementalSections, chipScores, SECTION_KEY_SCORE_WEIGHT);
        }

        // Get the count for each chip for display
        Map<RefinementChip, Long> chipCounts = mergeForCounting(initialChunks, supplementalSections).stream()
                .flatMap(section -> extractChipsForCounting(section).stream())
                .collect(Collectors.groupingBy(chip -> chip, Collectors.counting()));


        List<RefinementChip> sortedChips = chipScores.entrySet().stream()
                .sorted(Map.Entry.<RefinementChip, Double>comparingByValue().reversed())
                .map(entry -> {
                    RefinementChip chip = entry.getKey();
                    chip.setCount(chipCounts.getOrDefault(chip, 0L).intValue());
                    return chip;
                })
                .collect(Collectors.toList());
        List<RefinementChip> limited = new ArrayList<>(sortedChips.stream().limit(10).toList());
        ensureTypeIncluded(limited, sortedChips, "sectionName", 10);
        return limited;
    }

    /**
     * Adds context-driven refinement chips to the score map.
     */
    private void extractContextChips(JsonNode parentNode, List<String> keys, String pathPrefix, Map<RefinementChip, Double> chipScores, double score) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                RefinementChip chip = new RefinementChip(valueNode.asText(), "Context:" + pathPrefix + "." + key, 0);
                chipScores.merge(chip, score, Double::sum);
            }
        }
    }

    /**
     * Extracts chips from tags, keywords, and context for counting.
     */
    private List<RefinementChip> extractChipsForCounting(ConsolidatedEnrichedSection section) {
        List<RefinementChip> chips = new ArrayList<>();
        if (StringUtils.hasText(section.getOriginalFieldName())) {
            chips.add(new RefinementChip(section.getOriginalFieldName().trim(), "sectionName", 0));
        }
        if (section.getTags() != null) {
            section.getTags().forEach(tag -> chips.add(new RefinementChip(tag, "Tag", 0)));
        }
        if (section.getKeywords() != null) {
            section.getKeywords().forEach(keyword -> chips.add(new RefinementChip(keyword, "Keyword", 0)));
        }
        if (section.getContext() != null) {
            JsonNode contextNode = objectMapper.valueToTree(section.getContext());
            extractContextChipsForCounting(contextNode.path("facets"), List.of("sectionKey", "sectionName", "sectionModel", "eventType"), "facets", chips);
            extractContextChipsForCounting(contextNode.path("envelope"), List.of("sectionName", "locale", "country"), "envelope", chips);
        }
        return chips;
    }

    /**
     * Appends context chips to a list for count aggregation.
     */
    private void extractContextChipsForCounting(JsonNode parentNode, List<String> keys, String pathPrefix, List<RefinementChip> chips) {
        if (parentNode.isMissingNode()) return;

        for (String key : keys) {
            JsonNode valueNode = parentNode.path(key);
            if (valueNode.isTextual() && !valueNode.asText().isBlank()) {
                chips.add(new RefinementChip(valueNode.asText(), "Context:" + pathPrefix + "." + key, 0));
            }
        }
    }

    /**
     * Loads sections that match section keys extracted from the query.
     */
    private List<ConsolidatedEnrichedSection> loadSectionsForQuery(String query) {
        if (!StringUtils.hasText(query)) {
            return List.of();
        }
        Set<String> sectionKeys = extractSectionKeys(query);
        if (sectionKeys.isEmpty()) {
            return List.of();
        }
        List<ConsolidatedEnrichedSection> matched = new ArrayList<>();
        for (String key : sectionKeys) {
            matched.addAll(consolidatedRepo.findBySectionKey(key, 200));
        }
        return matched;
    }

    /**
     * Extracts section keys from the raw query text.
     */
    private Set<String> extractSectionKeys(String query) {
        Set<String> keys = new LinkedHashSet<>();
        Matcher matcher = SECTION_KEY_PATTERN.matcher(query);
        while (matcher.find()) {
            String key = matcher.group(0);
            if (StringUtils.hasText(key)) {
                keys.add(key.toLowerCase(Locale.ROOT));
            }
        }
        return keys;
    }

    /**
     * Adds chips from matching sections with a base score weight.
     */
    private void mergeChipsFromSections(List<ConsolidatedEnrichedSection> sections,
                                        Map<RefinementChip, Double> chipScores,
                                        double weight) {
        if (sections == null || sections.isEmpty()) {
            return;
        }
        double score = Math.max(0.01, weight);
        for (ConsolidatedEnrichedSection section : sections) {
            if (section == null) {
                continue;
            }
            List<RefinementChip> chips = extractChipsForCounting(section);
            for (RefinementChip chip : chips) {
                chipScores.merge(chip, score, Double::sum);
            }
        }
    }

    /**
     * Merges sections from vector results and supplemental matches for counting.
     */
    private List<ConsolidatedEnrichedSection> mergeForCounting(List<ContentChunkWithDistance> initialChunks,
                                                               List<ConsolidatedEnrichedSection> supplementalSections) {
        LinkedHashMap<UUID, ConsolidatedEnrichedSection> merged = new LinkedHashMap<>();
        if (initialChunks != null) {
            for (ContentChunkWithDistance chunk : initialChunks) {
                ConsolidatedEnrichedSection section = chunk != null && chunk.getContentChunk() != null
                        ? chunk.getContentChunk().getConsolidatedEnrichedSection()
                        : null;
                if (section != null && section.getId() != null) {
                    merged.put(section.getId(), section);
                }
            }
        }
        if (supplementalSections != null) {
            for (ConsolidatedEnrichedSection section : supplementalSections) {
                if (section != null && section.getId() != null) {
                    merged.putIfAbsent(section.getId(), section);
                }
            }
        }
        return new ArrayList<>(merged.values());
    }

    /**
     * Ensures at least one chip of the requested type appears in the limited list.
     */
    private void ensureTypeIncluded(List<RefinementChip> limited,
                                    List<RefinementChip> sortedChips,
                                    String type,
                                    int limit) {
        if (limited == null || sortedChips == null || type == null) {
            return;
        }
        boolean alreadyPresent = limited.stream().anyMatch(chip -> type.equals(chip.getType()));
        if (alreadyPresent) {
            return;
        }
        RefinementChip candidate = sortedChips.stream()
                .filter(chip -> type.equals(chip.getType()))
                .findFirst()
                .orElse(null);
        if (candidate == null) {
            return;
        }
        if (limited.size() < limit) {
            limited.add(candidate);
        } else if (!limited.isEmpty()) {
            limited.set(limited.size() - 1, candidate);
        }
    }
    /**
     * Converts a vector distance into a normalized similarity score.
     */
    private double similarityFromDistance(double d) {
        // Works for Euclidean distance: in (0, +inf)
        // Maps to (0,1]; closer → higher similarity.
        if (Double.isNaN(d) || d < 0) return 0.0;
        return 1.0 / (1.0 + d);
    }
}