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

    private static final int DEFAULT_CHIP_LIMIT = 15;
    private static final int MAX_CHIP_LIMIT = 50;

    @Autowired
    private VectorSearchService vectorSearchService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ConsolidatedEnrichedSectionRepository consolidatedRepo;

    private static final Pattern SECTION_KEY_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");
    private static final double SECTION_KEY_SCORE_WEIGHT = 0.2;
    private static final double ROLE_HINT_SCORE_WEIGHT = 0.15;
    private static final Set<String> ROLE_STOP_WORDS = Set.of(
            "section",
            "sections",
            "for",
            "of",
            "in",
            "on",
            "and",
            "the",
            "a",
            "an",
            "to",
            "with"
    );

    /**
     * Generates refinement chips by analyzing semantically similar content chunks.
     */
    public List<RefinementChip> getRefinementChips(String query) throws IOException {
        return getRefinementChips(query, DEFAULT_CHIP_LIMIT);
    }

    /**
     * Generates refinement chips with a requested limit.
     */
    public List<RefinementChip> getRefinementChips(String query, Integer limit) throws IOException {
        // Perform a semantic search across a broader candidate set for chip coverage.
        Double threshold = null;
        int chipLimit = normalizeLimit(limit);
        int initialLimit = Math.min(Math.max(50, chipLimit * 6), 200);
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
            String sectionKey = extractSectionKeyFromSection(section);
            if (StringUtils.hasText(sectionKey)) {
                RefinementChip chip = new RefinementChip(sectionKey, "sectionKey", 0);
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

        Set<String> sectionKeys = extractSectionKeys(query);
        for (String sectionKey : sectionKeys) {
            RefinementChip chip = new RefinementChip(sectionKey, "sectionKey", 0);
            chipScores.merge(chip, SECTION_KEY_SCORE_WEIGHT, Double::sum);
        }

        Set<String> roleHints = extractRoleHints(query, sectionKeys);
        for (String roleHint : roleHints) {
            RefinementChip chip = new RefinementChip(roleHint, "sectionName", 0);
            chipScores.merge(chip, ROLE_HINT_SCORE_WEIGHT, Double::sum);
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
        List<RefinementChip> limited = new ArrayList<>(sortedChips.stream().limit(chipLimit).toList());
        ensureTypeIncluded(limited, sortedChips, "sectionName", chipLimit);
        ensureTypeIncluded(limited, sortedChips, "sectionKey", chipLimit);
        for (String sectionKey : sectionKeys) {
            ensureValueIncluded(limited, sortedChips, "sectionKey", sectionKey, chipLimit);
        }
        for (String roleHint : roleHints) {
            ensureValueIncluded(limited, sortedChips, "sectionName", roleHint, chipLimit);
        }
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
        String sectionKey = extractSectionKeyFromSection(section);
        if (StringUtils.hasText(sectionKey)) {
            chips.add(new RefinementChip(sectionKey, "sectionKey", 0));
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
     * Extracts role hints from the query text (e.g., "headline").
     */
    private Set<String> extractRoleHints(String query, Set<String> sectionKeys) {
        if (!StringUtils.hasText(query)) {
            return Set.of();
        }
        Set<String> hints = new LinkedHashSet<>();
        String[] tokens = query.split("\\s+");
        for (String token : tokens) {
            if (!StringUtils.hasText(token)) {
                continue;
            }
            String cleaned = token.replaceAll("[^A-Za-z0-9_-]", "").toLowerCase(Locale.ROOT);
            if (!StringUtils.hasText(cleaned)) {
                continue;
            }
            if (sectionKeys != null && sectionKeys.contains(cleaned)) {
                continue;
            }
            if (cleaned.endsWith("-section")) {
                continue;
            }
            if (cleaned.length() < 3) {
                continue;
            }
            if (ROLE_STOP_WORDS.contains(cleaned)) {
                continue;
            }
            hints.add(cleaned);
        }
        return hints;
    }

    /**
     * Extracts the section key from context or section path metadata.
     */
    private String extractSectionKeyFromSection(ConsolidatedEnrichedSection section) {
        if (section == null) {
            return null;
        }
        Map<String, Object> context = section.getContext();
        if (context != null) {
            String fromContext = extractSectionKeyFromContext(context);
            if (StringUtils.hasText(fromContext)) {
                return fromContext;
            }
        }
        String fromPath = extractSectionKeyFromPath(section.getSectionPath());
        if (StringUtils.hasText(fromPath)) {
            return fromPath;
        }
        return extractSectionKeyFromPath(section.getSectionUri());
    }

    private String extractSectionKeyFromContext(Map<String, Object> context) {
        if (context == null) {
            return null;
        }
        Object facetsObj = context.get("facets");
        if (facetsObj instanceof Map<?, ?> facets) {
            Object sectionKey = facets.get("sectionKey");
            if (sectionKey instanceof String s && StringUtils.hasText(s)) {
                return s.toLowerCase(Locale.ROOT);
            }
        }
        Object envelopeObj = context.get("envelope");
        if (envelopeObj instanceof Map<?, ?> envelope) {
            Object sectionKey = envelope.get("sectionKey");
            if (sectionKey instanceof String s && StringUtils.hasText(s)) {
                return s.toLowerCase(Locale.ROOT);
            }
        }
        Object direct = context.get("sectionKey");
        if (direct instanceof String s && StringUtils.hasText(s)) {
            return s.toLowerCase(Locale.ROOT);
        }
        return null;
    }

    private String extractSectionKeyFromPath(String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        Matcher matcher = SECTION_KEY_PATTERN.matcher(path);
        if (matcher.find()) {
            return matcher.group(0).toLowerCase(Locale.ROOT);
        }
        String[] segments = path.split("/");
        if (segments.length > 0) {
            String last = segments[segments.length - 1];
            if (StringUtils.hasText(last) && last.toLowerCase(Locale.ROOT).contains("section")) {
                return last.toLowerCase(Locale.ROOT);
            }
        }
        return null;
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
     * Normalizes the requested chip limit.
     */
    private int normalizeLimit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_CHIP_LIMIT;
        }
        return Math.min(limit, MAX_CHIP_LIMIT);
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
     * Ensures a specific chip value is present when derived from the query.
     */
    private void ensureValueIncluded(List<RefinementChip> limited,
                                     List<RefinementChip> sortedChips,
                                     String type,
                                     String value,
                                     int limit) {
        if (limited == null || sortedChips == null || type == null || value == null) {
            return;
        }
        boolean alreadyPresent = limited.stream()
                .anyMatch(chip -> type.equals(chip.getType()) && value.equalsIgnoreCase(chip.getValue()));
        if (alreadyPresent) {
            return;
        }
        RefinementChip candidate = sortedChips.stream()
                .filter(chip -> type.equals(chip.getType()))
                .filter(chip -> value.equalsIgnoreCase(chip.getValue()))
                .findFirst()
                .orElse(new RefinementChip(value, type, 0));
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