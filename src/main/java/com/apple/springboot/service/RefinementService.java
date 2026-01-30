package com.apple.springboot.service;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.RefinementChip;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class RefinementService {

    @Autowired
    private VectorSearchService vectorSearchService;

    @Autowired
    private ObjectMapper objectMapper;

    /**
     * Generates refinement chips by analyzing semantically similar content chunks.
     */
    public List<RefinementChip> getRefinementChips(String query) throws IOException {
        // Perform a pure semantic search with a balanced threshold to get relevant documents.
        Double threshold = 0.9;
        List<ContentChunkWithDistance> initialChunks = vectorSearchService.search(query, null,20, null, null, null, threshold,null);

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

//            if (section.getOriginalFieldName() != null) {
//                String original_field_name= section.getOriginalFieldName();
//                    RefinementChip chip = new RefinementChip(original_field_name, "original_field_name", 0);
//                    chipScores.merge(chip, score, Double::sum);
//            }

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
                extractContextChips(contextNode.path("facets"), List.of("sectionKey", "eventType"), "facets", chipScores, score);
                extractContextChips(contextNode.path("envelope"), List.of("sectionName", "locale", "country"), "envelope", chipScores, score);
            }
        }

        // Get the count for each chip for display
        Map<RefinementChip, Long> chipCounts = initialChunks.stream()
                .map(chunk -> chunk.getContentChunk().getConsolidatedEnrichedSection())
                .filter(Objects::nonNull)
                .flatMap(section -> extractChipsForCounting(section).stream())
                .collect(Collectors.groupingBy(chip -> chip, Collectors.counting()));


        return chipScores.entrySet().stream()
                .sorted(Map.Entry.<RefinementChip, Double>comparingByValue().reversed())
                .limit(10)
                .map(entry -> {
                    RefinementChip chip = entry.getKey();
                    chip.setCount(chipCounts.getOrDefault(chip, 0L).intValue());
                    return chip;
                })
                .collect(Collectors.toList());
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
        if (section.getTags() != null) {
            section.getTags().forEach(tag -> chips.add(new RefinementChip(tag, "Tag", 0)));
        }
        if (section.getKeywords() != null) {
            section.getKeywords().forEach(keyword -> chips.add(new RefinementChip(keyword, "Keyword", 0)));
        }
        if (section.getContext() != null) {
            JsonNode contextNode = objectMapper.valueToTree(section.getContext());
            extractContextChipsForCounting(contextNode.path("facets"), List.of("sectionModel", "eventType"), "facets", chips);
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
     * Converts a vector distance into a normalized similarity score.
     */
    private double similarityFromDistance(double d) {
        // Works for Euclidean distance: in (0, +inf)
        // Maps to (0,1]; closer → higher similarity.
        if (Double.isNaN(d) || d < 0) return 0.0;
        return 1.0 / (1.0 + d);
    }
}