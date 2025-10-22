package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ChatbotService {
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;

    private static final Pattern SECTION_KEY_PATTERN = Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");

    public ChatbotService(VectorSearchService vectorSearchService,
                          ConsolidatedEnrichedSectionRepository consolidatedRepo) {
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
    }

    public List<ChatbotResultDto> query(ChatbotRequest request) {
        String key = request != null ? normalizeKey(request.getSectionKey()) : null;
        if (!StringUtils.hasText(key)) {
            key = extractKey(request != null ? request.getMessage() : null);
        }
        if (!StringUtils.hasText(key)) {
            return List.of();
        }
        int limit = (request != null && request.getLimit() != null && request.getLimit() > 0)
                ? Math.min(request.getLimit(), 200)
                : 15;

        try {
            // Use full user message as the embedding query when available; fallback to key
            String embeddingQuery = (request != null && StringUtils.hasText(request.getMessage()))
                    ? request.getMessage()
                    : key;

            List<ContentChunkWithDistance> results = vectorSearchService.search(
                    embeddingQuery,
                    request != null ? request.getOriginal_field_name() : null,
                    limit,
                    request != null ? request.getTags() : null,
                    request != null ? request.getKeywords() : null,
                    request != null ? request.getContext() : null,
                    null
            );

            final String sectionKeyFinal = key;
            // Map results to the requested flat format, with cf_id cf1, cf2, ...
            List<ChatbotResultDto> vectorDtos = results.stream()
                    .map(r -> {
                        var chunk = r.getContentChunk();
                        var section = chunk.getConsolidatedEnrichedSection();
                        ChatbotResultDto dto = new ChatbotResultDto();
                        dto.setSection(sectionKeyFinal);
                        dto.setSectionPath(section.getSectionPath());
                        dto.setSectionUri(section.getSectionUri());
                        dto.setCleansedText(section.getCleansedText());
                        dto.setSource("content_chunks");
                        dto.setContentRole(section.getOriginalFieldName());
                        dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                        dto.setMatchTerms(java.util.List.of(sectionKeyFinal));
                        return dto;
                    })
                    .collect(Collectors.toList());

            // Also pull matches from consolidated table by searching metadata only (avoid matching large cleansed_text)
            List<ConsolidatedEnrichedSection> consolidatedMatches;
            if (request != null && StringUtils.hasText(request.getMessage())) {
                consolidatedMatches = consolidatedRepo.findByMetadataQuery(request.getMessage(), limit);
            } else {
                consolidatedMatches = consolidatedRepo.findBySectionKey(sectionKeyFinal, limit);
            }

            // If caller provided original_field_name, filter consolidated results to that role
            if (request != null && StringUtils.hasText(request.getOriginal_field_name())) {
                final String roleFilter = request.getOriginal_field_name().toLowerCase();
                consolidatedMatches = consolidatedMatches.stream()
                        .filter(s -> s.getOriginalFieldName() != null && s.getOriginalFieldName().toLowerCase().contains(roleFilter))
                        .collect(java.util.stream.Collectors.toList());
            }
            List<ChatbotResultDto> consolidatedDtos = consolidatedMatches.stream()
                    .map(section -> {
                        ChatbotResultDto dto = new ChatbotResultDto();
                        dto.setSection(sectionKeyFinal);
                        dto.setSectionPath(section.getSectionPath());
                        dto.setSectionUri(section.getSectionUri());
                        dto.setCleansedText(section.getCleansedText());
                        dto.setSource("consolidated_enriched_sections");
                        dto.setContentRole(section.getOriginalFieldName());
                        dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                        dto.setMatchTerms(java.util.List.of(sectionKeyFinal));
                        return dto;
                    })
                    .collect(Collectors.toList());

            // Merge results with stable order: vector first, then consolidated; dedupe by (sectionPath, contentRole)
            LinkedHashMap<String, ChatbotResultDto> merged = new LinkedHashMap<>();
            for (ChatbotResultDto d : vectorDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath()) + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }
            for (ChatbotResultDto d : consolidatedDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath()) + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }

            List<ChatbotResultDto> mergedList = new ArrayList<>(merged.values());
            // Assign cf ids sequentially and enrich match terms from request
            for (int i = 0; i < mergedList.size(); i++) {
                ChatbotResultDto item = mergedList.get(i);
                item.setCfId("cf" + (i + 1));
                // Include key and any provided tags/keywords as match terms
                java.util.LinkedHashSet<String> terms = new java.util.LinkedHashSet<>();
                terms.add(sectionKeyFinal);
                if (request != null && request.getTags() != null) terms.addAll(request.getTags());
                if (request != null && request.getKeywords() != null) terms.addAll(request.getKeywords());
                item.setMatchTerms(new java.util.ArrayList<>(terms));
            }
            return mergedList;
        } catch (Exception e) {
            return List.of();
        }
    }

    private String extractKey(String message) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        Matcher m = SECTION_KEY_PATTERN.matcher(message);
        if (m.find()) {
            return normalizeKey(m.group(0));
        }
        for (String token : message.split("\\s+")) {
            long hyphens = token.chars().filter(ch -> ch == '-').count();
            if (hyphens >= 2) {
                return normalizeKey(token);
            }
        }
        return null;
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) return null;
        return key.trim().toLowerCase();
    }
}
