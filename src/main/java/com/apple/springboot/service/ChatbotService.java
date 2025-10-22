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

    private static final Pattern SECTION_KEY_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");

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

        boolean hasRoleQuery = request != null && StringUtils.hasText(request.getOriginal_field_name());
        int vectorPreLimit = hasRoleQuery ? Math.min(limit * 3, 100) : limit;

        try {
            String message = request != null ? request.getMessage() : null;
            String embeddingQuery = StringUtils.hasText(message) ? message : key;

            // Vector: do NOT hard-filter by original_field_name to allow partial queries
            List<ContentChunkWithDistance> results = vectorSearchService.search(
                    embeddingQuery,
                    null, // avoid strict equality in SQL for partial role queries
                    vectorPreLimit,
                    request != null ? request.getTags() : null,
                    request != null ? request.getKeywords() : null,
                    request != null ? request.getContext() : null,
                    null // threshold
            );

            final String sectionKeyFinal = key;

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
                        dto.setMatchTerms(List.of(sectionKeyFinal));
                        return dto;
                    })
                    .collect(Collectors.toList());

            // Post-filter vector by partial role (case-insensitive) when provided
            if (hasRoleQuery) {
                String roleFilter = request.getOriginal_field_name().toLowerCase();
                vectorDtos = vectorDtos.stream()
                        .filter(d -> d.getContentRole() != null && d.getContentRole().toLowerCase().contains(roleFilter))
                        .collect(Collectors.toList());
            }

            // Consolidated: search metadata (incl. context usagePath) using full message if available; otherwise section key
            List<ConsolidatedEnrichedSection> consolidatedMatches;
            if (StringUtils.hasText(message)) {
                consolidatedMatches = consolidatedRepo.findByMetadataQuery(message, Math.max(limit * 2, 50));
            } else {
                consolidatedMatches = consolidatedRepo.findBySectionKey(sectionKeyFinal, Math.max(limit * 2, 50));
            }

            // Partial role filtering (contains, case-insensitive) for consolidated
            if (hasRoleQuery) {
                final String roleFilter = request.getOriginal_field_name().toLowerCase();
                consolidatedMatches = consolidatedMatches.stream()
                        .filter(s -> s.getOriginalFieldName() != null
                                && s.getOriginalFieldName().toLowerCase().contains(roleFilter))
                        .collect(Collectors.toList());
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
                        dto.setMatchTerms(List.of(sectionKeyFinal));
                        return dto;
                    })
                    .collect(Collectors.toList());

            // Merge vector-first, then consolidated; dedupe by section_path + content_role
            LinkedHashMap<String, ChatbotResultDto> merged = new LinkedHashMap<>();
            for (ChatbotResultDto d : vectorDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath())
                        + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }
            for (ChatbotResultDto d : consolidatedDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath())
                        + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }

            List<ChatbotResultDto> mergedList = new ArrayList<>(merged.values());

            // Trim to limit after filtering
            if (mergedList.size() > limit) {
                mergedList = mergedList.subList(0, limit);
            }

            // Assign cf ids; enrich match_terms with tags/keywords and role if provided
            for (int i = 0; i < mergedList.size(); i++) {
                ChatbotResultDto item = mergedList.get(i);
                item.setCfId("cf" + (i + 1));

                var terms = new java.util.LinkedHashSet<String>();
                terms.add(sectionKeyFinal);
                if (hasRoleQuery) terms.add(request.getOriginal_field_name());
                if (request != null && request.getTags() != null) terms.addAll(request.getTags());
                if (request != null && request.getKeywords() != null) terms.addAll(request.getKeywords());
                item.setMatchTerms(new ArrayList<>(terms));
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