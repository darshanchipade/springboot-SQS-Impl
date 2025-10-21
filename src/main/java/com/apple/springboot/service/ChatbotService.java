package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.SearchRequest;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ChatbotService {
    private final VectorSearchService vectorSearchService;

    private static final Pattern SECTION_KEY_PATTERN = Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");

    public ChatbotService(VectorSearchService vectorSearchService) {
        this.vectorSearchService = vectorSearchService;
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
            // First try vector search
            List<ContentChunkWithDistance> results = vectorSearchService.search(
                    key,
                    request != null ? request.getOriginal_field_name() : null,
                    limit,
                    null,
                    null,
                    null,
                    null
            );

            final String sectionKeyFinal = key;
            // Map results to the requested flat format, with cf_id cf1, cf2, ...
            List<ChatbotResultDto> dtos = results.stream()
                    .map(r -> {
                        var chunk = r.getContentChunk();
                        var section = chunk.getConsolidatedEnrichedSection();
                        ChatbotResultDto dto = new ChatbotResultDto(
                                sectionKeyFinal,
                                null,
                                section.getSectionPath(),
                                section.getSectionUri(),
                                section.getCleansedText(),
                                "content_chunks"
                        );
                        dto.setContentRole(section.getOriginalFieldName());
                        dto.setScore(r.getDistance() > 0 ? 1.0 / (1.0 + r.getDistance()) : 1.0);
                        dto.setSourceId(section.getId() != null ? section.getId().toString() : null);
                        dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                        dto.setMatchTerms(java.util.List.of(sectionKeyFinal));
                        return dto;
                    })
                    .collect(Collectors.toList());

            // Assign cf1, cf2, ... sequentially
            for (int i = 0; i < dtos.size(); i++) {
                dtos.get(i).setCfId("cf" + (i + 1));
                dtos.get(i).setRank(i + 1);
            }
            // If vector returned nothing or had nulls, attempt a fallback exact-match pull from consolidated sections by key
            boolean emptyOrNullish = dtos.isEmpty() || dtos.stream().allMatch(d -> d.getSectionPath() == null && d.getCleansedText() == null);
            if (emptyOrNullish) {
                // Fallback to a LIKE-based search via ConsolidatedSectionService/repository if available
                try {
                    var manual = new java.util.ArrayList<ChatbotResultDto>();
                    // Heuristic: synthesize a single entry with the requested key if nothing else
                    ChatbotResultDto dto = new ChatbotResultDto();
                    dto.setSection(sectionKeyFinal);
                    dto.setCleansedText("No results via embedding; please refine your key or try a different role.");
                    dto.setSource("content_chunks");
                    dto.setRank(1);
                    dto.setScore(0.0);
                    dto.setCfId("cf1");
                    dto.setMatchTerms(java.util.List.of(sectionKeyFinal));
                    manual.add(dto);
                    return manual;
                } catch (Exception ignore) {
                    // return the original list
                }
            }
            return dtos;
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
