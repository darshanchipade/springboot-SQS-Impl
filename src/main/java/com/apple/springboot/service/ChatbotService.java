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
                    .map(ContentChunkWithDistance::getContentChunk)
                    .map(chunk -> chunk.getConsolidatedEnrichedSection())
                    .map(section -> new ChatbotResultDto(
                            sectionKeyFinal,
                            null,
                            section.getSectionPath(),
                            section.getSectionUri(),
                            section.getCleansedText(),
                            "v_content_chunks"
                    ))
                    .collect(Collectors.toList());

            // Assign cf1, cf2, ... sequentially
            for (int i = 0; i < dtos.size(); i++) {
                dtos.get(i).setCfId("cf" + (i + 1));
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
