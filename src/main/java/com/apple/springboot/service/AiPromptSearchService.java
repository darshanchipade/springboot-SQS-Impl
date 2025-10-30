package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class AiPromptSearchService {

    private static final Logger logger = LoggerFactory.getLogger(AiPromptSearchService.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final ObjectMapper objectMapper;

    @Autowired
    public AiPromptSearchService(BedrockEnrichmentService bedrockEnrichmentService,
                                 VectorSearchService vectorSearchService,
                                 ConsolidatedEnrichedSectionRepository consolidatedRepo,
                                 ObjectMapper objectMapper) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
        this.objectMapper = objectMapper;
    }

    public List<ChatbotResultDto> aiSearch(ChatbotRequest request) {
        String userMessage = request != null ? request.getMessage() : null;
        if (!StringUtils.hasText(userMessage)) {
            return List.of();
        }

        String promptTemplate = loadPromptTemplate();
        String prompt = promptTemplate
                .replace("{conversation_history}", "")
                .replace("{user_message}", userMessage);

        JsonNode aiJson = null;
        try {
            String raw = bedrockEnrichmentService.invokeChatForText(prompt, null);
            String cleaned = stripJsonFences(raw);
            aiJson = objectMapper.readTree(cleaned);
        } catch (ThrottledException te) {
            throw te; // surface throttling
        } catch (Exception e) {
            logger.warn("AI prompt parsing failed; falling back to direct semantic search. Error: {}", e.getMessage());
        }

        // Extract structured filters from AI output (if available)
        String query = userMessage;
        List<String> tags = new ArrayList<>();
        List<String> keywords = new ArrayList<>();
        String originalFieldName = null;
        Map<String, Object> context = new HashMap<>();

        if (aiJson != null) {
            if (aiJson.hasNonNull("query") && aiJson.get("query").isTextual()) {
                query = aiJson.get("query").asText(userMessage);
            }
            tags.addAll(readStrings(aiJson.get("tags")));
            keywords.addAll(readStrings(aiJson.get("keywords")));
            if (aiJson.hasNonNull("original_field_name") && aiJson.get("original_field_name").isTextual()) {
                originalFieldName = aiJson.get("original_field_name").asText();
            }
            if (aiJson.hasNonNull("context") && aiJson.get("context").isObject()) {
                context = objectMapper.convertValue(aiJson.get("context"), Map.class);
            } else if (aiJson.hasNonNull("contextPath") && aiJson.hasNonNull("contextValue")) {
                // Support simple path/value pairs like ["envelope","sectionName"] + "breadcrumbs-a11y"
                context = buildContextFromPath(aiJson.get("contextPath"), aiJson.get("contextValue"));
            }
        }

        // Merge with any explicit filters on the request
        if (request != null) {
            if (request.getTags() != null) tags.addAll(request.getTags());
            if (request.getKeywords() != null) keywords.addAll(request.getKeywords());
            if (request.getContext() != null) context.putAll(request.getContext());
            if (StringUtils.hasText(request.getOriginal_field_name())) originalFieldName = request.getOriginal_field_name();
        }

        // Normalize and de-duplicate filters
        tags = tags.stream().filter(StringUtils::hasText).map(String::trim).distinct().collect(Collectors.toList());
        keywords = keywords.stream().filter(StringUtils::hasText).map(String::trim).distinct().collect(Collectors.toList());

        int limit = (request != null && request.getLimit() != null && request.getLimit() > 0)
                ? Math.min(request.getLimit(), 200)
                : 15;

        try {
            // Vector search
            List<ContentChunkWithDistance> vectorResults = vectorSearchService.search(
                    query,
                    originalFieldName,
                    limit,
                    tags.isEmpty() ? null : tags,
                    keywords.isEmpty() ? null : keywords,
                    context.isEmpty() ? null : context,
                    null
            );

            List<ChatbotResultDto> vectorDtos = vectorResults.stream()
                    .map(r -> toDto(r.getContentChunk().getConsolidatedEnrichedSection(), "content_chunks"))
                    .collect(Collectors.toList());

            // Metadata search across consolidated (summary/tags/keywords/paths)
            List<ConsolidatedEnrichedSection> metaMatches = consolidatedRepo.findByMetadataQuery(query, Math.max(limit * 2, 50));
            if (StringUtils.hasText(originalFieldName)) {
                String roleFilter = originalFieldName.toLowerCase();
                metaMatches = metaMatches.stream()
                        .filter(s -> s.getOriginalFieldName() != null && s.getOriginalFieldName().toLowerCase().contains(roleFilter))
                        .collect(Collectors.toList());
            }
            List<ChatbotResultDto> metaDtos = metaMatches.stream()
                    .map(s -> toDto(s, "consolidated_enriched_sections"))
                    .collect(Collectors.toList());

            // Merge, dedupe by section_path + content_role, keep vector-first order
            LinkedHashMap<String, ChatbotResultDto> merged = new LinkedHashMap<>();
            for (ChatbotResultDto d : vectorDtos) {
                merged.putIfAbsent((d.getSectionPath() == null ? "" : d.getSectionPath()) + "|" + (d.getContentRole() == null ? "" : d.getContentRole()), d);
            }
            for (ChatbotResultDto d : metaDtos) {
                merged.putIfAbsent((d.getSectionPath() == null ? "" : d.getSectionPath()) + "|" + (d.getContentRole() == null ? "" : d.getContentRole()), d);
            }

            List<ChatbotResultDto> mergedList = new ArrayList<>(merged.values());
            if (mergedList.size() > limit) {
                mergedList = mergedList.subList(0, limit);
            }

            // Assign cf ids and annotate terms
            for (int i = 0; i < mergedList.size(); i++) {
                ChatbotResultDto item = mergedList.get(i);
                item.setCfId("cf" + (i + 1));
                item.setSection("ai-search");
                Set<String> terms = new LinkedHashSet<>();
                terms.add("ai-search");
                if (StringUtils.hasText(originalFieldName)) terms.add(originalFieldName);
                terms.addAll(tags);
                terms.addAll(keywords);
                item.setMatchTerms(new ArrayList<>(terms));
            }

            return mergedList;
        } catch (Exception e) {
            logger.error("AI search failed: {}", e.getMessage(), e);
            return List.of();
        }
    }

    private ChatbotResultDto toDto(ConsolidatedEnrichedSection section, String source) {
        ChatbotResultDto dto = new ChatbotResultDto();
        dto.setSection("ai-search");
        dto.setSectionPath(section.getSectionPath());
        dto.setSectionUri(section.getSectionUri());
        dto.setCleansedText(section.getCleansedText());
        dto.setSource(source);
        dto.setContentRole(section.getOriginalFieldName());
        dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
        dto.setMatchTerms(List.of("ai-search"));
        // enrich with page_id, tenant, locale
        dto.setLocale(extractLocale(section));
        dto.setTenant(extractTenant(section));
        dto.setPageId(extractPageId(section));
        return dto;
    }

    private String extractLocale(ConsolidatedEnrichedSection s) {
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof java.util.Map<?,?> m) {
                Object loc = m.get("locale");
                if (loc instanceof String str && org.springframework.util.StringUtils.hasText(str)) return str;
            }
        }
        String fromPath = extractLocaleFromPath(s.getSectionUri());
        if (fromPath == null) fromPath = extractLocaleFromPath(s.getSectionPath());
        return fromPath;
    }

    private String extractTenant(ConsolidatedEnrichedSection s) {
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof java.util.Map<?,?> m) {
                Object ten = m.get("tenant");
                if (ten instanceof String str && org.springframework.util.StringUtils.hasText(str)) return str;
            }
        }
        String fromUri = extractTenantFromPath(s.getSectionUri());
        if (fromUri != null) return fromUri;
        String fromPath = extractTenantFromPath(s.getSectionPath());
        return fromPath != null ? fromPath : "applecom-cms";
    }

    private String extractPageId(ConsolidatedEnrichedSection s) {
        String pid = extractPageIdFromPath(s.getSectionUri());
        if (pid == null) pid = extractPageIdFromPath(s.getSectionPath());
        return pid;
    }

    private String extractLocaleFromPath(String path) {
        if (!org.springframework.util.StringUtils.hasText(path)) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/([a-z]{2}_[A-Z]{2})/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractTenantFromPath(String path) {
        if (!org.springframework.util.StringUtils.hasText(path)) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/content/dam/([^/]+)/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractPageIdFromPath(String path) {
        if (!org.springframework.util.StringUtils.hasText(path)) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/[a-z]{2}_[A-Z]{2}/([^/]+)/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String loadPromptTemplate() {
        try {
            ClassPathResource resource = new ClassPathResource("prompts/interactive_search_prompt.txt");
            byte[] bytes = resource.getInputStream().readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            // Fallback minimal prompt if resource missing
            return "Generate only one JSON object with fields among: query, tags, keywords, original_field_name, context. No text outside JSON.\n\n<conversation_history>\n{conversation_history}\n</conversation_history>\n<user_message>\n{user_message}\n</user_message>";
        }
    }

    private List<String> readStrings(JsonNode node) {
        if (node == null || node.isNull()) return List.of();
        if (node.isTextual()) return List.of(node.asText());
        if (node.isArray()) {
            List<String> out = new ArrayList<>();
            node.forEach(n -> { if (n.isTextual()) out.add(n.asText()); });
            return out;
        }
        return List.of();
    }

    private Map<String, Object> buildContextFromPath(JsonNode pathNode, JsonNode valueNode) {
        Map<String, Object> root = new HashMap<>();
        List<String> path = new ArrayList<>();
        if (pathNode != null && pathNode.isArray()) {
            pathNode.forEach(n -> { if (n.isTextual()) path.add(n.asText()); });
        } else if (pathNode != null && pathNode.isTextual()) {
            path.addAll(Arrays.asList(pathNode.asText().split("\\.")));
        }
        if (path.isEmpty()) return root;

        Map<String, Object> current = root;
        for (int i = 0; i < path.size(); i++) {
            String key = path.get(i);
            if (i == path.size() - 1) {
                current.put(key, valueNode != null && valueNode.isTextual() ? valueNode.asText() : String.valueOf(valueNode));
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> next = (Map<String, Object>) current.get(key);
                if (next == null) {
                    next = new HashMap<>();
                    current.put(key, next);
                }
                current = next;
            }
        }
        return root;
    }

    private String stripJsonFences(String text) {
        if (text == null) return null;
        String trimmed = text.trim();
        if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
            // remove code fences of any type
            trimmed = trimmed.substring(3, trimmed.length() - 3).trim();
            // also strip possible leading language like 'json' on first line
            int nl = trimmed.indexOf('\n');
            if (nl > 0 && trimmed.substring(0, nl).matches("[a-zA-Z0-9_-]+")) {
                trimmed = trimmed.substring(nl + 1).trim();
            }
        }
        return trimmed;
    }
}
