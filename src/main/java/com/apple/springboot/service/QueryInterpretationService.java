package com.apple.springboot.service;

import com.apple.springboot.model.QueryInterpretation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class QueryInterpretationService {

    private static final Logger log = LoggerFactory.getLogger(QueryInterpretationService.class);
    private static final int LOG_VALUE_LIMIT = 500;

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final ObjectMapper objectMapper;
    private final String promptTemplate;

    public QueryInterpretationService(BedrockEnrichmentService bedrockEnrichmentService,
                                      ObjectMapper objectMapper) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.objectMapper = objectMapper;
        this.promptTemplate = loadPromptTemplate();
    }

    /**
     * Calls Bedrock to interpret the user message into structured filters.
     */
    public Optional<QueryInterpretation> interpret(String userMessage, Map<String, Object> existingContext) {
        if (!StringUtils.hasText(userMessage)) {
            return Optional.empty();
        }

        String prompt = promptTemplate.replace("{user_message}", escapeBraces(userMessage));
        try {
            String rawResponse = bedrockEnrichmentService.invokeChatForText(prompt, 400);
            String jsonPayload = stripJsonFences(rawResponse);
            JsonNode root = objectMapper.readTree(jsonPayload);
            QueryInterpretation interpretation = parseInterpretation(root, userMessage);
            log.info("Query interpretation input message='{}'", clip(userMessage));
            log.info("Query interpretation raw response='{}'", clip(rawResponse));
            log.info("Query interpretation parsed payload='{}'", clip(jsonPayload));
            log.info(
                    "Query interpretation fields rawQuery='{}', sectionKey='{}', role='{}', pageId='{}', locale='{}', language='{}', country='{}', tags={}, keywords={}",
                    clip(interpretation.rawQuery()),
                    interpretation.sectionKey(),
                    interpretation.role(),
                    interpretation.pageId(),
                    interpretation.locale(),
                    interpretation.language(),
                    interpretation.country(),
                    interpretation.tags(),
                    interpretation.keywords()
            );
            return Optional.of(interpretation);
        } catch (ThrottledException te) {
            throw te;
        } catch (Exception ex) {
            log.debug("Query interpretation failed, falling back to heuristics: {}", ex.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Parses a JSON response into a QueryInterpretation instance.
     */
    private QueryInterpretation parseInterpretation(JsonNode root, String userMessage) {
        if (root == null || !root.isObject()) {
            return new QueryInterpretation(userMessage, null, null, null, null, null, null, null, List.of(), List.of(), Map.of());
        }
        return new QueryInterpretation(
                textOrDefault(root.path("rawQuery"), userMessage),
                textOrNull(root.path("sectionKey")),
                textOrNull(root.path("sectionName")),
                textOrNull(root.path("pageId")),
                textOrNull(root.path("role")),
                textOrNull(root.path("locale")),
                textOrNull(root.path("language")),
                textOrNull(root.path("country")),
                readArray(root.path("tags")),
                readArray(root.path("keywords")),
                readObject(root.path("context"))
        );
    }

    /**
     * Reads a JSON array or scalar node into a list of strings.
     */
    private List<String> readArray(JsonNode node) {
        if (node == null || node.isNull()) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        if (node.isArray()) {
            node.forEach(item -> {
                if (item != null && item.isTextual()) {
                    String text = item.asText().trim();
                    if (!text.isEmpty()) {
                        values.add(text);
                    }
                }
            });
        } else if (node.isTextual()) {
            String text = node.asText().trim();
            if (!text.isEmpty()) {
                values.add(text);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(values));
    }

    /**
     * Converts a JSON object node into an immutable map.
     */
    private Map<String, Object> readObject(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return Map.of();
        }
        if (!node.isObject()) {
            return Map.of();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = objectMapper.convertValue(node, Map.class);
        if (map == null) {
            return Map.of();
        }
        return Collections.unmodifiableMap(new LinkedHashMap<>(map));
    }

    /**
     * Returns trimmed text when the node is textual, otherwise null.
     */
    private String textOrNull(JsonNode node) {
        if (node != null && node.isTextual()) {
            String text = node.asText().trim();
            if (!text.isEmpty()) {
                return text;
            }
        }
        return null;
    }

    /**
     * Returns a textual node value or a default when missing.
     */
    private String textOrDefault(JsonNode node, String defaultValue) {
        String value = textOrNull(node);
        return value != null ? value : defaultValue;
    }

    /**
     * Removes Markdown-style code fences around JSON output.
     */
    private String stripJsonFences(String text) {
        if (text == null) {
            return null;
        }
        String trimmed = text.trim();
        if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
            trimmed = trimmed.substring(3, trimmed.length() - 3).trim();
            int newline = trimmed.indexOf('\n');
            if (newline > 0 && trimmed.substring(0, newline).matches("[a-zA-Z0-9_-]+")) {
                trimmed = trimmed.substring(newline + 1).trim();
            }
        }
        return trimmed;
    }

    /**
     * Loads the query interpretation prompt from the classpath.
     */
    private String loadPromptTemplate() {
        try {
            ClassPathResource resource = new ClassPathResource("prompts/query_interpretation_prompt.txt");
            byte[] bytes = resource.getInputStream().readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("Failed to load query interpretation prompt template: {}", e.getMessage());
            return "Extract metadata from the user message and return a JSON object. "
                    + "Keys: sectionKey, sectionName, pageId, role, locale, language, country, tags, keywords, context, rawQuery. "
                    + "Use empty string or empty array/object when unknown.\n\n"
                    + "User message:\n{user_message}";
        }
    }

    /**
     * Escapes braces so the prompt template replacement is safe.
     */
    private String escapeBraces(String input) {
        return input == null ? "" : input.replace("{", "{{").replace("}", "}}");
    }

    /**
     * Truncates long values to keep log output readable.
     */
    private String clip(String value) {
        if (!StringUtils.hasText(value)) {
            return value;
        }
        String normalized = value.replaceAll("\\s+", " ").trim();
        if (normalized.length() <= LOG_VALUE_LIMIT) {
            return normalized;
        }
        return normalized.substring(0, LOG_VALUE_LIMIT) + "...";
    }
}
