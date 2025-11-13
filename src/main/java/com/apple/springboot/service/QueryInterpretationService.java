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
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class QueryInterpretationService {

    private static final Logger log = LoggerFactory.getLogger(QueryInterpretationService.class);

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final ObjectMapper objectMapper;
    private final String promptTemplate;

    public QueryInterpretationService(BedrockEnrichmentService bedrockEnrichmentService,
                                      ObjectMapper objectMapper) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.objectMapper = objectMapper;
        this.promptTemplate = loadPromptTemplate();
    }

    public Optional<QueryInterpretation> interpret(String userMessage, Map<String, Object> existingContext) {
        if (!StringUtils.hasText(userMessage)) {
            return Optional.empty();
        }

        String prompt = promptTemplate.replace("{user_message}", escapeBraces(userMessage));
        try {
            String rawResponse = bedrockEnrichmentService.invokeChatForText(prompt, 400);
            String jsonPayload = stripJsonFences(rawResponse);
            JsonNode root = objectMapper.readTree(jsonPayload);
            return Optional.of(parseInterpretation(root, userMessage));
        } catch (ThrottledException te) {
            throw te;
        } catch (Exception ex) {
            log.debug("Query interpretation failed, falling back to heuristics: {}", ex.getMessage());
            return Optional.empty();
        }
    }

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
        return List.copyOf(values);
    }

    private Map<String, Object> readObject(JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) {
            return Map.of();
        }
        if (!node.isObject()) {
            return Map.of();
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = objectMapper.convertValue(node, Map.class);
        return map != null ? Map.copyOf(map) : Map.of();
    }

    private String textOrNull(JsonNode node) {
        if (node != null && node.isTextual()) {
            String text = node.asText().trim();
            if (!text.isEmpty()) {
                return text;
            }
        }
        return null;
    }

    private String textOrDefault(JsonNode node, String defaultValue) {
        String value = textOrNull(node);
        return value != null ? value : defaultValue;
    }

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

    private String escapeBraces(String input) {
        return input == null ? "" : input.replace("{", "{{").replace("}", "}}");
    }
}
