package com.apple.springboot.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class AIResponseValidator {

    private static final Logger logger = LoggerFactory.getLogger(AIResponseValidator.class);

    /**
     * Validates the structure of the enriched data map returned by the Bedrock service.
     *
     * @param bedrockResponse The map containing the enrichment results.
     * @return true if the structure is valid, false otherwise.
     */
    public boolean isValid(Map<String, Object> bedrockResponse) {
        if (bedrockResponse == null || bedrockResponse.isEmpty()) {
            logger.warn("Validation failed: Bedrock response map is null or empty.");
            return false;
        }

        if (bedrockResponse.containsKey("error")) {
            logger.warn("Validation failed: Bedrock response contains an explicit error key: {}", bedrockResponse.get("error"));
            return false;
        }

        // Check for top-level keys
        if (!bedrockResponse.containsKey("standardEnrichments") || !(bedrockResponse.get("standardEnrichments") instanceof Map)) {
            logger.warn("Validation failed: Missing or invalid 'standardEnrichments' object.");
            return false;
        }
        if (!bedrockResponse.containsKey("context") || !(bedrockResponse.get("context") instanceof Map)) {
            logger.warn("Validation failed: Missing or invalid 'context' object.");
            return false;
        }

        // Check structure of the 'context' object
        @SuppressWarnings("unchecked")
        Map<String, Object> context = (Map<String, Object>) bedrockResponse.get("context");

        if (!isFieldPresentAndOfType(context, "fullContextId", String.class)) {
            logger.warn("Validation failed: 'fullContextId' is missing or not a String in context object.");
            return false;
        }
        if (!isFieldPresentAndOfType(context, "sourcePath", String.class)) {
            logger.warn("Validation failed: 'sourcePath' is missing or not a String in context object.");
            return false;
        }

        // Check for provenance
        if (!isFieldPresentAndOfType(context, "provenance", Map.class)) {
            logger.warn("Validation failed: 'provenance' is missing or not a Map in context object.");
            return false;
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> provenance = (Map<String, Object>) context.get("provenance");
        if (!isFieldPresentAndOfType(provenance, "modelId", String.class)) {
            logger.warn("Validation failed: 'modelId' is missing or not a String in provenance object.");
            return false;
        }

        logger.debug("AI response validation successful.");
        return true;
    }

    /**
     * Checks whether a map contains a non-null value for a key with the expected type.
     */
    private boolean isFieldPresentAndOfType(Map<String, Object> map, String key, Class<?> type) {
        Object value = map.get(key);
        if (value == null) {
            return false;
        }
        return type.isInstance(value);
    }
}
