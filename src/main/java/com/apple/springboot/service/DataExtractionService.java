package com.apple.springboot.service;

import com.apple.springboot.model.ExtractedContentOutput;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;


import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections; // Added for empty list
import java.util.List;
import java.util.regex.Pattern;

/**
 * Service layer for extracting and cleansing 'copy', '_path', and 'value' content from a local JSON file.
 * Uses JsonNode for dynamic traversal to find relevant text fields regardless of section type.
 * Includes retry mechanisms for transient errors during file reading.
 * Now extracts and includes the '_model' associated with each extracted text field.
 * **No longer persists data to a database.**
 * **Now explicitly processes the 'content.disclaimers' path, correctly handling its object-with-items structure.**
 */
@Service
public class DataExtractionService {

    private static final Logger logger = LoggerFactory.getLogger(DataExtractionService.class);

    @Value("${app.json.file.path}")
    private String jsonFilePath;

    private final ResourceLoader resourceLoader;
    private final ObjectMapper objectMapper;

    // Regex patterns for AEM-specific placeholders
    private static final Pattern NBSP_PATTERN = Pattern.compile("\\{%nbsp%\\}");
    //private static final Pattern SOSUMI_PATTERN = Pattern.compile("\\{%sosumi type=\"[^\"]+\" metadata=\"\\d+\"%\\}");
    private static final Pattern BR_PATTERN = Pattern.compile("\\{%br%\\}");
    private static final Pattern URL_PATTERN = Pattern.compile(":\\s*\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)");
    //private static final Pattern WJ_PATTERN = Pattern.compile("\\(\\{%wj%\\}\\)");
    private static final Pattern NESTED_URL_PATTERN = Pattern.compile(":\\[\\s*:\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)\\]\\(\\{%wj%\\}\\)");
    private static final Pattern METADATA_PATTERN = Pattern.compile("\\{% metadata=\"\\d+\" %\\}");
   // private static final Pattern APR_PATTERN = Pattern.compile("\\{%apr value=\"[^\"]+\"%\\}");

    @Autowired
    public DataExtractionService(ResourceLoader resourceLoader, ObjectMapper objectMapper) {
        this.resourceLoader = resourceLoader;
        this.objectMapper = objectMapper;
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE, false);
    }

    /**
     * Reads the local JSON file, processes its content, dynamically extracts
     * and cleanses all 'copy', '_path', and 'value' fields found within the 'sections' and 'disclaimers'
     * parts of the JSON it will basically lok for the copy content element.
     *
     * @return A list of ExtractedContentOutput entities.
     * @throws IOException if file reading or processing fails.
     */
    public List<ExtractedContentOutput> extractAndCleanseContentFromFile() throws IOException {
        logger.info("Attempting to read and process JSON data from file: {}",  jsonFilePath);

        List<ExtractedContentOutput> extractedContents = new ArrayList<>();
        String jsonContent;
        //Get the JSON file from application properties and then read the file
        Resource resource = resourceLoader.getResource("classpath:" + jsonFilePath);
        try (Reader reader = new InputStreamReader(resource.getInputStream(), StandardCharsets.UTF_8)) {
            jsonContent = FileCopyUtils.copyToString(reader);
        } catch (IOException e) {
            logger.error("Failed to read JSON file from classpath: {}", jsonFilePath, e);
            throw e; // Re-throw the IOException to trigger retry/recovery
        }

        JsonNode rootNode = objectMapper.readTree(jsonContent);

        if (rootNode == null || !rootNode.has("content")) {
            logger.warn("No 'content' node found in the provided JSON payload.");
            return extractedContents;
        }

        JsonNode contentNode = rootNode.get("content");

        // Process 'sections' if present content>sections
        if (contentNode.has("sections") && contentNode.get("sections").isArray()) {
            JsonNode sectionsNode = contentNode.get("sections");
            for (int i = 0; i < sectionsNode.size(); i++) {
                JsonNode sectionNode = sectionsNode.get(i);
                String sectionPath = sectionNode.has("_path") && sectionNode.get("_path").isTextual() ?
                        sectionNode.get("_path").asText() : "content/sections[" + i + "]";

                String sectionModel = sectionNode.has("_model") && sectionNode.get("_model").isTextual() ?
                        sectionNode.get("_model").asText() : null;

                findAndCleanseTextContent(sectionNode, sectionPath, sectionModel, extractedContents);
            }
            logger.info("Successfully processed 'sections' array.");
        } else {
            logger.warn("No 'sections' array found under 'content' node.");
        }

        // Process 'disclaimers' if present
        // Changed to check if 'disclaimers' is an object and then access its 'items' array
        if (contentNode.has("disclaimers") && contentNode.get("disclaimers").isObject()) {
            JsonNode disclaimersObjectNode = contentNode.get("disclaimers");
            logger.info("Found 'disclaimers' object under 'content' node. Checking for 'items' array...");
            if (disclaimersObjectNode.has("items") && disclaimersObjectNode.get("items").isArray()) {
                JsonNode disclaimersArrayNode = disclaimersObjectNode.get("items");
                for (int i = 0; i < disclaimersArrayNode.size(); i++) {
                    JsonNode disclaimerNode = disclaimersArrayNode.get(i);
                    String disclaimerPath = "content/disclaimers/items[" + i + "]"; // Updated path to reflect 'items' array

                    String disclaimerModel = disclaimerNode.has("_model") && disclaimerNode.get("_model").isTextual() ?
                            disclaimerNode.get("_model").asText() : null;

                    logger.debug("Processing disclaimer item at path: {} with model: {}", disclaimerPath, disclaimerModel);
                    findAndCleanseTextContent(disclaimerNode, disclaimerPath, disclaimerModel, extractedContents);
                }
                logger.info("Successfully processed 'content.disclaimers.items' array.");
            } else {
                logger.warn("No 'items' array found under 'content.disclaimers' object, or it's not an array.");
            }
        } else {
            logger.warn("No 'disclaimers' object found under 'content' node, or it's not an object.");
        }

        logger.info("Successfully extracted {} total content entries from file: {}", extractedContents.size(), jsonFilePath);
        return extractedContents;
    }

    /**
     * Recovery method for @Retryable. This method is called if all retry attempts fail for IOException.
     *
     * @param e The IOException that caused the retry to fail.
     * @return A list containing a single ExtractedContentOutput indicating the failure.
     */

    public List<ExtractedContentOutput> recover(IOException e) {
        logger.error("All retry attempts failed to process JSON file: {}. Recovering...", jsonFilePath, e);
        // Return an empty list with an error message to indicate failure
        return Collections.singletonList(new ExtractedContentOutput(
                "Error: " + jsonFilePath,
                "file_read_error",
                "Failed to read file after multiple attempts: " + e.getMessage(),
                "error"
        ));
    }

    /**
     * finds and cleanses 'copy', '_path', and 'value' fields within a JsonNode.
     * Passes down the most relevant '_model' from the current object or its ancestors.
     *
     * @param node The current JsonNode to traverse.
     * @param currentPath The current path in the JSON structure.
     * @param currentModel The '_model' value from the closest ancestor object, or null if not found.
     * @param extractedContents The list to add extracted and cleansed contents to.
     */
    private void findAndCleanseTextContent(JsonNode node, String currentPath, String currentModel, List<ExtractedContentOutput> extractedContents) {
        if (node == null) {
            return;
        }

        if (node.isObject()) {
            String newModel;
            if (node.has("_model") && node.get("_model").isTextual()) {
                newModel = node.get("_model").asText();
            } else {
                newModel = currentModel;
            }

            node.fields().forEachRemaining(entry -> {
                String fieldName = entry.getKey();
                JsonNode fieldValue = entry.getValue();
                String newPath = currentPath.isEmpty() ? fieldName : currentPath + "/" + fieldName;

                // Check for 'copy' or '_path' fields that are textual
                if (("copy".equals(fieldName) || "_path".equals(fieldName)) && fieldValue.isTextual()) {
                    String cleansedContent = cleanseCopyText(fieldValue.asText());
                    if (cleansedContent != null && !cleansedContent.isEmpty()) {
                        extractedContents.add(new ExtractedContentOutput(newPath, fieldName, cleansedContent, newModel));
                        logger.debug("Extracted content from path: {} (field: {}) with model: {}", newPath, fieldName, newModel);

                        // Specific logging for content potentially related to disclaimers/legal
                        if (newPath.contains("disclaimers")) { // Check if path contains "disclaimers"
                            logger.info("Extracted DISCLAIMER content: Path={}, Field={}, Content='{}'", newPath, fieldName, cleansedContent);
                        } else if (node.has("categories") && node.get("categories").isArray()) {
                            for (JsonNode category : node.get("categories")) {
                                if (category.isTextual() && "legal".equals(category.asText())) {
                                    logger.info("Found LEGAL content: Path={}, Field={}, Content='{}'", newPath, fieldName, cleansedContent);
                                    break;
                                }
                            }
                        }
                    } else {
                        logger.warn("Cleansed content is null or empty for path: {} (field: {}). Original: '{}'", newPath, fieldName, fieldValue.asText());
                    }
                } else if ("analyticsAttributes".equals(fieldName) && fieldValue.isArray()) {
                    for (int i = 0; i < fieldValue.size(); i++) {
                        JsonNode analyticsAttributeNode = fieldValue.get(i);
                        String analyticsPath = newPath + "[" + i + "]";
                        String analyticsModel = newModel;

                        if (analyticsAttributeNode.has("_model") && analyticsAttributeNode.get("_model").isTextual()) {
                            analyticsModel = analyticsAttributeNode.get("_model").asText();
                        }

                        if (analyticsAttributeNode.isObject() && analyticsAttributeNode.has("value") && analyticsAttributeNode.get("value").isTextual()) {
                            String rawValue = analyticsAttributeNode.get("value").asText();
                            String cleansedValue = cleanseCopyText(rawValue);
                            if (cleansedValue != null && !cleansedValue.isEmpty()) {
                                extractedContents.add(new ExtractedContentOutput(analyticsPath + "/value", "value", cleansedValue, analyticsModel));
                                logger.debug("Extracted analytics value from path: {}/value with model: {}", analyticsPath, analyticsModel);
                            } else {
                                logger.warn("Cleansed analytics value is null or empty for path: {}/value. Original: '{}'", analyticsPath, rawValue);
                            }
                        }
                        findAndCleanseTextContent(analyticsAttributeNode, analyticsPath, analyticsModel, extractedContents);
                    }
                } else if (fieldValue.isObject() || fieldValue.isArray()) {
                    findAndCleanseTextContent(fieldValue, newPath, newModel, extractedContents);
                }
            });
        } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode element = node.get(i);
                String newPath = currentPath + "[" + i + "]";
                findAndCleanseTextContent(element, newPath, currentModel, extractedContents);
            }
        }
    }

    /**
     * Cleanses the given text by removing AEM-specific placeholders.
     *
     * @param text The input text to cleanse.
     * @return The cleansed text, or null if the input is null or results in an empty string after cleansing.
     */
    private String cleanseCopyText(String text) {
        if (text == null) {
            return null;
        }

        String cleansed = text;

        // Replace AEM-specific placeholders with appropriate characters or remove them
        cleansed = NBSP_PATTERN.matcher(cleansed).replaceAll(" "); // Replace non-breaking space with regular space
        //cleansed = SOSUMI_PATTERN.matcher(cleansed).replaceAll(""); // Remove sosumi placeholders
        cleansed = BR_PATTERN.matcher(cleansed).replaceAll("\n"); // Replace break tag with newline
        cleansed = URL_PATTERN.matcher(cleansed).replaceAll(""); // Remove URL placeholders
        //cleansed = WJ_PATTERN.matcher(cleansed).replaceAll(""); // Remove wj placeholders
        cleansed = NESTED_URL_PATTERN.matcher(cleansed).replaceAll(""); // Remove nested URL placeholders
        cleansed = METADATA_PATTERN.matcher(cleansed).replaceAll(""); // Remove metadata placeholders
        //cleansed = APR_PATTERN.matcher(cleansed).replaceAll(""); // Remove apr placeholders

        cleansed = cleansed.trim(); // Trim leading/trailing whitespace

        // Return null if the cleansed string is empty, otherwise return the cleansed string
        return cleansed.isEmpty() ? null : cleansed;
    }
}