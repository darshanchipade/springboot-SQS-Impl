package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.BedrockRuntimeException;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

@Service
public class BedrockEnrichmentService {

    private static final Logger logger = LoggerFactory.getLogger(BedrockEnrichmentService.class);
    private final BedrockRuntimeClient bedrockClient;
    private final ObjectMapper objectMapper;
    private final String bedrockModelId;
    private final String bedrockRegion;
    private final String embeddingModelId;
    private final int bedrockMaxTokens;

    @Value("${app.enrichment.computeItemVector:false}")
    private boolean computeItemVector;
    @Autowired
    public BedrockEnrichmentService(ObjectMapper objectMapper,
                                    @Value("${aws.region}") String region,
                                    @Value("${aws.bedrock.modelId}") String modelId,
                                    @Value("${aws.bedrock.embeddingModelId}") String embeddingModelId,
                                    @Value("${app.bedrock.maxTokens:512}") int bedrockMaxTokens) {
        this.objectMapper = objectMapper;
        this.bedrockRegion = region;
        this.bedrockModelId = modelId;
        this.embeddingModelId = embeddingModelId;
        this.bedrockMaxTokens = Math.max(128, bedrockMaxTokens);

        if (region == null) {
            logger.error("AWS Region for Bedrock is null. Cannot initialize BedrockRuntimeClient.");
            throw new IllegalArgumentException("AWS Region for Bedrock must not be null.");
        }

        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.of(this.bedrockRegion))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        logger.info("BedrockEnrichmentService initialized with region: {} and model ID: {}", this.bedrockRegion, this.bedrockModelId);
    }

    public String getConfiguredModelId() {
        return this.bedrockModelId;
    }

    public float[] generateEmbedding(String text) throws IOException {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("inputText", text);

        String payloadJson = objectMapper.writeValueAsString(payload);
        SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

        InvokeModelRequest request = InvokeModelRequest.builder()
                .modelId(embeddingModelId)
                .contentType("application/json")
                .accept("application/json")
                .body(body)
                .build();

        try {
            InvokeModelResponse response = invokeWithRetry(request, true);
            JsonNode responseJson = objectMapper.readTree(response.body().asUtf8String());
            JsonNode embeddingNode = responseJson.get("embedding");
            float[] embedding = new float[embeddingNode.size()];
            for (int i = 0; i < embeddingNode.size(); i++) {
                embedding[i] = embeddingNode.get(i).floatValue();
            }
            return embedding;
        } catch (ThrottledException te) {
            // IMPORTANT: bubble up so SQS listener does NOT delete the message
            throw te;
        } catch (BedrockRuntimeException e) {
            logger.error("Bedrock API error during embedding generation: {}", e.awsErrorDetails().errorMessage(), e);
            throw new IOException("Bedrock API error during embedding generation.", e);
        }
    }

    private String createEnrichmentPrompt(JsonNode itemContent, EnrichmentContext context) throws JsonProcessingException {
        String cleansedContent = itemContent.path("cleansedContent").asText("");
        String contextJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(context);

        String promptTemplate =
                "You are an expert content analyst. Analyze the text and return enrichments as a single JSON object ONLY (no markdown, no code fences, no commentary).\n" +
                        "\n" +
                        "Input content:\n<content>\n%s\n</content>\n" +
                        "\n" +
                        "Context (JSON):\n<context>\n%s\n</context>\n" +
                        "\n" +
                        "Rules:\n" +
                        "- Output MUST be exactly one JSON object with one top-level key: \"standardEnrichments\".\n" +
                        "- Do not include any text before/after the JSON. No backticks. No explanations.\n" +
                        "- Use context (e.g., pathHierarchy, locale, facets, model) to tailor results.\n" +
                        "- Keep summary ≤ 2 sentences; do not copy the content verbatim.\n" +
                        "- keywords/tags: lowercase, unique, ≤ 10 keywords, ≤ 5 tags; no stopwords.\n" +
                        "- sentiment ∈ {\"positive\",\"neutral\",\"negative\"}.\n" +
                        "- classification: short category like \"product description\", \"legal disclaimer\", \"promotional heading\", etc.\n" +
                        "- If uncertain, return best-effort values; never null; use [] for empty arrays and \"unknown\" when needed.\n" +
                        "\n" +
                        "Output JSON schema (example shape; values must reflect the input):\n" +
                        "{\n" +
                        "  \"standardEnrichments\": {\n" +
                        "    \"summary\": \"\",\n" +
                        "    \"keywords\": [\"\"],\n" +
                        "    \"sentiment\": \"\",\n" +
                        "    \"classification\": \"\",\n" +
                        "    \"tags\": [\"\"]\n" +
                        "  }\n" +
                        "}";
        return String.format(promptTemplate, cleansedContent, contextJson);
    }
    public Map<String, Object> enrichItem(JsonNode itemContent, EnrichmentContext context) {
        String effectiveModelId = this.bedrockModelId;
        String sourcePath = (context != null && context.getEnvelope() != null) ? context.getEnvelope().getSourcePath() : "Unknown";
        logger.info("Starting enrichment for item using model: {}. Item path: {}", effectiveModelId, sourcePath);

        Map<String, Object> results = new HashMap<>();
        results.put("enrichedWithModel", effectiveModelId);

        try {
            String prompt = createEnrichmentPrompt(itemContent, context);

            ObjectNode payload = objectMapper.createObjectNode();
            payload.put("anthropic_version", "bedrock-2023-05-31");
            payload.put("max_tokens", bedrockMaxTokens);
            List<ObjectNode> messages = new ArrayList<>();
            ObjectNode userMessage = objectMapper.createObjectNode();
            userMessage.put("role", "user");
            userMessage.put("content", prompt);
            messages.add(userMessage);
            payload.set("messages", objectMapper.valueToTree(messages));

            String payloadJson = objectMapper.writeValueAsString(payload);
            SdkBytes body = SdkBytes.fromUtf8String(payloadJson);

            InvokeModelRequest request = InvokeModelRequest.builder()
                    .modelId(bedrockModelId)
                    .contentType("application/json")
                    .accept("application/json")
                    .body(body)
                    .build();

            InvokeModelResponse response = invokeWithRetry(request, false);
            String responseBodyString = response.body().asUtf8String();
            JsonNode responseJson = objectMapper.readTree(responseBodyString);
            JsonNode contentBlock = responseJson.path("content");

            if (contentBlock.isArray() && contentBlock.size() > 0) {
                String textContent = contentBlock.get(0).path("text").asText("").trim();

                if (textContent.startsWith("```json")) {
                    textContent = textContent.substring(7).trim();
                    if (textContent.endsWith("```")) {
                        textContent = textContent.substring(0, textContent.length() - 3).trim();
                    }
                }

                if (textContent.startsWith("{") && textContent.endsWith("}")) {
                    try {
                        Map<String, Object> aiResults = objectMapper.readValue(textContent, new TypeReference<>() {});
                        aiResults.put("enrichedWithModel", effectiveModelId);
                        return aiResults;
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to parse JSON content from Bedrock response: {}. Error: {}", textContent, e.getMessage());
                        results.put("error", "Failed to parse JSON from Bedrock response");
                        results.put("raw_bedrock_response", textContent);
                        return results;
                    }
                } else {
                    logger.error("Bedrock response content is not a JSON object after stripping fences: {}", textContent);
                    results.put("error", "Bedrock response content is not a JSON object");
                    results.put("raw_bedrock_response", textContent);
                }
            } else {
                logger.error("Bedrock response does not contain expected content block or content is not an array.");
                results.put("error", "Bedrock response structure unexpected");
                results.put("raw_bedrock_response", responseBodyString);
            }
        } catch (ThrottledException te) {
            // Crucial: let the caller (SQS listener) decide retry/delete; do not swallow
            throw te;
        } catch (BedrockRuntimeException e) {
            logger.error("Bedrock API error during enrichment for model {}: {}", effectiveModelId, e.awsErrorDetails().errorMessage(), e);
            results.put("error", "Bedrock API error: " + e.awsErrorDetails().errorMessage());
            results.put("aws_error_code", e.awsErrorDetails().errorCode());
            return results;
        } catch (Exception e) {
            logger.error("Unexpected error during Bedrock enrichment for model {}: {}", effectiveModelId, e.getMessage(), e);
            results.put("error", "Unexpected error during enrichment: " + e.getMessage());
            return results;
        }
        return results;
    }

    private InvokeModelResponse invokeWithRetry(InvokeModelRequest request, boolean isEmbedding) {
        final int maxAttempts = 6;
        final long baseBackoffMs = isEmbedding ? 400L : 800L;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return bedrockClient.invokeModel(request);
            } catch (BedrockRuntimeException e) {
                int statusCode = e.statusCode();
                String code = e.awsErrorDetails() != null ? e.awsErrorDetails().errorCode() : null;
                boolean throttled = statusCode == 429
                        || "ThrottlingException".equalsIgnoreCase(code)
                        || "TooManyRequestsException".equalsIgnoreCase(code)
                        || "ProvisionedThroughputExceededException".equalsIgnoreCase(code);

                if (!throttled) {
                    throw e; // non-throttling error -> bubble up
                }

                if (attempt == maxAttempts) {
                    logger.warn("Bedrock throttled after {} attempts; surfacing throttling.", maxAttempts);
                    throw new ThrottledException("Bedrock throttling after retries", e);
                }

                long jitter = ThreadLocalRandom.current().nextLong(50, 200);
                long sleepMs = (long) Math.min(10_000, baseBackoffMs * Math.pow(2, attempt - 1) + jitter);
                logger.warn("Bedrock throttled (attempt {}/{}). Backing off for {} ms. Error: {}",
                        attempt, maxAttempts, sleepMs, e.awsErrorDetails() != null ? e.awsErrorDetails().errorMessage() : e.getMessage());
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted during backoff", ie);
                }
            }
        }
        throw new RuntimeException("Unreachable");
    }
}