package com.apple.springboot.service;

import com.apple.springboot.dto.EnrichedContentGenerateRequest;
import com.apple.springboot.dto.EnrichedContentUpdateRequest;
import com.apple.springboot.dto.EnrichedContentUpdateResponse;
import com.apple.springboot.model.ContentHash;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.model.EnrichedContentRevision;
import com.apple.springboot.model.EnrichmentContext;
import com.apple.springboot.repository.ContentHashRepository;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.apple.springboot.repository.EnrichedContentRevisionRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class EnrichedContentUpdateService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichedContentUpdateService.class);
    private static final String USAGE_REF_DELIM = " ::ref:: ";
    private static final Set<String> SUPPORTED_FIELDS = Set.of("summary", "classification", "keywords", "tags");
    private static final String SOURCE_AI = "AI";
    private static final String SOURCE_USER = "USER";
    private static final String SOURCE_REGENERATE = "REGENERATE";

    private final EnrichedContentElementRepository elementRepository;
    private final EnrichedContentRevisionRepository revisionRepository;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepository;
    private final ContentHashRepository contentHashRepository;
    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final AIResponseValidator aiResponseValidator;
    private final ObjectMapper objectMapper;

    /**
     * Creates the service that handles manual edits and Bedrock regeneration.
     */
    public EnrichedContentUpdateService(EnrichedContentElementRepository elementRepository,
                                        EnrichedContentRevisionRepository revisionRepository,
                                        ConsolidatedEnrichedSectionRepository consolidatedRepository,
                                        ContentHashRepository contentHashRepository,
                                        BedrockEnrichmentService bedrockEnrichmentService,
                                        AIResponseValidator aiResponseValidator,
                                        ObjectMapper objectMapper) {
        this.elementRepository = elementRepository;
        this.revisionRepository = revisionRepository;
        this.consolidatedRepository = consolidatedRepository;
        this.contentHashRepository = contentHashRepository;
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.aiResponseValidator = aiResponseValidator;
        this.objectMapper = objectMapper;
    }

    /**
     * Applies manual edits, persists the element, and records a USER revision.
     */
    @Transactional
    public EnrichedContentUpdateResponse applyManualUpdate(UUID elementId, EnrichedContentUpdateRequest request) {
        EnrichedContentElement element = loadElement(elementId);
        UpdatedFields updatedFields = resolveManualUpdate(element, request);
        if (updatedFields.updatedFields == null || updatedFields.updatedFields.isEmpty()) {
            return buildResponse(element, findLatestRevision(element.getId()));
        }
        applyUpdates(element, updatedFields);
        element.setUserOverrideActive(true);
        element.setNewAiAvailable(false);
        elementRepository.save(element);

        EnrichedContentRevision revision = recordRevision(element, updatedFields, SOURCE_USER, buildMetadata(request, updatedFields));
        syncConsolidatedSections(element, updatedFields);

        return buildResponse(element, revision);
    }

    /**
     * Regenerates enrichment fields using Bedrock, honoring preview-only mode.
     */
    @Transactional
    public EnrichedContentUpdateResponse regenerateFromBedrock(UUID elementId, EnrichedContentGenerateRequest request) {
        EnrichedContentElement element = loadElement(elementId);
        Set<String> fieldsToUpdate = normalizeFieldList(request != null ? request.getFields() : null);
        if (fieldsToUpdate.isEmpty()) {
            fieldsToUpdate = SUPPORTED_FIELDS;
        }
        boolean previewOnly = request != null && Boolean.TRUE.equals(request.getPreview());

        Map<String, Object> bedrockResponse = callBedrock(element);
        if (bedrockResponse.containsKey("error")) {
            throw new IllegalStateException("Bedrock enrichment failed: " + bedrockResponse.get("error"));
        }

        @SuppressWarnings("unchecked")
        Map<String, Object> standardEnrichments = (Map<String, Object>) bedrockResponse.getOrDefault("standardEnrichments", bedrockResponse);
        UpdatedFields updatedFields = resolveGeneratedUpdate(element, standardEnrichments, fieldsToUpdate);
        updatedFields.modelUsed = bedrockEnrichmentService.getConfiguredModelId();
        boolean hasUpdates = updatedFields.updatedFields != null && !updatedFields.updatedFields.isEmpty();

        if (previewOnly) {
            Map<String, Object> previewPayload = new HashMap<>();
            previewPayload.put("summary", updatedFields.summary);
            previewPayload.put("classification", updatedFields.classification);
            previewPayload.put("keywords", updatedFields.keywords);
            previewPayload.put("tags", updatedFields.tags);
            previewPayload.put("modelUsed", updatedFields.modelUsed);
            previewPayload.put("updatedFields", updatedFields.updatedFields);

            EnrichedContentUpdateResponse response = new EnrichedContentUpdateResponse(element, null);
            response.setPreview(previewPayload);
            return response;
        }

        if (!hasUpdates) {
            element.setBedrockModelUsed(updatedFields.modelUsed);
            element.setUserOverrideActive(false);
            element.setNewAiAvailable(false);
            elementRepository.save(element);
            return buildResponse(element, findLatestRevision(element.getId()));
        }

        applyUpdates(element, updatedFields);
        element.setBedrockModelUsed(updatedFields.modelUsed);
        element.setUserOverrideActive(false);
        element.setNewAiAvailable(false);
        elementRepository.save(element);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("requestedFields", new ArrayList<>(fieldsToUpdate));
        metadata.put("updatedFields", updatedFields.updatedFields);
        metadata.put("modelUsed", updatedFields.modelUsed);
        EnrichedContentRevision revision = recordRevision(element, updatedFields, SOURCE_REGENERATE, metadata);
        syncConsolidatedSections(element, updatedFields);

        return buildResponse(element, revision);
    }

    /**
     * Returns the revision history for an enriched content element.
     */
    @Transactional(readOnly = true)
    public List<EnrichedContentRevision> listRevisions(UUID elementId) {
        EnrichedContentElement element = loadElement(elementId);
        return revisionRepository.findAllByEnrichedContentElementIdOrderByRevisionDesc(element.getId());
    }

    /**
     * Restores a previous revision and records the restoration as USER.
     */
    @Transactional
    public EnrichedContentUpdateResponse restoreFromRevision(UUID elementId, UUID revisionId) {
        EnrichedContentElement element = loadElement(elementId);
        EnrichedContentRevision revision = revisionRepository.findById(revisionId)
                .orElseThrow(() -> new IllegalArgumentException("Revision not found for id " + revisionId));
        if (!element.getId().equals(revision.getEnrichedContentElementId())) {
            throw new IllegalArgumentException("Revision does not belong to enriched content element " + elementId);
        }

        UpdatedFields updatedFields = new UpdatedFields();
        updatedFields.summary = normalizeText(revision.getSummary());
        updatedFields.classification = normalizeText(revision.getClassification());
        updatedFields.keywords = sanitizeList(revision.getKeywords());
        updatedFields.tags = sanitizeList(revision.getTags());
        updatedFields.modelUsed = revision.getModelUsed();
        updatedFields.updatedFields = determineUpdatedFields(element, updatedFields);

        applyUpdates(element, updatedFields);
        if (revision.getModelUsed() != null) {
            element.setBedrockModelUsed(revision.getModelUsed());
        }
        element.setUserOverrideActive(true);
        element.setNewAiAvailable(false);
        elementRepository.save(element);

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("action", "RESTORE");
        metadata.put("restoredRevisionId", revision.getId());
        metadata.put("restoredRevision", revision.getRevision());
        metadata.put("restoredSource", revision.getSource());
        metadata.put("updatedFields", updatedFields.updatedFields);
        EnrichedContentRevision newRevision = recordRevision(element, updatedFields, SOURCE_USER, metadata);
        syncConsolidatedSections(element, updatedFields);

        return buildResponse(element, newRevision);
    }

    /**
     * Loads an enriched content element or throws if missing.
     */
    private EnrichedContentElement loadElement(UUID elementId) {
        Optional<EnrichedContentElement> elementOpt = elementRepository.findById(elementId);
        if (elementOpt.isEmpty()) {
            throw new IllegalArgumentException("Enriched content element not found for id " + elementId);
        }
        return elementOpt.get();
    }

    /**
     * Resolves field updates from a manual edit request.
     */
    private UpdatedFields resolveManualUpdate(EnrichedContentElement element, EnrichedContentUpdateRequest request) {
        UpdatedFields fields = new UpdatedFields();
        fields.summary = resolveText(request != null ? request.getSummary() : null, element.getSummary());
        fields.classification = resolveText(request != null ? request.getClassification() : null, element.getClassification());
        fields.keywords = resolveList(request != null ? request.getKeywords() : null, element.getKeywords());
        fields.tags = resolveList(request != null ? request.getTags() : null, element.getTags());
        fields.updatedFields = determineUpdatedFields(element, fields);
        return fields;
    }

    /**
     * Resolves field updates from a generated Bedrock response.
     */
    private UpdatedFields resolveGeneratedUpdate(EnrichedContentElement element,
                                                Map<String, Object> standardEnrichments,
                                                Set<String> fieldsToUpdate) {
        UpdatedFields fields = new UpdatedFields();
        fields.summary = resolveGeneratedText(
                element.getSummary(),
                fieldsToUpdate.contains("summary") ? standardEnrichments.get("summary") : null
        );
        fields.classification = resolveGeneratedText(
                element.getClassification(),
                fieldsToUpdate.contains("classification") ? standardEnrichments.get("classification") : null
        );
        fields.keywords = resolveGeneratedList(
                element.getKeywords(),
                fieldsToUpdate.contains("keywords") ? standardEnrichments.get("keywords") : null
        );
        fields.tags = resolveGeneratedList(
                element.getTags(),
                fieldsToUpdate.contains("tags") ? standardEnrichments.get("tags") : null
        );
        fields.updatedFields = determineUpdatedFields(element, fields);
        return fields;
    }

    /**
     * Applies resolved field values onto the enriched content element.
     */
    private void applyUpdates(EnrichedContentElement element, UpdatedFields fields) {
        element.setSummary(fields.summary);
        element.setClassification(fields.classification);
        element.setKeywords(fields.keywords);
        element.setTags(fields.tags);
    }

    /**
     * Persists a revision for the supplied updated fields.
     */
    private EnrichedContentRevision recordRevision(EnrichedContentElement element,
                                                   UpdatedFields updatedFields,
                                                   String source,
                                                   Map<String, Object> metadata) {
        Integer maxRevision = revisionRepository.findMaxRevisionForElement(element.getId());
        int nextRevision = Optional.ofNullable(maxRevision).orElse(0) + 1;

        String normalizedSource = normalizeSource(source);
        EnrichedContentRevision revision = EnrichedContentRevision.builder()
                .enrichedContentElementId(element.getId())
                .cleansedDataId(element.getCleansedDataId())
                .revision(nextRevision)
                .summary(updatedFields.summary)
                .classification(updatedFields.classification)
                .keywords(updatedFields.keywords)
                .tags(updatedFields.tags)
                .source(normalizedSource)
                .modelUsed(updatedFields.modelUsed)
                .metadata(metadata)
                .createdAt(OffsetDateTime.now())
                .build();
        revisionRepository.save(revision);
        return revision;
    }

    /**
     * Normalizes a revision source value to AI, USER, or REGENERATE.
     */
    private String normalizeSource(String source) {
        if (source == null || source.isBlank()) {
            return SOURCE_USER;
        }
        String normalized = source.trim().toUpperCase();
        if (normalized.contains("USER") || normalized.contains("RESTORE")) {
            return SOURCE_USER;
        }
        if (normalized.contains("REGENERATE")) {
            return SOURCE_REGENERATE;
        }
        if (normalized.equals(SOURCE_AI) || normalized.contains("AI")) {
            return SOURCE_AI;
        }
        return SOURCE_USER;
    }

    /**
     * Builds the response payload for update or regeneration requests.
     */
    private EnrichedContentUpdateResponse buildResponse(EnrichedContentElement element, EnrichedContentRevision revision) {
        if (revision == null) {
            return new EnrichedContentUpdateResponse(element, null);
        }
        EnrichedContentUpdateResponse.RevisionSnapshot snapshot = new EnrichedContentUpdateResponse.RevisionSnapshot(
                revision.getId(),
                revision.getRevision(),
                revision.getSource(),
                revision.getModelUsed(),
                revision.getCreatedAt()
        );
        return new EnrichedContentUpdateResponse(element, snapshot);
    }

    /**
     * Finds the latest revision for an element if available.
     */
    private EnrichedContentRevision findLatestRevision(UUID elementId) {
        if (elementId == null) {
            return null;
        }
        return revisionRepository.findFirstByEnrichedContentElementIdOrderByRevisionDesc(elementId)
                .orElse(null);
    }

    /**
     * Builds metadata for manual updates.
     */
    private Map<String, Object> buildMetadata(EnrichedContentUpdateRequest request, UpdatedFields fields) {
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("updatedFields", fields.updatedFields);
        if (request != null) {
            if (request.getEditedBy() != null) {
                metadata.put("editedBy", request.getEditedBy());
            }
            if (request.getNote() != null) {
                metadata.put("note", request.getNote());
            }
        }
        return metadata;
    }

    /**
     * Returns the incoming list or falls back to the existing value.
     */
    private List<String> resolveList(List<String> incoming, List<String> fallback) {
        if (incoming == null) {
            return fallback;
        }
        return sanitizeList(incoming);
    }

    /**
     * Returns the incoming text or falls back to the existing value.
     */
    private String resolveText(String incoming, String fallback) {
        if (incoming == null) {
            return fallback;
        }
        String normalized = normalizeText(incoming);
        return normalized;
    }

    /**
     * Resolves generated text when present, otherwise keeps the fallback.
     */
    private String resolveGeneratedText(String fallback, Object generatedValue) {
        if (generatedValue == null) {
            return fallback;
        }
        String resolved = normalizeText(extractString(generatedValue));
        return resolved != null ? resolved : fallback;
    }

    /**
     * Resolves generated list values when present, otherwise keeps the fallback.
     */
    private List<String> resolveGeneratedList(List<String> fallback, Object generatedValue) {
        if (generatedValue == null) {
            return fallback;
        }
        List<String> resolved = sanitizeList(normalizeListFromObject(generatedValue));
        return resolved != null ? resolved : fallback;
    }

    /**
     * Trims text and converts empty strings to null.
     */
    private String normalizeText(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * Converts an arbitrary object into a string representation.
     */
    private String extractString(Object value) {
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof List<?> list) {
            List<String> tokens = list.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .map(String::trim)
                    .filter(token -> !token.isEmpty())
                    .collect(Collectors.toList());
            if (!tokens.isEmpty()) {
                return String.join(", ", tokens);
            }
        }
        if (value != null) {
            return value.toString();
        }
        return null;
    }

    /**
     * Normalizes list values from arrays, JSON strings, or delimited text.
     */
    private List<String> normalizeListFromObject(Object value) {
        if (value instanceof List<?> list) {
            List<String> tokens = list.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .map(String::trim)
                    .filter(token -> !token.isEmpty())
                    .collect(Collectors.toList());
            return tokens;
        }
        if (value instanceof String s) {
            String trimmed = s.trim();
            if (trimmed.isEmpty()) {
                return List.of();
            }
            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                try {
                    return objectMapper.readValue(trimmed, new TypeReference<List<String>>() {});
                } catch (Exception ignored) {
                    // fall through
                }
            }
            String[] parts = trimmed.split("[,;|>]+");
            List<String> tokens = new ArrayList<>();
            for (String part : parts) {
                String token = part.trim();
                if (!token.isEmpty()) {
                    tokens.add(token);
                }
            }
            return tokens;
        }
        return List.of();
    }

    /**
     * Trims, deduplicates, and removes empty values from a list.
     */
    private List<String> sanitizeList(List<String> values) {
        if (values == null) {
            return null;
        }
        LinkedHashSet<String> deduped = new LinkedHashSet<>();
        for (String value : values) {
            if (value == null) continue;
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                deduped.add(trimmed);
            }
        }
        return new ArrayList<>(deduped);
    }

    /**
     * Normalizes requested field names and filters to supported fields.
     */
    private Set<String> normalizeFieldList(List<String> fields) {
        if (fields == null || fields.isEmpty()) {
            return Set.of();
        }
        return fields.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .map(String::toLowerCase)
                .filter(SUPPORTED_FIELDS::contains)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Determines which fields have changed from the existing element.
     */
    private List<String> determineUpdatedFields(EnrichedContentElement element, UpdatedFields fields) {
        List<String> updated = new ArrayList<>();
        if (!Objects.equals(element.getSummary(), fields.summary)) updated.add("summary");
        if (!Objects.equals(element.getClassification(), fields.classification)) updated.add("classification");
        if (!Objects.equals(safeList(element.getKeywords()), safeList(fields.keywords))) updated.add("keywords");
        if (!Objects.equals(safeList(element.getTags()), safeList(fields.tags))) updated.add("tags");
        return updated;
    }

    /**
     * Returns a non-null list for safe comparisons.
     */
    private List<String> safeList(List<String> values) {
        return values == null ? List.of() : values;
    }

    /**
     * Calls Bedrock to enrich the element and validates the response.
     */
    private Map<String, Object> callBedrock(EnrichedContentElement element) {
        Map<String, String> itemContent = new HashMap<>();
        itemContent.put("cleansedContent", element.getCleansedText());
        JsonNode itemJson = objectMapper.valueToTree(itemContent);

        EnrichmentContext context = null;
        if (element.getContext() != null) {
            try {
                context = objectMapper.convertValue(element.getContext(), EnrichmentContext.class);
            } catch (IllegalArgumentException e) {
                logger.warn("Unable to map enrichment context for element {}: {}", element.getId(), e.getMessage());
            }
        }

        Map<String, Object> result = bedrockEnrichmentService.enrichItem(itemJson, context);
        Map<String, Object> validationPayload = new HashMap<>(result);
        Map<String, Object> ctx = new HashMap<>();
        if (element.getContext() != null) {
            ctx.putAll(element.getContext());
        }
        ctx.put("fullContextId", element.getItemSourcePath() + "::" + element.getItemOriginalFieldName());
        ctx.put("sourcePath", element.getItemSourcePath());
        Map<String, Object> provenance = new HashMap<>();
        provenance.put("modelId", bedrockEnrichmentService.getConfiguredModelId());
        ctx.put("provenance", provenance);
        validationPayload.put("context", ctx);

        if (!aiResponseValidator.isValid(validationPayload)) {
            throw new IllegalStateException("Bedrock enrichment failed validation.");
        }
        return result;
    }

    /**
     * Synchronizes consolidated section rows after updates are applied.
     */
    private void syncConsolidatedSections(EnrichedContentElement element, UpdatedFields updatedFields) {
        if (element.getCleansedDataId() == null || element.getVersion() == null) {
            return;
        }
        if (element.getItemOriginalFieldName() == null) {
            return;
        }

        Set<String> usagePaths = new LinkedHashSet<>();
        String usagePathFromContext = extractUsagePath(element);
        if (usagePathFromContext != null && !usagePathFromContext.isBlank()) {
            usagePaths.add(usagePathFromContext);
        }

        if (element.getItemSourcePath() != null) {
            try {
                List<ContentHash> hashes = contentHashRepository
                        .findAllBySourcePathAndItemType(element.getItemSourcePath(), element.getItemOriginalFieldName());
                for (ContentHash hash : hashes) {
                    if (hash != null && hash.getUsagePath() != null && !hash.getUsagePath().isBlank()) {
                        usagePaths.add(hash.getUsagePath());
                    }
                }
            } catch (Exception e) {
                logger.debug("Unable to load usage paths for {}::{}: {}",
                        element.getItemSourcePath(), element.getItemOriginalFieldName(), e.getMessage());
            }
        }

        if (usagePaths.isEmpty() && element.getItemSourcePath() != null) {
            usagePaths.add(element.getItemSourcePath());
        }

        for (String usagePath : usagePaths) {
            String[] split = splitUsagePath(usagePath);
            String sectionPath = split[0];
            String sectionUri = split[1];
            if (sectionPath == null) sectionPath = element.getItemSourcePath();
            if (sectionUri == null) sectionUri = element.getItemSourcePath();

            consolidatedRepository
                    .findAllByCleansedDataIdAndVersionAndOriginalFieldNameAndSectionPathAndSectionUri(
                            element.getCleansedDataId(),
                            element.getVersion(),
                            element.getItemOriginalFieldName(),
                            sectionPath,
                            sectionUri
                    )
                    .forEach(section -> {
                        section.setSummary(updatedFields.summary);
                        section.setClassification(updatedFields.classification);
                        section.setKeywords(updatedFields.keywords);
                        section.setTags(updatedFields.tags);
                        if (updatedFields.modelUsed != null) {
                            section.setModelUsed(updatedFields.modelUsed);
                        }
                        section.setSavedAt(OffsetDateTime.now());
                        consolidatedRepository.save(section);
                    });
        }
    }

    /**
     * Reads usagePath from context, falling back to itemSourcePath.
     */
    private String extractUsagePath(EnrichedContentElement element) {
        Map<String, Object> ctx = element.getContext();
        if (ctx != null) {
            Object envelopeObj = ctx.get("envelope");
            if (envelopeObj instanceof Map<?, ?> env) {
                Object usagePath = env.get("usagePath");
                if (usagePath instanceof String s && !s.isBlank()) {
                    return s;
                }
            }
        }
        return element.getItemSourcePath();
    }

    /**
     * Splits a usagePath into section path and URI segments.
     */
    private String[] splitUsagePath(String usagePath) {
        if (usagePath == null || usagePath.isBlank()) return new String[]{null, null};
        int idx = usagePath.indexOf(USAGE_REF_DELIM);
        if (idx < 0) return new String[]{usagePath, usagePath};
        String left = usagePath.substring(0, idx).trim();
        String right = usagePath.substring(idx + USAGE_REF_DELIM.length()).trim();
        return new String[]{left.isEmpty() ? null : left, right.isEmpty() ? null : right};
    }

    private static class UpdatedFields {
        private String summary;
        private String classification;
        private List<String> keywords;
        private List<String> tags;
        private String modelUsed;
        private List<String> updatedFields = List.of();
    }
}
