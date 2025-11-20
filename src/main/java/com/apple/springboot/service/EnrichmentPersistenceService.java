package com.apple.springboot.service;

import com.apple.springboot.service.CleansedItemDetail;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class EnrichmentPersistenceService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentPersistenceService.class);

    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final ObjectMapper objectMapper;
    private final EntityManager entityManager;

    public EnrichmentPersistenceService(EnrichedContentElementRepository enrichedContentElementRepository,
                                        ObjectMapper objectMapper,
                                        EntityManager entityManager) {
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.objectMapper = objectMapper;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry,
                                    Map<String, Object> bedrockResponse, String elementStatus) throws JsonProcessingException {

        // Find existing element or create a new one to prevent duplicates.
        Optional<EnrichedContentElement> existingElementOpt = enrichedContentElementRepository.findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(
                parentEntry.getId(), itemDetail.sourcePath, itemDetail.originalFieldName);

        EnrichedContentElement enrichedElement = existingElementOpt.orElse(new EnrichedContentElement());

        if (enrichedElement.getId() == null) { // It's a new entity
            enrichedElement.setCleansedDataId(parentEntry.getId());
            enrichedElement.setVersion(parentEntry.getVersion());
            enrichedElement.setSourceUri(parentEntry.getSourceUri());
            enrichedElement.setItemSourcePath(itemDetail.sourcePath);
            enrichedElement.setItemOriginalFieldName(itemDetail.originalFieldName);
            enrichedElement.setItemModelHint(itemDetail.model);
        }

        enrichedElement.setCleansedText(itemDetail.cleansedContent);
        enrichedElement.setEnrichedAt(OffsetDateTime.now());
        enrichedElement.setContext(objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));

        @SuppressWarnings("unchecked")
        Map<String, Object> standardEnrichments = (Map<String, Object>) bedrockResponse.getOrDefault("standardEnrichments", bedrockResponse);

        enrichedElement.setSummary((String) standardEnrichments.get("summary"));
        enrichedElement.setSentiment((String) standardEnrichments.get("sentiment"));
        enrichedElement.setClassification((String) standardEnrichments.get("classification"));
        enrichedElement.setKeywords((List<String>) standardEnrichments.get("keywords"));
        enrichedElement.setTags((List<String>) standardEnrichments.get("tags"));
        enrichedElement.setBedrockModelUsed((String) bedrockResponse.get("enrichedWithModel"));
        enrichedElement.setStatus(elementStatus);
        Map<String, Object> metadata = new HashMap<>();
        metadata.put("enrichedWithModel", bedrockResponse.get("enrichedWithModel"));
        metadata.put("enrichmentTimestamp", enrichedElement.getEnrichedAt().toString());
        try {
            enrichedElement.setEnrichmentMetadata(objectMapper.writeValueAsString(metadata));
        } catch (JsonProcessingException e) {
            logger.warn("Could not serialize enrichment metadata for item path: {}", itemDetail.sourcePath, e);
            enrichedElement.setEnrichmentMetadata("{\"error\":\"Could not serialize metadata\"}");
        }

        enrichedContentElementRepository.save(enrichedElement);
        entityManager.flush();
        logger.info("Persisted {} status for CleansedDataStore ID {}", elementStatus, parentEntry.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveErrorEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry, String status, String errorMessage) {

        Optional<EnrichedContentElement> existingElementOpt = enrichedContentElementRepository.findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(
                parentEntry.getId(), itemDetail.sourcePath, itemDetail.originalFieldName);

        EnrichedContentElement errorElement = existingElementOpt.orElse(new EnrichedContentElement());

        if (errorElement.getId() == null) { // It's a new entity
            errorElement.setCleansedDataId(parentEntry.getId());
            errorElement.setVersion(parentEntry.getVersion());
            errorElement.setSourceUri(parentEntry.getSourceUri());
            errorElement.setItemSourcePath(itemDetail.sourcePath);
            errorElement.setItemOriginalFieldName(itemDetail.originalFieldName);
            errorElement.setItemModelHint(itemDetail.model);
        }

        errorElement.setCleansedText(itemDetail.cleansedContent);
        errorElement.setEnrichedAt(OffsetDateTime.now());
        errorElement.setStatus(status);
        errorElement.setContext(objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));

        Map<String, Object> bedrockMeta = new HashMap<>();
        bedrockMeta.put("enrichmentError", errorMessage);
        try {
            errorElement.setEnrichmentMetadata(objectMapper.writeValueAsString(bedrockMeta));
        } catch (JsonProcessingException e) {
            errorElement.setEnrichmentMetadata("{\"error\":\"Could not serialize error metadata\"}");
        }

        enrichedContentElementRepository.save(errorElement);
        entityManager.flush();
        logger.debug("Saved error element for item path: {}", itemDetail.sourcePath);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveSkippedEnrichedElement(CleansedItemDetail itemDetail, CleansedDataStore parentEntry, String status) {

        Optional<EnrichedContentElement> existingElementOpt = enrichedContentElementRepository.findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(
                parentEntry.getId(), itemDetail.sourcePath, itemDetail.originalFieldName);

        EnrichedContentElement skippedElement = existingElementOpt.orElse(new EnrichedContentElement());

        if (skippedElement.getId() == null) {
            skippedElement.setCleansedDataId(parentEntry.getId());
            skippedElement.setVersion(parentEntry.getVersion());
            skippedElement.setSourceUri(parentEntry.getSourceUri());
            skippedElement.setItemSourcePath(itemDetail.sourcePath);
            skippedElement.setItemOriginalFieldName(itemDetail.originalFieldName);
            skippedElement.setItemModelHint(itemDetail.model);
        }

        skippedElement.setCleansedText(itemDetail.cleansedContent);
        skippedElement.setEnrichedAt(OffsetDateTime.now());
        skippedElement.setStatus(status);
        skippedElement.setContext(objectMapper.convertValue(itemDetail.context, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {}));

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("enrichmentSkipped", true);
        metadata.put("reason", "Excluded via configuration");
        metadata.put("timestamp", skippedElement.getEnrichedAt().toString());
        try {
            skippedElement.setEnrichmentMetadata(objectMapper.writeValueAsString(metadata));
        } catch (JsonProcessingException e) {
            skippedElement.setEnrichmentMetadata("{\"enrichmentSkipped\":true}");
        }

        enrichedContentElementRepository.save(skippedElement);
        entityManager.flush();
        logger.debug("Saved skipped enrichment element for item path: {}", itemDetail.sourcePath);
    }
}
