package com.apple.springboot.service;

import com.apple.springboot.dto.CleansedContextResponse;
import com.apple.springboot.dto.EnrichmentResultResponse;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class EnrichmentReadService {

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;

    /**
     * Creates the read service used by enrichment view endpoints.
     */
    public EnrichmentReadService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                 EnrichedContentElementRepository enrichedContentElementRepository) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
    }

    /**
     * Loads a cleansed context snapshot and enriches it with metadata.
     */
    public Optional<CleansedContextResponse> loadCleansedContext(UUID cleansedId) {
        return cleansedDataStoreRepository.findById(cleansedId).map(store -> {
            CleansedContextResponse response = new CleansedContextResponse();
            response.setMetadata(buildMetadata(store));
            response.setStartedAt(asEpochMillis(store.getCleansedAt()));
            response.setStatus(store.getStatus());
            response.setStatusHistory(buildStatusHistory(store));
            response.setItems(extractItems(store));
            return response;
        });
    }

    /**
     * Loads enrichment results and aggregates metrics for the response.
     */
    public Optional<EnrichmentResultResponse> loadEnrichmentResult(UUID cleansedId) {
        List<EnrichedContentElement> elements = enrichedContentElementRepository
                .findByCleansedDataIdOrderByEnrichedAtAsc(cleansedId);
        if (elements.isEmpty()) {
            return Optional.empty();
        }
        EnrichmentResultResponse response = new EnrichmentResultResponse();
        response.setElements(elements);
        response.setMetrics(buildMetrics(elements));
        return Optional.of(response);
    }

    /**
     * Builds response metadata from a cleansed record.
     */
    private CleansedContextResponse.Metadata buildMetadata(CleansedDataStore store) {
        return CleansedContextResponse.buildMetadata(
                store.getId(),
                store.getSourceUri(),
                null,
                asEpochMillis(store.getCleansedAt()),
                store.getVersion(),
                null
        );
    }

    /**
     * Creates a basic status history for the cleansed record.
     */
    private List<CleansedContextResponse.StatusEntry> buildStatusHistory(CleansedDataStore store) {
        List<CleansedContextResponse.StatusEntry> history = new ArrayList<>();
        history.add(CleansedContextResponse.buildStatusEntry("ENRICHMENT_TRIGGERED", store.getCleansedAt()));
        if (store.getStatus() != null) {
            history.add(CleansedContextResponse.buildStatusEntry(store.getStatus(), OffsetDateTime.now()));
        }
        return history;
    }

    /**
     * Returns the stored cleansed items list or an empty list.
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> extractItems(CleansedDataStore store) {
        List<Map<String, Object>> cleansedItems = store.getCleansedItems();
        if (cleansedItems == null) {
            return Collections.emptyList();
        }
        return cleansedItems;
    }

    /**
     * Aggregates metrics across enriched content elements.
     */
    private EnrichmentResultResponse.Metrics buildMetrics(List<EnrichedContentElement> elements) {
        EnrichmentResultResponse.Metrics metrics = new EnrichmentResultResponse.Metrics();
        Integer fieldsTagged = sumIntegers(elements, "getFieldsTagged");
        metrics.setTotalFieldsTagged(fieldsTagged != null ? fieldsTagged : elements.size());

        Double readability = averageDoubles(elements, "getReadabilityDelta");
        metrics.setReadabilityImproved(readability);

        Integer errorsFound = sumIntegers(elements, "getErrorsFound");
        metrics.setErrorsFound(errorsFound);
        return metrics;
    }

    /**
     * Converts a timestamp into epoch milliseconds.
     */
    private Long asEpochMillis(OffsetDateTime timestamp) {
        return timestamp != null ? timestamp.toInstant().toEpochMilli() : null;
    }

    /**
     * Sums integer values from a numeric getter on each element.
     */
    private Integer sumIntegers(List<EnrichedContentElement> elements, String getterName) {
        int sum = 0;
        boolean found = false;
        for (EnrichedContentElement element : elements) {
            Number number = invokeNumberGetter(element, getterName);
            if (number != null) {
                sum += number.intValue();
                found = true;
            }
        }
        return found ? sum : null;
    }

    /**
     * Computes the average of a numeric getter across all elements.
     */
    private Double averageDoubles(List<EnrichedContentElement> elements, String getterName) {
        double total = 0;
        int count = 0;
        for (EnrichedContentElement element : elements) {
            Number number = invokeNumberGetter(element, getterName);
            if (number != null) {
                total += number.doubleValue();
                count++;
            }
        }
        return count > 0 ? total / count : null;
    }

    /**
     * Invokes a numeric getter by name using reflection.
     */
    private Number invokeNumberGetter(EnrichedContentElement element, String getterName) {
        try {
            Method method = element.getClass().getMethod(getterName);
            Object value = method.invoke(element);
            if (value instanceof Number) {
                return (Number) value;
            }
        } catch (ReflectiveOperationException ignored) {
            // If the method does not exist on the entity we simply skip it.
        }
        return null;
    }
}
