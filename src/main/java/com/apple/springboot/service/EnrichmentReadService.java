package com.apple.springboot.service;

import com.apple.springboot.dto.CleansedContextResponse;
import com.apple.springboot.dto.EnrichmentResultResponse;
import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.EnrichedContentElement;
import com.apple.springboot.model.RawDataStore;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.EnrichedContentElementRepository;
import com.apple.springboot.repository.RawDataStoreRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

@Service
public class EnrichmentReadService {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentReadService.class);
    private static final List<String> LOCALE_KEYS = List.of(
            "locale", "localeCode", "locale_code", "languageLocale", "language_locale"
    );
    private static final List<String> PAGE_ID_KEYS = List.of("pageId", "page_id", "pageID");
    private static final int MAX_METADATA_NODES = 1500;

    private final CleansedDataStoreRepository cleansedDataStoreRepository;
    private final EnrichedContentElementRepository enrichedContentElementRepository;
    private final RawDataStoreRepository rawDataStoreRepository;
    private final ObjectMapper objectMapper;

    /**
     * Creates the read service used by enrichment view endpoints.
     */
    public EnrichmentReadService(CleansedDataStoreRepository cleansedDataStoreRepository,
                                 EnrichedContentElementRepository enrichedContentElementRepository,
                                 RawDataStoreRepository rawDataStoreRepository,
                                 ObjectMapper objectMapper) {
        this.cleansedDataStoreRepository = cleansedDataStoreRepository;
        this.enrichedContentElementRepository = enrichedContentElementRepository;
        this.rawDataStoreRepository = rawDataStoreRepository;
        this.objectMapper = objectMapper;
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
    public CleansedContextResponse.Metadata describeMetadata(CleansedDataStore store) {
        return buildMetadata(store);
    }

    /**
     * Builds response metadata from a cleansed record.
     */
    private CleansedContextResponse.Metadata buildMetadata(CleansedDataStore store) {
        LocalePageId localePageId = resolveLocaleAndPageId(store);
        return CleansedContextResponse.buildMetadata(
                store.getId(),
                store.getSourceUri(),
                null,
                asEpochMillis(store.getCleansedAt()),
                store.getVersion(),
                null,
                localePageId.locale(),
                localePageId.pageId()
        );
    }

    /**
     * Resolves locale and page ID values from available ingestion metadata.
     */
    private LocalePageId resolveLocaleAndPageId(CleansedDataStore store) {
        LocalePageId fromContext = extractFromItems(store.getCleansedItems());
        LocalePageId fromMetadata = extractFromRawMetadata(store);
        String locale = firstNonBlank(fromContext.locale(), fromMetadata.locale());
        String pageId = firstNonBlank(fromContext.pageId(), fromMetadata.pageId());
        return new LocalePageId(locale, pageId);
    }

    /**
     * Extracts locale and page ID values from stored raw metadata JSON.
     */
    private LocalePageId extractFromRawMetadata(CleansedDataStore store) {
        UUID rawDataId = store.getRawDataId();
        if (rawDataId == null) {
            return new LocalePageId(null, null);
        }
        Optional<RawDataStore> rawDataOpt = rawDataStoreRepository.findById(rawDataId);
        if (rawDataOpt.isEmpty()) {
            return new LocalePageId(null, null);
        }
        String metadataJson = rawDataOpt.get().getSourceMetadata();
        if (metadataJson == null || metadataJson.isBlank()) {
            return new LocalePageId(null, null);
        }
        try {
            JsonNode node = objectMapper.readTree(metadataJson);
            return extractLocaleAndPageId(node);
        } catch (Exception e) {
            logger.warn("Failed to parse source metadata for locale/pageId extraction. Raw data ID: {}", rawDataId, e);
            return new LocalePageId(null, null);
        }
    }

    /**
     * Extracts locale and page ID values from cleansed item context maps.
     */
    private LocalePageId extractFromItems(List<Map<String, Object>> items) {
        if (items == null || items.isEmpty()) {
            return new LocalePageId(null, null);
        }
        String locale = null;
        String pageId = null;
        for (Map<String, Object> item : items) {
            if (item == null || (locale != null && pageId != null)) {
                continue;
            }
            Object context = item.get("context");
            if (context == null) {
                continue;
            }
            LocalePageId candidate = extractLocaleAndPageId(context);
            if (locale == null) {
                locale = candidate.locale();
            }
            if (pageId == null) {
                pageId = candidate.pageId();
            }
        }
        return new LocalePageId(locale, pageId);
    }

    /**
     * Attempts to extract locale and page ID values from an object tree.
     */
    private LocalePageId extractLocaleAndPageId(Object payload) {
        if (payload == null) {
            return new LocalePageId(null, null);
        }
        JsonNode node = payload instanceof JsonNode
                ? (JsonNode) payload
                : objectMapper.valueToTree(payload);
        return extractLocaleAndPageId(node);
    }

    /**
     * Traverses a JsonNode tree to locate locale and page ID values.
     */
    private LocalePageId extractLocaleAndPageId(JsonNode payload) {
        if (payload == null || payload.isNull()) {
            return new LocalePageId(null, null);
        }
        Deque<JsonNode> stack = new ArrayDeque<>();
        stack.push(payload);
        Map<JsonNode, Boolean> visited = new IdentityHashMap<>();
        int scanned = 0;
        String locale = null;
        String pageId = null;

        while (!stack.isEmpty() && scanned < MAX_METADATA_NODES && (locale == null || pageId == null)) {
            JsonNode current = stack.pop();
            if (current == null || current.isNull() || visited.put(current, Boolean.TRUE) != null) {
                continue;
            }
            scanned += 1;
            if (current.isObject()) {
                if (locale == null) {
                    locale = pickStringKey(current, LOCALE_KEYS, this::normalizeLocale);
                }
                if (pageId == null) {
                    pageId = pickStringKey(current, PAGE_ID_KEYS, Function.identity());
                }
                current.fields().forEachRemaining(entry -> {
                    JsonNode value = entry.getValue();
                    if (value != null && value.isContainerNode()) {
                        stack.push(value);
                    }
                });
            } else if (current.isArray()) {
                current.forEach(entry -> {
                    if (entry != null && entry.isContainerNode()) {
                        stack.push(entry);
                    }
                });
            }
        }

        return new LocalePageId(locale, pageId);
    }

    /**
     * Reads the first non-blank string value for the provided keys.
     */
    private String pickStringKey(JsonNode node, List<String> keys, Function<String, String> normalizer) {
        for (String key : keys) {
            JsonNode valueNode = node.get(key);
            String value = textOrNull(valueNode);
            if (value != null) {
                return normalizer.apply(value);
            }
        }
        return null;
    }

    /**
     * Normalizes locale values into the ll_CC format when possible.
     */
    private String normalizeLocale(String locale) {
        if (locale == null) {
            return null;
        }
        String trimmed = locale.trim();
        if (trimmed.isBlank()) {
            return null;
        }
        String normalized = trimmed.replace('-', '_');
        if (normalized.length() == 5 && normalized.charAt(2) == '_') {
            String language = normalized.substring(0, 2).toLowerCase(java.util.Locale.ROOT);
            String country = normalized.substring(3).toUpperCase(java.util.Locale.ROOT);
            return language + "_" + country;
        }
        return trimmed;
    }

    /**
     * Returns the first non-blank string value.
     */
    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        if (second != null && !second.isBlank()) {
            return second;
        }
        return null;
    }

    /**
     * Converts a JsonNode to a trimmed string when possible.
     */
    private String textOrNull(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        String value = node.asText(null);
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    /**
     * Holder for locale and page ID values.
     */
    private record LocalePageId(String locale, String pageId) {}

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
