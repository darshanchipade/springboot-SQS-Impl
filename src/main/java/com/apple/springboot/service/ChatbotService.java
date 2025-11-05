package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ChatbotService {
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;

    private static final Pattern SECTION_KEY_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");
    private static final Pattern LOCALE_PATTERN =
            Pattern.compile("(?i)\\b([a-z]{2})[-_]([a-z]{2})\\b");
    private static final Set<String> STOP_WORDS = Set.of(
            "give", "me", "all", "the", "content", "for", "please", "show", "accessibility",
            "accessibilitytext", "text", "copy", "section", "sections", "ribbon", "and", "or", "with", "get",
            "need", "want", "results", "data", "information", "info", "help", "list", "of", "to",
            "an", "a", "on", "in", "by", "from"
    );
    private static final Set<String> ISO_LANGUAGE_CODES = Collections.unmodifiableSet(
            Arrays.stream(Locale.getISOLanguages())
                    .map(code -> code.toLowerCase(Locale.ROOT))
                    .collect(Collectors.toSet())
    );
    private static final Set<String> ISO_COUNTRY_CODES = Collections.unmodifiableSet(
            Arrays.stream(Locale.getISOCountries())
                    .map(code -> code.toUpperCase(Locale.ROOT))
                    .collect(Collectors.toSet())
    );
    private static final Map<String, String> COUNTRY_NAME_INDEX = buildCountryNameIndex();

    public ChatbotService(VectorSearchService vectorSearchService,
                          ConsolidatedEnrichedSectionRepository consolidatedRepo) {
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
    }

    public List<ChatbotResultDto> query(ChatbotRequest request) {
        String key = request != null ? normalizeKey(request.getSectionKey()) : null;
        if (!StringUtils.hasText(key)) {
            key = extractKey(request != null ? request.getMessage() : null);
        }
        if (!StringUtils.hasText(key)) {
            return List.of();
        }

        int limit = (request != null && request.getLimit() != null && request.getLimit() > 0)
                ? Math.min(request.getLimit(), 200)
                : 15;

        boolean hasRoleQuery = request != null && StringUtils.hasText(request.getOriginal_field_name());
        int vectorPreLimit = hasRoleQuery ? Math.min(limit * 3, 100) : limit;

        try {
            String message = request != null ? request.getMessage() : null;
            String embeddingQuery = StringUtils.hasText(message) ? message : key;
            LocaleCriteria localeCriteria = extractLocaleCriteria(message);
            if (request != null && request.getContext() != null) {
                enrichCriteriaFromContext(localeCriteria, request.getContext());
            }

            Map<String, Object> localeContext = buildLocaleContext(localeCriteria);
            Map<String, Object> effectiveContext = mergeContextFilters(
                    request != null ? request.getContext() : null,
                    localeContext
            );

            // Vector: do NOT hard-filter by original_field_name to allow partial queries
            List<ContentChunkWithDistance> results = vectorSearchService.search(
                    embeddingQuery,
                    null, // avoid strict equality in SQL for partial role queries
                    vectorPreLimit,
                    request != null ? request.getTags() : null,
                    request != null ? request.getKeywords() : null,
                    effectiveContext.isEmpty() ? null : effectiveContext,
                    null // threshold
            );

            final String sectionKeyFinal = key;

            List<ChatbotResultDto> vectorDtos = results.stream()
                    .map(r -> {
                        var chunk = r.getContentChunk();
                        var section = chunk.getConsolidatedEnrichedSection();
                        ChatbotResultDto dto = new ChatbotResultDto();
                        dto.setSection(sectionKeyFinal);
                        dto.setSectionPath(section.getSectionPath());
                          dto.setSectionUri(section.getSectionUri());
                          dto.setCleansedText(section.getCleansedText());
                          dto.setSource("content_chunks");
                          dto.setContentRole(section.getOriginalFieldName());
                          dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                          dto.setMatchTerms(List.of(sectionKeyFinal));
                          String locale = extractLocale(section);
                          String language = extractLanguage(section, locale);
                          String country = extractCountry(section, locale);
                          if (locale != null) {
                              int idx = locale.indexOf('_');
                              if (idx > 0) {
                                  if (language == null) {
                                      language = locale.substring(0, idx);
                                  }
                                  if (country == null) {
                                      country = locale.substring(idx + 1);
                                  }
                              }
                          }
                          dto.setLocale(locale);
                          dto.setCountry(country);
                          dto.setLanguage(language);
                          dto.setTenant(extractTenant(section));
                          dto.setPageId(extractPageId(section));
                        return dto;
                    })
                    .collect(Collectors.toList());

            vectorDtos = applyCriteriaFilter(vectorDtos, localeCriteria);

            // Post-filter vector by partial role (case-insensitive) when provided
            if (hasRoleQuery) {
                String roleFilter = request.getOriginal_field_name().toLowerCase();
                vectorDtos = vectorDtos.stream()
                        .filter(d -> d.getContentRole() != null && d.getContentRole().toLowerCase().contains(roleFilter))
                        .collect(Collectors.toList());
            }

            // Consolidated: search metadata (incl. context usagePath) using full message if available; otherwise section key
            List<ConsolidatedEnrichedSection> consolidatedMatches;
            if (StringUtils.hasText(message)) {
                consolidatedMatches = consolidatedRepo.findByMetadataQuery(message, Math.max(limit * 2, 50));
            } else {
                consolidatedMatches = consolidatedRepo.findBySectionKey(sectionKeyFinal, Math.max(limit * 2, 50));
            }

            // Partial role filtering (contains, case-insensitive) for consolidated
            if (hasRoleQuery) {
                final String roleFilter = request.getOriginal_field_name().toLowerCase();
                consolidatedMatches = consolidatedMatches.stream()
                        .filter(s -> s.getOriginalFieldName() != null
                                && s.getOriginalFieldName().toLowerCase().contains(roleFilter))
                        .collect(Collectors.toList());
            }

            List<ChatbotResultDto> consolidatedDtos = consolidatedMatches.stream()
                    .map(section -> {
                        ChatbotResultDto dto = new ChatbotResultDto();
                        dto.setSection(sectionKeyFinal);
                        dto.setSectionPath(section.getSectionPath());
                        dto.setSectionUri(section.getSectionUri());
                          dto.setCleansedText(section.getCleansedText());
                          dto.setSource("consolidated_enriched_sections");
                          dto.setContentRole(section.getOriginalFieldName());
                          dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                          dto.setMatchTerms(List.of(sectionKeyFinal));
                          String locale = extractLocale(section);
                          String language = extractLanguage(section, locale);
                          String country = extractCountry(section, locale);
                          if (locale != null) {
                              int idx = locale.indexOf('_');
                              if (idx > 0) {
                                  if (language == null) {
                                      language = locale.substring(0, idx);
                                  }
                                  if (country == null) {
                                      country = locale.substring(idx + 1);
                                  }
                              }
                          }
                          dto.setLocale(locale);
                          dto.setCountry(country);
                          dto.setLanguage(language);
                          dto.setTenant(extractTenant(section));
                          dto.setPageId(extractPageId(section));
                        return dto;
                    })
                    .collect(Collectors.toList());

            consolidatedDtos = applyCriteriaFilter(consolidatedDtos, localeCriteria);

            // Merge vector-first, then consolidated; dedupe by section_path + content_role
            LinkedHashMap<String, ChatbotResultDto> merged = new LinkedHashMap<>();
            for (ChatbotResultDto d : vectorDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath())
                        + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }
            for (ChatbotResultDto d : consolidatedDtos) {
                String dedupKey = (d.getSectionPath() == null ? "" : d.getSectionPath())
                        + "|" + (d.getContentRole() == null ? "" : d.getContentRole());
                merged.putIfAbsent(dedupKey, d);
            }

            List<ChatbotResultDto> mergedList = new ArrayList<>(merged.values());

            // Trim to limit after filtering
            if (mergedList.size() > limit) {
                mergedList = mergedList.subList(0, limit);
            }

            // Assign cf ids; enrich match_terms with tags/keywords and role if provided
            for (int i = 0; i < mergedList.size(); i++) {
                ChatbotResultDto item = mergedList.get(i);
                item.setCfId("cf" + (i + 1));

                var terms = new java.util.LinkedHashSet<String>();
                terms.add(sectionKeyFinal);
                if (hasRoleQuery) terms.add(request.getOriginal_field_name());
                if (request != null && request.getTags() != null) terms.addAll(request.getTags());
                if (request != null && request.getKeywords() != null) terms.addAll(request.getKeywords());
                if (!localeCriteria.pageIds.isEmpty()) terms.addAll(localeCriteria.pageIds);
                item.setMatchTerms(new ArrayList<>(terms));
            }

            return mergedList;
        } catch (Exception e) {
            return List.of();
        }
    }

    private String extractKey(String message) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        Matcher m = SECTION_KEY_PATTERN.matcher(message);
        if (m.find()) {
            return normalizeKey(m.group(0));
        }
        for (String token : message.split("\\s+")) {
            long hyphens = token.chars().filter(ch -> ch == '-').count();
            if (hyphens >= 2) {
                return normalizeKey(token);
            }
        }
        return null;
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) return null;
        return key.trim().toLowerCase();
    }

    private String extractLocale(ConsolidatedEnrichedSection s) {
        String fromContext = normalizeLocale(getEnvelopeValue(s, "locale"));
        if (StringUtils.hasText(fromContext)) {
            return fromContext;
        }
        String fromPath = normalizeLocale(extractLocaleFromPath(s.getSectionUri()));
        if (fromPath == null) fromPath = normalizeLocale(extractLocaleFromPath(s.getSectionPath()));
        return fromPath;
    }

    private String extractLanguage(ConsolidatedEnrichedSection s, String localeFallback) {
        String fromContext = getEnvelopeValue(s, "language");
        if (StringUtils.hasText(fromContext)) {
            return fromContext.toLowerCase(Locale.ROOT);
        }
        if (StringUtils.hasText(localeFallback)) {
            String normalized = normalizeLocale(localeFallback);
            if (!StringUtils.hasText(normalized)) {
                return null;
            }
            int idx = normalized.indexOf('_');
            if (idx > 0) {
                return normalized.substring(0, idx).toLowerCase(Locale.ROOT);
            }
        }
        return null;
    }

    private String extractCountry(ConsolidatedEnrichedSection s, String localeFallback) {
        String fromContext = getEnvelopeValue(s, "country");
        if (StringUtils.hasText(fromContext)) {
            return fromContext.toUpperCase(Locale.ROOT);
        }
        if (StringUtils.hasText(localeFallback)) {
            String normalized = normalizeLocale(localeFallback);
            if (!StringUtils.hasText(normalized)) {
                return null;
            }
            int idx = normalized.indexOf('_');
            if (idx >= 0 && idx + 1 < normalized.length()) {
                return normalized.substring(idx + 1).toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    private String extractTenant(ConsolidatedEnrichedSection s) {
        String fromContext = getEnvelopeValue(s, "tenant");
        if (StringUtils.hasText(fromContext)) {
            return fromContext;
          }
        String fromUri = extractTenantFromPath(s.getSectionUri());
        if (fromUri != null) return fromUri;
        String fromPath = extractTenantFromPath(s.getSectionPath());
        return fromPath != null ? fromPath : "applecom-cms";
    }

    private String extractPageId(ConsolidatedEnrichedSection s) {
        String pid = normalizePageId(extractPageIdFromPath(s.getSectionUri()));
        if (pid == null) pid = normalizePageId(extractPageIdFromPath(s.getSectionPath()));
        if (pid == null) pid = normalizePageId(getFacetValue(s, "pageId"));
        return pid;
    }

    private String extractLocaleFromPath(String path) {
        if (!StringUtils.hasText(path)) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/([a-z]{2}_[A-Z]{2})/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractTenantFromPath(String path) {
        if (!StringUtils.hasText(path)) return null;
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/content/dam/([^/]+)/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractPageIdFromPath(String path) {
        if (!StringUtils.hasText(path)) return null;
        // Expect ... /<locale>/<pageId>/ ...
        java.util.regex.Matcher m = java.util.regex.Pattern
                .compile("/[a-z]{2}_[A-Z]{2}/([^/]+)/")
                .matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

      private String getEnvelopeValue(ConsolidatedEnrichedSection section, String key) {
          if (section == null || section.getContext() == null) {
              return null;
          }
          Object envelope = section.getContext().get("envelope");
          if (envelope instanceof Map<?,?> map) {
              Object value = map.get(key);
              if (value instanceof String str && StringUtils.hasText(str)) {
                  return str;
              }
          }
          return null;
      }

    private String getFacetValue(ConsolidatedEnrichedSection section, String key) {
        if (section == null || section.getContext() == null) {
            return null;
        }
        Object facets = section.getContext().get("facets");
        if (facets instanceof Map<?, ?> map) {
            Object value = map.get(key);
            if (value instanceof String str && StringUtils.hasText(str)) {
                return str;
            }
            if (value instanceof Collection<?> collection) {
                for (Object item : collection) {
                    if (item instanceof String str && StringUtils.hasText(str)) {
                        return str;
                    }
                }
            }
        }
        return null;
    }

    private String normalizeLocale(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        String trimmed = raw.trim().replace('-', '_');
        String[] parts = trimmed.split("_");
        if (parts.length == 2) {
            String language = parts[0].toLowerCase(Locale.ROOT);
            String country = parts[1].toUpperCase(Locale.ROOT);
            return language + "_" + country;
        }
        return null;
    }

    private String normalizePageId(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    private LocaleCriteria extractLocaleCriteria(String message) {
        final LocaleCriteria criteria = new LocaleCriteria();
        if (!StringUtils.hasText(message)) {
            return criteria;
        }

        Matcher matcher = LOCALE_PATTERN.matcher(message);
        while (matcher.find()) {
            String language = matcher.group(1).toLowerCase(Locale.ROOT);
            String country = matcher.group(2).toUpperCase(Locale.ROOT);
            String locale = language + "_" + country;
            if (ISO_LANGUAGE_CODES.contains(language) && ISO_COUNTRY_CODES.contains(country)) {
                criteria.locales.add(locale);
                criteria.languages.add(language);
                criteria.countries.add(country);
            }
        }

        String messageLower = message.toLowerCase(Locale.ROOT);
        COUNTRY_NAME_INDEX.forEach((name, code) -> {
            if (messageLower.contains(name)) {
                criteria.countries.add(code);
            }
        });

        String[] tokens = message.split("[^A-Za-z0-9_]+");
        for (String token : tokens) {
            if (!StringUtils.hasText(token)) continue;
            String trimmed = token.trim();
            String lower = trimmed.toLowerCase(Locale.ROOT);

            if (LOCALE_PATTERN.matcher(trimmed).matches()) {
                continue;
            }

            String mappedCountry = mapCountryCode(trimmed);
            boolean recognized = false;
            if (mappedCountry != null) {
                criteria.countries.add(mappedCountry);
                recognized = true;
            }

            if (ISO_LANGUAGE_CODES.contains(lower)) {
                criteria.languages.add(lower);
                recognized = true;
            }

            if (!recognized && isPotentialPageIdToken(lower)) {
                criteria.pageIds.add(lower);
            }
        }

        return criteria;
    }

    private void enrichCriteriaFromContext(LocaleCriteria criteria, Map<String, Object> context) {
        if (context == null || context.isEmpty()) {
            return;
        }
        addContextValue(criteria, context.get("locale"), ValueType.LOCALE);
        addContextValue(criteria, context.get("country"), ValueType.COUNTRY);
        addContextValue(criteria, context.get("language"), ValueType.LANGUAGE);
        addContextValue(criteria, context.get("pageId"), ValueType.PAGE_ID);

        Object envelope = context.get("envelope");
        if (envelope instanceof Map<?, ?> envMap) {
            Map<?, ?> env = envMap;
            addContextValue(criteria, env.get("locale"), ValueType.LOCALE);
            addContextValue(criteria, env.get("country"), ValueType.COUNTRY);
            addContextValue(criteria, env.get("language"), ValueType.LANGUAGE);
        }

        Object facets = context.get("facets");
        if (facets instanceof Map<?, ?> facetsMap) {
            Map<?, ?> fm = facetsMap;
            addContextValue(criteria, fm.get("pageId"), ValueType.PAGE_ID);
            addContextValue(criteria, fm.get("locale"), ValueType.LOCALE);
        }
    }

    private void addContextValue(LocaleCriteria criteria, Object value, ValueType type) {
        if (value == null) {
            return;
        }
        if (value instanceof String str) {
            addValue(criteria, str, type);
        } else if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                if (item instanceof String strItem) {
                    addValue(criteria, strItem, type);
                }
            }
        }
    }

    private void addValue(LocaleCriteria criteria, String value, ValueType type) {
        if (!StringUtils.hasText(value)) {
            return;
        }
        switch (type) {
            case LOCALE -> {
                String normalized = normalizeLocale(value);
                if (StringUtils.hasText(normalized)) {
                    criteria.locales.add(normalized);
                    int idx = normalized.indexOf('_');
                    if (idx > 0) {
                        criteria.languages.add(normalized.substring(0, idx));
                        String mapped = mapCountryCode(normalized.substring(idx + 1));
                        if (mapped != null) {
                            criteria.countries.add(mapped);
                        }
                    }
                }
            }
            case COUNTRY -> {
                String mapped = mapCountryCode(value);
                if (mapped != null) {
                    criteria.countries.add(mapped);
                }
            }
            case LANGUAGE -> {
                String lower = value.toLowerCase(Locale.ROOT);
                if (ISO_LANGUAGE_CODES.contains(lower)) {
                    criteria.languages.add(lower);
                }
            }
            case PAGE_ID -> {
                String lower = value.toLowerCase(Locale.ROOT);
                if (!lower.isBlank() && !STOP_WORDS.contains(lower)) {
                    criteria.pageIds.add(lower);
                }
            }
        }
    }

    private List<ChatbotResultDto> applyCriteriaFilter(List<ChatbotResultDto> dtos, LocaleCriteria criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return dtos;
        }
        List<ChatbotResultDto> filtered = dtos.stream()
                .filter(dto -> matchesLocaleCriteria(dto, criteria) && matchesPageCriteria(dto, criteria.pageIds))
                .collect(Collectors.toList());
        if (filtered.isEmpty() && criteria != null && !criteria.pageIds.isEmpty()) {
            filtered = dtos.stream()
                    .filter(dto -> matchesLocaleCriteria(dto, criteria))
                    .collect(Collectors.toList());
        }
        return filtered;
    }

    private boolean matchesLocaleCriteria(ChatbotResultDto dto, LocaleCriteria criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return true;
        }

        String locale = normalizeLocale(dto.getLocale());
        if (locale == null) {
            locale = normalizeLocale(extractLocaleFromPath(dto.getSectionUri()));
            if (locale == null) {
                locale = normalizeLocale(extractLocaleFromPath(dto.getSectionPath()));
            }
        }

        String country = dto.getCountry() != null ? mapCountryCode(dto.getCountry()) : null;
        String language = dto.getLanguage() != null ? dto.getLanguage().toLowerCase(Locale.ROOT) : null;

        if (locale != null) {
            int idx = locale.indexOf('_');
            if (idx > 0) {
                if (language == null) {
                    language = locale.substring(0, idx);
                }
                if (country == null) {
                        country = locale.substring(idx + 1);
                }
            }
        }

        boolean localeOk = criteria.locales.isEmpty() || (locale != null && criteria.locales.contains(locale));
        boolean languageOk = criteria.languages.isEmpty() || (language != null && criteria.languages.contains(language));
        boolean countryOk = criteria.countries.isEmpty()
                || (country != null && criteria.countries.contains(country))
                || matchesCountryFromPath(dto, criteria.countries);

        return localeOk && languageOk && countryOk;
    }

    private boolean matchesCountryFromPath(ChatbotResultDto dto, Set<String> countries) {
        if (countries == null || countries.isEmpty()) {
            return true;
        }
        String uri = dto.getSectionUri() != null ? dto.getSectionUri().toUpperCase(Locale.ROOT) : null;
        String path = dto.getSectionPath() != null ? dto.getSectionPath().toUpperCase(Locale.ROOT) : null;
        for (String target : countries) {
            String normalized = target.toUpperCase(Locale.ROOT);
            String needle = "_" + normalized;
            if ((uri != null && uri.contains(needle)) || (path != null && path.contains(needle))) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesPageCriteria(ChatbotResultDto dto, Set<String> pageIds) {
        if (pageIds == null || pageIds.isEmpty()) {
            return true;
        }
        String pageId = dto.getPageId();
        String pageIdLower = pageId != null ? pageId.toLowerCase(Locale.ROOT) : null;
        if (pageIdLower != null && pageIds.contains(pageIdLower)) {
            return true;
        }
        String uri = dto.getSectionUri() != null ? dto.getSectionUri().toLowerCase(Locale.ROOT) : null;
        String path = dto.getSectionPath() != null ? dto.getSectionPath().toLowerCase(Locale.ROOT) : null;
        for (String candidate : pageIds) {
            String lower = candidate.toLowerCase(Locale.ROOT);
            if ((uri != null && (uri.contains("/" + lower + "/") || uri.endsWith("/" + lower)))
                    || (path != null && (path.contains("/" + lower + "/") || path.endsWith("/" + lower)))) {
                return true;
            }
        }
        return false;
    }

    private String mapCountryCode(String codeOrName) {
        if (!StringUtils.hasText(codeOrName)) {
            return null;
        }
        String trimmed = codeOrName.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        String mapped = COUNTRY_NAME_INDEX.get(lower);
        if (mapped != null) {
            return mapped;
        }
        String upper = trimmed.toUpperCase(Locale.ROOT);
        if (ISO_COUNTRY_CODES.contains(upper)) {
            return upper;
        }
        return null;
    }

    private boolean isPotentialPageIdToken(String token) {
        if (!StringUtils.hasText(token)) {
            return false;
        }
        String lower = token.toLowerCase(Locale.ROOT);
        if (STOP_WORDS.contains(lower)) {
            return false;
        }
        if (lower.length() < 3 || lower.length() > 40) {
            return false;
        }
        if (lower.contains("-") || lower.contains("'") || lower.contains("_")) {
            return false;
        }
        if (lower.endsWith("section") || lower.endsWith("sections")) {
            return false;
        }
        for (int i = 0; i < lower.length(); i++) {
            char c = lower.charAt(i);
            if (!Character.isLetterOrDigit(c)) {
                return false;
            }
        }
        return true;
    }

    private static class LocaleCriteria {
        private final Set<String> locales = new HashSet<>();
        private final Set<String> countries = new HashSet<>();
        private final Set<String> languages = new HashSet<>();
        private final Set<String> pageIds = new HashSet<>();

        boolean isEmpty() {
            return locales.isEmpty() && countries.isEmpty() && languages.isEmpty() && pageIds.isEmpty();
        }
    }

    private enum ValueType {
        LOCALE,
        COUNTRY,
        LANGUAGE,
        PAGE_ID
    }

    private Map<String, Object> buildLocaleContext(LocaleCriteria criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> filter = new LinkedHashMap<>();
        if (!criteria.locales.isEmpty()) {
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("locale", new ArrayList<>(criteria.locales));
            filter.put("envelope", envelope);
        }
        if (filter.isEmpty()) {
            return Collections.emptyMap();
        }
        return filter;
    }

    private Map<String, Object> mergeContextFilters(Map<String, Object> base, Map<String, Object> addition) {
        boolean baseEmpty = base == null || base.isEmpty();
        boolean additionEmpty = addition == null || addition.isEmpty();
        if (baseEmpty && additionEmpty) {
            return Collections.emptyMap();
        }
        Map<String, Object> merged = new LinkedHashMap<>();
        if (!baseEmpty) {
            base.forEach((k, v) -> merged.put(k, cloneContextValue(v)));
        }
        if (!additionEmpty) {
            addition.forEach((k, v) -> merged.merge(k, cloneContextValue(v), this::mergeContextValues));
        }
        return merged;
    }

    private Object cloneContextValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((k, v) -> copy.put(String.valueOf(k), cloneContextValue(v)));
            return copy;
        }
        if (value instanceof Collection<?> collection) {
            List<String> copy = new ArrayList<>();
            for (Object item : collection) {
                if (item != null) {
                    copy.add(item.toString());
                }
            }
            return copy;
        }
        return value;
    }

    private Object mergeContextValues(Object existing, Object addition) {
        if (existing instanceof Map<?, ?> existingMap && addition instanceof Map<?, ?> additionMap) {
            Map<String, Object> result = new LinkedHashMap<>();
            existingMap.forEach((k, v) -> result.put(String.valueOf(k), cloneContextValue(v)));
            additionMap.forEach((k, v) -> result.merge(String.valueOf(k), cloneContextValue(v), this::mergeContextValues));
            return result;
        }
        Collection<?> existingCollection = existing instanceof Collection<?> ? (Collection<?>) existing : null;
        Collection<?> additionCollection = addition instanceof Collection<?> ? (Collection<?>) addition : null;
        if (existingCollection != null || additionCollection != null) {
            LinkedHashSet<String> combined = new LinkedHashSet<>();
            if (existingCollection != null) {
                for (Object item : existingCollection) {
                    if (item != null) combined.add(item.toString());
                }
            } else if (existing != null) {
                combined.add(existing.toString());
            }
            if (additionCollection != null) {
                for (Object item : additionCollection) {
                    if (item != null) combined.add(item.toString());
                }
            } else if (addition != null) {
                combined.add(addition.toString());
            }
            return new ArrayList<>(combined);
        }
        if (existing == null) {
            return addition;
        }
        if (addition == null) {
            return existing;
        }
        if (existing.equals(addition)) {
            return existing;
        }
        LinkedHashSet<String> combined = new LinkedHashSet<>();
        combined.add(existing.toString());
        combined.add(addition.toString());
        return new ArrayList<>(combined);
    }

    private static Map<String, String> buildCountryNameIndex() {
        Map<String, String> index = new HashMap<>();
        for (Locale locale : Locale.getAvailableLocales()) {
            String country = locale.getCountry();
            if (!StringUtils.hasText(country)) {
                continue;
            }
            String code = country.toUpperCase(Locale.ROOT);
            String english = locale.getDisplayCountry(Locale.ENGLISH);
            if (StringUtils.hasText(english)) {
                index.putIfAbsent(english.toLowerCase(Locale.ROOT), code);
            }
            String localName = locale.getDisplayCountry(locale);
            if (StringUtils.hasText(localName)) {
                index.putIfAbsent(localName.toLowerCase(Locale.ROOT), code);
            }
        }
        for (String code : ISO_COUNTRY_CODES) {
            index.putIfAbsent(code.toLowerCase(Locale.ROOT), code);
        }
        // Common synonyms and regional naming variations
        index.put("usa", "US");
        index.put("u.s.", "US");
        index.put("u.s.a", "US");
        index.put("america", "US");
        index.put("united states", "US");
        index.put("united states of america", "US");
        index.put("uk", "GB");
        index.put("united kingdom", "GB");
        index.put("great britain", "GB");
        index.put("britain", "GB");
        index.put("south korea", "KR");
        index.put("north korea", "KP");
        index.put("korea", "KR");
        index.put("hong kong", "HK");
        index.put("macau", "MO");
        index.put("mainland china", "CN");
        index.put("people's republic of china", "CN");
        index.put("czech republic", "CZ");
        index.put("ivory coast", "CI");
        index.put("uae", "AE");
        index.put("united arab emirates", "AE");
        index.put("republic of korea", "KR");
        index.put("saudi arabia", "SA");
        index.put("kingdom of saudi arabia", "SA");
        return Collections.unmodifiableMap(index);
    }
}