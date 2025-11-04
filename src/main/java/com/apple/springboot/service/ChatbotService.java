package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
    private static final Set<String> SUPPORTED_LANGUAGE_CODES = Set.of(
            "en", "fr", "de", "es", "it", "ja", "ko", "pt", "zh", "nl"
    );
    private static final Set<String> SUPPORTED_COUNTRY_CODES = Set.of(
            "US", "CA", "GB", "UK", "FR", "DE", "ES", "IT", "JP", "CN", "AU", "NZ", "IN", "MX", "BR"
    );
    private static final Map<String, String> COUNTRY_NAME_TO_CODE = Map.ofEntries(
            Map.entry("united states", "US"),
            Map.entry("usa", "US"),
            Map.entry("america", "US"),
            Map.entry("canada", "CA"),
            Map.entry("australia", "AU"),
            Map.entry("united kingdom", "GB"),
            Map.entry("great britain", "GB"),
            Map.entry("britain", "GB"),
            Map.entry("uk", "GB")
    );

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

            // Vector: do NOT hard-filter by original_field_name to allow partial queries
            List<ContentChunkWithDistance> results = vectorSearchService.search(
                    embeddingQuery,
                    null, // avoid strict equality in SQL for partial role queries
                    vectorPreLimit,
                    request != null ? request.getTags() : null,
                    request != null ? request.getKeywords() : null,
                    request != null ? request.getContext() : null,
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
                          // enrich with page, tenant, locale + derived language/country
                          String locale = extractLocale(section);
                          dto.setLocale(locale);
                          dto.setCountry(extractCountry(section, locale));
                          dto.setLanguage(extractLanguage(section, locale));
                        dto.setTenant(extractTenant(section));
                        dto.setPageId(extractPageId(section));
                        return dto;
                    })
                    .collect(Collectors.toList());

            vectorDtos = applyLocaleFilter(vectorDtos, localeCriteria);

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
                          dto.setLocale(locale);
                          dto.setCountry(extractCountry(section, locale));
                          dto.setLanguage(extractLanguage(section, locale));
                        dto.setTenant(extractTenant(section));
                        dto.setPageId(extractPageId(section));
                        return dto;
                    })
                    .collect(Collectors.toList());

            consolidatedDtos = applyLocaleFilter(consolidatedDtos, localeCriteria);

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
        String pid = extractPageIdFromPath(s.getSectionUri());
        if (pid == null) pid = extractPageIdFromPath(s.getSectionPath());
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

    private String normalizeLocale(String raw) {
        if (!StringUtils.hasText(raw)) {
            return null;
        }
        String trimmed = raw.trim().replace('-', '_');
        String[] parts = trimmed.split("_");
        if (parts.length == 2) {
            return parts[0].toLowerCase(Locale.ROOT) + "_" + parts[1].toUpperCase(Locale.ROOT);
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private LocaleCriteria extractLocaleCriteria(String message) {
        LocaleCriteria criteria = new LocaleCriteria();
        if (!StringUtils.hasText(message)) {
            return criteria;
        }

        Matcher matcher = LOCALE_PATTERN.matcher(message);
        while (matcher.find()) {
            String language = matcher.group(1).toLowerCase(Locale.ROOT);
            String country = matcher.group(2).toUpperCase(Locale.ROOT);
            String locale = language + "_" + country;
            criteria.locales.add(locale);
            criteria.languages.add(language);
            criteria.countries.add(mapCountryCode(country));
        }

        String messageLower = message.toLowerCase(Locale.ROOT);
        COUNTRY_NAME_TO_CODE.forEach((name, code) -> {
            if (messageLower.contains(name)) {
                criteria.countries.add(code);
            }
        });

        String[] tokens = message.split("[^A-Za-z0-9_]+");
        for (String token : tokens) {
            if (!StringUtils.hasText(token)) continue;
            String trimmed = token.trim();
            String lower = trimmed.toLowerCase(Locale.ROOT);
            String upper = trimmed.toUpperCase(Locale.ROOT);

            if (trimmed.length() == 2 && trimmed.equals(trimmed.toUpperCase(Locale.ROOT)) && SUPPORTED_COUNTRY_CODES.contains(mapCountryCode(upper))) {
                criteria.countries.add(mapCountryCode(upper));
            } else if (COUNTRY_NAME_TO_CODE.containsKey(lower)) {
                criteria.countries.add(COUNTRY_NAME_TO_CODE.get(lower));
            }

            if (SUPPORTED_LANGUAGE_CODES.contains(lower)) {
                criteria.languages.add(lower);
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

        Object envelope = context.get("envelope");
        if (envelope instanceof Map<?, ?> envMap) {
            Map<?, ?> env = envMap;
            addContextValue(criteria, env.get("locale"), ValueType.LOCALE);
            addContextValue(criteria, env.get("country"), ValueType.COUNTRY);
            addContextValue(criteria, env.get("language"), ValueType.LANGUAGE);
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
                        criteria.countries.add(mapCountryCode(normalized.substring(idx + 1)));
                    }
                }
            }
            case COUNTRY -> {
                String mapped = mapCountryCode(value.toUpperCase(Locale.ROOT));
                if (SUPPORTED_COUNTRY_CODES.contains(mapped)) {
                    criteria.countries.add(mapped);
                }
            }
            case LANGUAGE -> {
                String lower = value.toLowerCase(Locale.ROOT);
                if (SUPPORTED_LANGUAGE_CODES.contains(lower)) {
                    criteria.languages.add(lower);
                }
            }
        }
    }

    private List<ChatbotResultDto> applyLocaleFilter(List<ChatbotResultDto> dtos, LocaleCriteria criteria) {
        if (criteria == null || criteria.isEmpty()) {
            return dtos;
        }
        return dtos.stream()
                .filter(dto -> matchesLocaleCriteria(dto, criteria))
                .collect(Collectors.toList());
    }

    private boolean matchesLocaleCriteria(ChatbotResultDto dto, LocaleCriteria criteria) {
        String locale = normalizeLocale(dto.getLocale());
        String country = dto.getCountry() != null ? mapCountryCode(dto.getCountry().toUpperCase(Locale.ROOT)) : null;
        String language = dto.getLanguage() != null ? dto.getLanguage().toLowerCase(Locale.ROOT) : null;

        if (!criteria.locales.isEmpty()) {
            if (locale == null || !criteria.locales.contains(locale)) {
                return false;
            }
        }
        if (!criteria.countries.isEmpty()) {
            if (country == null || !criteria.countries.contains(country)) {
                return false;
            }
        }
        if (!criteria.languages.isEmpty()) {
            if (language == null || !criteria.languages.contains(language)) {
                return false;
            }
        }
        return true;
    }

    private String mapCountryCode(String code) {
        if (!StringUtils.hasText(code)) {
            return code;
        }
        String upper = code.toUpperCase(Locale.ROOT);
        if ("UK".equals(upper)) {
            return "GB";
        }
        return upper;
    }

    private static class LocaleCriteria {
        private final Set<String> locales = new HashSet<>();
        private final Set<String> countries = new HashSet<>();
        private final Set<String> languages = new HashSet<>();

        boolean isEmpty() {
            return locales.isEmpty() && countries.isEmpty() && languages.isEmpty();
        }
    }

    private enum ValueType {
        LOCALE,
        COUNTRY,
        LANGUAGE
    }
}