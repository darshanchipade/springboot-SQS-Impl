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
import java.util.UUID;
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

        String message = request != null ? request.getMessage() : null;

        LocaleCriteria localeCriteria = extractLocaleCriteria(message);
        if (request != null && request.getContext() != null) {
            enrichCriteriaFromContext(localeCriteria, request.getContext());
        }

        boolean explicitRole = request != null && StringUtils.hasText(request.getOriginal_field_name());
        String roleHint = explicitRole ? request.getOriginal_field_name().trim().toLowerCase(Locale.ROOT) : null;
        boolean inferredRole = false;
        if (!explicitRole && userExplicitlyRequestedRole(message, key)) {
            String inferred = inferRoleHint(message, key);
            if (StringUtils.hasText(inferred)) {
                roleHint = inferred.trim().toLowerCase(Locale.ROOT);
                inferredRole = true;
            }
        }
        int vectorPreLimit = StringUtils.hasText(roleHint) ? Math.min(limit * 3, 100) : limit;

        try {
            String embeddingQuery = StringUtils.hasText(message) ? message : key;

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
            final Set<String> sectionKeyVariants = StringUtils.hasText(sectionKeyFinal)
                    ? new LinkedHashSet<>(buildSectionKeyVariants(sectionKeyFinal))
                    : Collections.emptySet();

            List<ChatbotResultDto> vectorDtos = new ArrayList<>();
            for (ContentChunkWithDistance result : results) {
                var section = result.getContentChunk().getConsolidatedEnrichedSection();
                if (!sectionKeyVariants.isEmpty() && !matchesSectionKey(section, sectionKeyVariants)) {
                    continue;
                }
                ChatbotResultDto dto = new ChatbotResultDto();
                dto.setSection(sectionKeyFinal);
                dto.setSectionPath(section.getSectionPath());
                dto.setSectionUri(section.getSectionUri());
                dto.setCleansedText(section.getCleansedText());
                dto.setSource("content_chunks");
                dto.setContentRole(section.getOriginalFieldName());
                dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
                dto.setMatchTerms(List.of(sectionKeyFinal));

                LocaleTriple localeInfo = resolveLocaleInfo(section, localeCriteria);
                dto.setLocale(localeInfo.locale());
                dto.setLanguage(localeInfo.language());
                dto.setCountry(localeInfo.country());

                dto.setTenant(extractTenant(section));
                dto.setPageId(extractPageId(section));
                vectorDtos.add(dto);
            }

            vectorDtos = applyCriteriaFilter(vectorDtos, localeCriteria);

            // Consolidated search: prioritise explicit section-key matches, fallback to metadata query
            List<ConsolidatedEnrichedSection> consolidatedMatches = new ArrayList<>();
            if (StringUtils.hasText(sectionKeyFinal)) {
                LinkedHashMap<UUID, ConsolidatedEnrichedSection> agg = new LinkedHashMap<>();
                addSectionRows(agg, consolidatedRepo.findBySectionKey(
                        sectionKeyFinal,
                        Math.max(limit * 4, 2000)));
                addSectionRows(agg, consolidatedRepo.findByContextSectionKey(
                        sectionKeyFinal,
                        Math.max(limit * 4, 2000)));

                for (String variant : buildSectionKeyVariants(sectionKeyFinal)) {
                    addSectionRows(agg, consolidatedRepo.findBySectionKey(
                            variant,
                            Math.max(limit * 4, 2000)));
                }

                if (!agg.isEmpty()) {
                    consolidatedMatches = new ArrayList<>(agg.values());
                }
            }

            if (consolidatedMatches.isEmpty()) {
                if (StringUtils.hasText(message)) {
                    consolidatedMatches = consolidatedRepo.findByMetadataQuery(message, Math.max(limit * 2, 50));
                } else if (StringUtils.hasText(sectionKeyFinal)) {
                    consolidatedMatches = consolidatedRepo.findBySectionKey(sectionKeyFinal, Math.max(limit * 2, 50));
                }
            }

            if (!sectionKeyVariants.isEmpty() && !consolidatedMatches.isEmpty()) {
                consolidatedMatches = consolidatedMatches.stream()
                        .filter(section -> matchesSectionKey(section, sectionKeyVariants))
                        .collect(Collectors.toList());
            }

            boolean applyRoleFilter = false;
            if (explicitRole && StringUtils.hasText(roleHint)) {
                applyRoleFilter = true;
            } else if (inferredRole && StringUtils.hasText(roleHint)) {
                if (roleExists(roleHint, vectorDtos, consolidatedMatches)) {
                    applyRoleFilter = true;
                } else {
                    roleHint = null;
                }
            }

            if (applyRoleFilter && StringUtils.hasText(roleHint)) {
                final String roleFilter = roleHint.toLowerCase(Locale.ROOT);
                vectorDtos = vectorDtos.stream()
                        .filter(d -> d.getContentRole() != null && d.getContentRole().toLowerCase(Locale.ROOT).contains(roleFilter))
                        .collect(Collectors.toList());
                consolidatedMatches = consolidatedMatches.stream()
                        .filter(s -> s.getOriginalFieldName() != null
                                && s.getOriginalFieldName().toLowerCase(Locale.ROOT).contains(roleFilter))
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

                        LocaleTriple localeInfo = resolveLocaleInfo(section, localeCriteria);
                        dto.setLocale(localeInfo.locale());
                        dto.setLanguage(localeInfo.language());
                        dto.setCountry(localeInfo.country());

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

            List<ChatbotResultDto> mergedList = finalizeResults(new ArrayList<>(merged.values()),
                    localeCriteria, request, sectionKeyFinal, roleHint, applyRoleFilter, limit);

            if (mergedList.isEmpty() && StringUtils.hasText(sectionKeyFinal)) {
                LinkedHashMap<UUID, ConsolidatedEnrichedSection> fallbackAgg = new LinkedHashMap<>();
                addSectionRows(fallbackAgg, consolidatedRepo.findBySectionKey(
                        sectionKeyFinal,
                        Math.max(limit * 4, 2000)));
                addSectionRows(fallbackAgg, consolidatedRepo.findByContextSectionKey(
                        sectionKeyFinal,
                        Math.max(limit * 4, 2000)));
                List<ConsolidatedEnrichedSection> fallbackRows = new ArrayList<>(fallbackAgg.values());
                if (fallbackRows.isEmpty()) {
                    fallbackRows = consolidatedRepo.findByMetadataQuery(sectionKeyFinal, Math.max(limit * 2, 50));
                }
                if (!sectionKeyVariants.isEmpty()) {
                    fallbackRows = fallbackRows.stream()
                            .filter(section -> matchesSectionKey(section, sectionKeyVariants))
                            .collect(Collectors.toList());
                }
                if (applyRoleFilter && StringUtils.hasText(roleHint)) {
                    final String roleFilter = roleHint.toLowerCase(Locale.ROOT);
                    fallbackRows = fallbackRows.stream()
                            .filter(s -> s.getOriginalFieldName() != null
                                    && s.getOriginalFieldName().toLowerCase(Locale.ROOT).contains(roleFilter))
                            .collect(Collectors.toList());
                }

                List<ChatbotResultDto> fallbackDtos = fallbackRows.stream()
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

                            LocaleTriple localeInfo = resolveLocaleInfo(section, localeCriteria);
                            dto.setLocale(localeInfo.locale());
                            dto.setLanguage(localeInfo.language());
                            dto.setCountry(localeInfo.country());

                            dto.setTenant(extractTenant(section));
                            dto.setPageId(extractPageId(section));
                            return dto;
                        })
                        .collect(Collectors.toList());

                fallbackDtos = applyCriteriaFilter(fallbackDtos, localeCriteria);
                mergedList = finalizeResults(fallbackDtos, localeCriteria, request, sectionKeyFinal, roleHint, applyRoleFilter, limit);
            }

            return mergedList;
        } catch (Exception e) {
            return List.of();
        }
    }

    private boolean matchesSectionKey(ConsolidatedEnrichedSection section, Set<String> variants) {
        if (section == null || variants == null || variants.isEmpty()) {
            return true;
        }
        String original = normalizeKey(section.getOriginalFieldName());
        String path = section.getSectionPath() != null ? section.getSectionPath().toLowerCase(Locale.ROOT) : null;
        String uri = section.getSectionUri() != null ? section.getSectionUri().toLowerCase(Locale.ROOT) : null;
        List<String> contextKeys = extractContextSectionKeyValues(section);

        for (String variant : variants) {
            if (!StringUtils.hasText(variant)) {
                continue;
            }
            String needle = variant.toLowerCase(Locale.ROOT);
            if (StringUtils.hasText(original) && (original.equals(needle) || original.contains(needle))) {
                return true;
            }
            if (path != null && path.contains(needle)) {
                return true;
            }
            if (uri != null && uri.contains(needle)) {
                return true;
            }
            for (String ctx : contextKeys) {
                if (ctx.contains(needle)) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<String> extractContextSectionKeyValues(ConsolidatedEnrichedSection section) {
        if (section == null || section.getContext() == null) {
            return List.of();
        }
        List<String> values = new ArrayList<>();
        collectContextStrings(section.getContext().get("sectionKey"), values);

        Object facets = section.getContext().get("facets");
        if (facets instanceof Map<?, ?> facetsMap) {
            collectContextStrings(facetsMap.get("sectionKey"), values);
        }

        Object envelope = section.getContext().get("envelope");
        if (envelope instanceof Map<?, ?> envelopeMap) {
            collectContextStrings(envelopeMap.get("sectionKey"), values);
            collectContextStrings(envelopeMap.get("usagePath"), values);
        }

        return values.stream()
                .filter(StringUtils::hasText)
                .map(str -> str.trim().toLowerCase(Locale.ROOT))
                .collect(Collectors.toList());
    }

    private void collectContextStrings(Object value, List<String> target) {
        if (value == null || target == null) {
            return;
        }
        if (value instanceof String str) {
            target.add(str);
        } else if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (item instanceof String strItem) {
                    target.add(strItem);
                }
            }
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

        String[] tokens = message.split("\\s+");

        for (String raw : tokens) {
            String token = sanitizeToken(raw);
            if (!StringUtils.hasText(token)) {
                continue;
            }
            long hyphenCount = token.chars().filter(ch -> ch == '-').count();
            if (hyphenCount >= 2) {
                return normalizeKey(token);
            }
        }

        for (String raw : tokens) {
            String candidate = maybeSectionKeyFromToken(raw);
            if (StringUtils.hasText(candidate)) {
                return normalizeKey(candidate);
            }
        }

        return null;
    }

    private String sanitizeToken(String token) {
        if (!StringUtils.hasText(token)) {
            return null;
        }
        String cleaned = token.trim()
                .replaceAll("^[^A-Za-z0-9_-]+", "")
                .replaceAll("[^A-Za-z0-9_-]+$", "")
                .toLowerCase(Locale.ROOT);
        return StringUtils.hasText(cleaned) ? cleaned : null;
    }

    private String maybeSectionKeyFromToken(String raw) {
        String token = sanitizeToken(raw);
        if (!StringUtils.hasText(token)) {
            return null;
        }
        if (!token.contains("-")) {
            return null;
        }
        if (LOCALE_PATTERN.matcher(token).matches()) {
            return null;
        }
        if (!token.endsWith("-section")) {
            token = token + "-section";
        }
        return token;
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) return null;
        return key.trim().toLowerCase();
    }

    private String inferRoleHint(String text, String sectionKey) {
        if (!StringUtils.hasText(text)) return null;
        String t = text.toLowerCase(Locale.ROOT).trim();

        String roleCandidate = null;
        if (StringUtils.hasText(sectionKey)) {
            String sk = Pattern.quote(sectionKey.toLowerCase(Locale.ROOT));
            Matcher mFor = Pattern.compile("(?i)\\bgive\\s+me\\s+(.+?)\\s+(?:for|of)\\s+.*" + sk + "\\b").matcher(t);
            if (mFor.find()) roleCandidate = mFor.group(1);
            if (roleCandidate == null) {
                Matcher mGeneric = Pattern.compile("(?i)\\b(.+?)\\s+(?:for|of)\\s+.*" + sk + "\\b").matcher(t);
                if (mGeneric.find()) roleCandidate = mGeneric.group(1);
            }
        }
        if (roleCandidate == null) {
            Matcher mFor = Pattern.compile("(?i)\\bgive\\s+me\\s+(.+?)\\s+(?:for|of)\\s+").matcher(t);
            if (mFor.find()) roleCandidate = mFor.group(1);
            if (roleCandidate == null) {
                Matcher mGeneric = Pattern.compile("(?i)\\b([a-z0-9_-]{3,})\\b\\s+(?:for|of)\\b").matcher(t);
                if (mGeneric.find()) roleCandidate = mGeneric.group(1);
            }
        }

        if (!StringUtils.hasText(roleCandidate)) return null;

        List<String> tokens = Arrays.stream(roleCandidate.split("\\s+"))
                .map(s -> s.replaceAll("[^a-z0-9_-]", ""))
                .filter(StringUtils::hasText)
                .collect(Collectors.toList());
        if (tokens.isEmpty()) return null;

        return tokens.get(tokens.size() - 1);
    }

    private boolean userExplicitlyRequestedRole(String text, String sectionKey) {
        if (!StringUtils.hasText(text)) return false;
        String t = text.toLowerCase(Locale.ROOT);
        if (StringUtils.hasText(sectionKey)) {
            String sk = Pattern.quote(sectionKey.toLowerCase(Locale.ROOT));
            if (Pattern.compile("(?i)\\b(?:give\\s+me\\s+)?([a-z0-9_-]+)\\s+(?:for|of)\\s+.*" + sk + "\\b")
                    .matcher(t).find()) return true;
        }
        return Pattern.compile("(?i)\\b(?:give\\s+me\\s+)?([a-z0-9_-]+)\\s+(?:for|of)\\b").matcher(t).find();
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
            if (mappedCountry != null) {
                criteria.countries.add(mappedCountry);
                continue;
            }

            if (ISO_LANGUAGE_CODES.contains(lower)) {
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
                if (!lower.isBlank()) {
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

    private LocaleTriple resolveLocaleInfo(ConsolidatedEnrichedSection section, LocaleCriteria criteria) {
        String rawLocale = extractLocale(section);
        String locale = normalizeLocale(rawLocale);
        if (!StringUtils.hasText(locale)) {
            locale = rawLocale;
        }

        String language = extractLanguage(section, locale);
        String country = extractCountry(section, locale);

        if (!StringUtils.hasText(language)) {
            String contextLanguage = getEnvelopeValue(section, "language");
            if (StringUtils.hasText(contextLanguage)) {
                language = contextLanguage.toLowerCase(Locale.ROOT);
            }
        }

        if (!StringUtils.hasText(country)) {
            String contextCountry = getEnvelopeValue(section, "country");
            if (StringUtils.hasText(contextCountry)) {
                String mapped = mapCountryCode(contextCountry);
                if (mapped != null) {
                    country = mapped;
                }
            }
        }

        if (!StringUtils.hasText(locale) && criteria != null && !criteria.locales.isEmpty()) {
            String candidate = criteria.locales.iterator().next();
            String normalized = normalizeLocale(candidate);
            locale = normalized != null ? normalized : candidate;
        }

        if (!StringUtils.hasText(language) && criteria != null && !criteria.languages.isEmpty()) {
            language = criteria.languages.iterator().next();
        }

        if (!StringUtils.hasText(country) && criteria != null && !criteria.countries.isEmpty()) {
            country = criteria.countries.iterator().next();
        }

        if (StringUtils.hasText(locale)) {
            String normalized = normalizeLocale(locale);
            if (StringUtils.hasText(normalized)) {
                locale = normalized;
                int idx = locale.indexOf('_');
                if (idx > 0) {
                    if (!StringUtils.hasText(language)) {
                        language = locale.substring(0, idx);
                    }
                    if (!StringUtils.hasText(country)) {
                        country = locale.substring(idx + 1);
                    }
                }
            }
        }

        if (StringUtils.hasText(language)) {
            language = language.toLowerCase(Locale.ROOT);
        }
        if (StringUtils.hasText(country)) {
            String normalizedCountry = mapCountryCode(country);
            if (StringUtils.hasText(normalizedCountry)) {
                country = normalizedCountry;
            }
            country = country.toUpperCase(Locale.ROOT);
        }

        return new LocaleTriple(locale, language, country);
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
        // fall back: use second segment if present (works for en_CA or path pieces)
        int idx = lower.indexOf('_');
        if (idx >= 0 && idx + 1 < lower.length()) {
            String fallback = lower.substring(idx + 1);
            mapped = COUNTRY_NAME_INDEX.get(fallback);
            if (mapped != null) {
                return mapped;
            }
            String fallbackUpper = fallback.toUpperCase(Locale.ROOT);
            if (ISO_COUNTRY_CODES.contains(fallbackUpper)) {
                return fallbackUpper;
            }
        }
        return null;
    }

    private record LocaleTriple(String locale, String language, String country) {}

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

    private void addSectionRows(LinkedHashMap<UUID, ConsolidatedEnrichedSection> agg,
                                List<ConsolidatedEnrichedSection> rows) {
        if (agg == null || rows == null) {
            return;
        }
        for (ConsolidatedEnrichedSection row : rows) {
            if (row != null && row.getId() != null) {
                agg.putIfAbsent(row.getId(), row);
            }
        }
    }

    private boolean roleExists(String roleHint,
                               List<ChatbotResultDto> vectorDtos,
                               List<ConsolidatedEnrichedSection> consolidatedMatches) {
        if (!StringUtils.hasText(roleHint)) {
            return false;
        }
        String needle = roleHint.toLowerCase(Locale.ROOT);
        if (vectorDtos != null) {
            for (ChatbotResultDto dto : vectorDtos) {
                if (dto.getContentRole() != null
                        && dto.getContentRole().toLowerCase(Locale.ROOT).contains(needle)) {
                    return true;
                }
            }
        }
        if (consolidatedMatches != null) {
            for (ConsolidatedEnrichedSection section : consolidatedMatches) {
                if (section != null && section.getOriginalFieldName() != null
                        && section.getOriginalFieldName().toLowerCase(Locale.ROOT).contains(needle)) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<ChatbotResultDto> finalizeResults(List<ChatbotResultDto> items,
                                                   LocaleCriteria localeCriteria,
                                                   ChatbotRequest request,
                                                   String sectionKey,
                                                   String roleHint,
                                                   boolean applyRoleFilter,
                                                   int limit) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }

        List<ChatbotResultDto> trimmed = items.size() > limit ? new ArrayList<>(items.subList(0, limit))
                : new ArrayList<>(items);

        String inferredLocale = localeCriteria.locales.isEmpty() ? null : localeCriteria.locales.iterator().next();
        String inferredCountry = localeCriteria.countries.isEmpty() ? null : localeCriteria.countries.iterator().next();
        String inferredLanguage = localeCriteria.languages.isEmpty() ? null : localeCriteria.languages.iterator().next();

        for (int i = 0; i < trimmed.size(); i++) {
            ChatbotResultDto item = trimmed.get(i);
            item.setCfId("cf" + (i + 1));

            var terms = new LinkedHashSet<String>();
            if (StringUtils.hasText(sectionKey)) {
                terms.add(sectionKey);
            }
            if (applyRoleFilter && StringUtils.hasText(roleHint)) {
                terms.add(roleHint);
            }
            if (request != null && request.getTags() != null) {
                terms.addAll(request.getTags());
            }
            if (request != null && request.getKeywords() != null) {
                terms.addAll(request.getKeywords());
            }
            if (!localeCriteria.pageIds.isEmpty()) {
                terms.addAll(localeCriteria.pageIds);
            }
            item.setMatchTerms(new ArrayList<>(terms));

            if (!StringUtils.hasText(item.getLocale()) && StringUtils.hasText(inferredLocale)) {
                item.setLocale(inferredLocale);
            }
            if (!StringUtils.hasText(item.getCountry()) && StringUtils.hasText(inferredCountry)) {
                item.setCountry(inferredCountry);
            }
            if (!StringUtils.hasText(item.getLanguage()) && StringUtils.hasText(inferredLanguage)) {
                item.setLanguage(inferredLanguage);
            }

            if (StringUtils.hasText(item.getLocale())) {
                String normalized = normalizeLocale(item.getLocale());
                if (StringUtils.hasText(normalized)) {
                    int idx = normalized.indexOf('_');
                    if (idx > 0) {
                        String langPart = normalized.substring(0, idx);
                        String countryPart = normalized.substring(idx + 1);
                        if (!StringUtils.hasText(item.getLanguage())) {
                            item.setLanguage(langPart);
                        }
                        if (!StringUtils.hasText(item.getCountry())) {
                            item.setCountry(countryPart);
                        }
                    }
                }
            }
        }

        return trimmed;
    }

    private List<String> buildSectionKeyVariants(String sectionKey) {
        if (!StringUtils.hasText(sectionKey)) {
            return List.of();
        }
        String normalized = sectionKey.toLowerCase(Locale.ROOT).trim();
        LinkedHashSet<String> variants = new LinkedHashSet<>();
        variants.add(normalized);

        String root = stripKnownSuffixes(normalized);
        addVariantsFromRoot(variants, root);

        String singularRoot = singularizeKeySegment(root);
        if (StringUtils.hasText(singularRoot) && !singularRoot.equals(root)) {
            addVariantsFromRoot(variants, singularRoot);
        }

        String normalizedSingular = singularizeKeySegment(normalized);
        if (StringUtils.hasText(normalizedSingular)) {
            variants.add(normalizedSingular);
        }

        return new ArrayList<>(variants);
    }

    private void addVariantsFromRoot(Set<String> variants, String root) {
        if (!StringUtils.hasText(root)) {
            return;
        }
        String base = root.toLowerCase(Locale.ROOT).trim();
        variants.add(base);
        variants.add(base + "-section");
        variants.add(base + "-items");
        variants.add(base + "-items-horizontal");
        variants.add(base + "-items-vertical");
        variants.add(base + "-items-wide");
        variants.add(base + "-items-desktop");
        variants.add(base + "-items-mobile");
    }

    private String stripKnownSuffixes(String key) {
        if (!StringUtils.hasText(key)) {
            return key;
        }
        String result = key.toLowerCase(Locale.ROOT).trim();
        String[] suffixes = {
                "-items-horizontal",
                "-items-vertical",
                "-items-wide",
                "-items-desktop",
                "-items-mobile",
                "-items",
                "-section"
        };
        boolean stripped;
        do {
            stripped = false;
            for (String suffix : suffixes) {
                if (result.endsWith(suffix)) {
                    result = result.substring(0, result.length() - suffix.length());
                    stripped = true;
                    break;
                }
            }
        } while (stripped && StringUtils.hasText(result));
        return result;
    }

    private String singularizeKeySegment(String key) {
        if (!StringUtils.hasText(key)) {
            return key;
        }
        String value = key.toLowerCase(Locale.ROOT).trim();
        int idx = value.lastIndexOf('-');
        String prefix = idx >= 0 ? value.substring(0, idx + 1) : "";
        String segment = idx >= 0 ? value.substring(idx + 1) : value;
        if (segment.length() <= 3) {
            return key;
        }
        if (segment.endsWith("ies") && segment.length() > 3) {
            segment = segment.substring(0, segment.length() - 3) + "y";
        } else if (segment.endsWith("ses") && segment.length() > 3) {
            segment = segment.substring(0, segment.length() - 2);
        } else if (segment.endsWith("s") && segment.length() > 3 && !segment.endsWith("ss")) {
            segment = segment.substring(0, segment.length() - 1);
        } else {
            return key;
        }
        String singular = prefix + segment;
        return StringUtils.hasText(singular) ? singular : key;
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