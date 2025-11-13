package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ChatbotService {
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;

    private static final Pattern SECTION_PATTERN = Pattern.compile("(?i)\\b([a-z0-9][a-z0-9\\s-]*section)\\b");
    private static final Pattern ROLE_PATTERN = Pattern.compile("(?i)\\bgive\\s+me\\s+([a-z0-9\\s-]+?)\\s+(?:for|of)\\b");
    private static final Pattern MESSAGE_LOCALE_PATTERN = Pattern.compile("(?i)\\b([a-z]{2})[_-]([a-z]{2})\\b");
    private static final Pattern NORMALIZE_LOCALE_PATTERN = Pattern.compile("(?i)^([a-z]{2})[_-]([a-z]{2})$");

    private static final Set<String> KNOWN_PAGE_IDS = Set.of("ipad", "iphone", "watch", "mac", "macbook", "airpods");
    private static final Pattern PAGE_ID_PATTERN = Pattern.compile("(?i)\\b(" + String.join("|", KNOWN_PAGE_IDS) + ")\\b");

    private static final Set<String> KNOWN_ROLE_KEYWORDS = Set.of("headline", "title", "copy", "body", "content", "description");

    private static final Map<String, String> LANGUAGE_ALIASES = Map.of(
            "english", "en",
            "spanish", "es",
            "french", "fr",
            "german", "de",
            "japanese", "ja",
            "korean", "ko",
            "chinese", "zh"
    );

    private static final List<String> COUNTRY_PHRASES = List.of(
            "united states of america",
            "united states",
            "united kingdom"
    );

    private static final Map<String, String> COUNTRY_TOKEN_TO_CODE;

    static {
        Map<String, String> tokens = new HashMap<>();
        tokens.put("us", "US");
        tokens.put("usa", "US");
        tokens.put("u.s", "US");
        tokens.put("u.s.a", "US");
        tokens.put("unitedstates", "US");
        tokens.put("unitedstatesofamerica", "US");
        tokens.put("america", "US");
        tokens.put("uk", "GB");
        tokens.put("unitedkingdom", "GB");
        COUNTRY_TOKEN_TO_CODE = Collections.unmodifiableMap(tokens);
    }

    public ChatbotService(VectorSearchService vectorSearchService,
                          ConsolidatedEnrichedSectionRepository consolidatedRepo) {
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
    }

    public List<ChatbotResultDto> query(ChatbotRequest request) {
        SearchCriteria criteria = buildCriteria(request);
        if (!StringUtils.hasText(criteria.sectionKey())) {
            return List.of();
        }

        int limit = determineLimit(request);

        Map<String, Object> derivedContext = buildDerivedContext(criteria);
        Map<String, Object> effectiveContext = mergeContext(
                request != null ? request.getContext() : null,
                derivedContext
        );

        List<ChatbotResultDto> vectorResults = fetchVectorResults(request, criteria, effectiveContext, limit);
        List<ChatbotResultDto> consolidatedResults = fetchConsolidatedResults(criteria, limit);

        List<ChatbotResultDto> combined = mergeResults(vectorResults, consolidatedResults, limit);
        assignCfIds(combined, criteria, request);

        return combined;
    }

    private int determineLimit(ChatbotRequest request) {
        if (request == null || request.getLimit() == null || request.getLimit() <= 0) {
            return 15;
        }
        return Math.min(request.getLimit(), 200);
    }

    private List<ChatbotResultDto> fetchVectorResults(ChatbotRequest request,
                                                      SearchCriteria criteria,
                                                      Map<String, Object> context,
                                                      int limit) {
        if (!StringUtils.hasText(criteria.embeddingQuery())) {
            return List.of();
        }
        try {
            List<ContentChunkWithDistance> rows = vectorSearchService.search(
                    criteria.embeddingQuery(),
                    null,
                    limit,
                    request != null ? request.getTags() : null,
                    request != null ? request.getKeywords() : null,
                    context.isEmpty() ? null : context,
                    null,
                    criteria.sectionKey()
            );

            return rows.stream()
                    .map(row -> {
                        ConsolidatedEnrichedSection section = row.getContentChunk().getConsolidatedEnrichedSection();
                        ChatbotResultDto dto = mapSection(section, "content_chunks");
                        if (dto != null && !StringUtils.hasText(dto.getContentRole())) {
                            dto.setContentRole(firstNonBlank(row.getContentChunk().getSourceField()));
                        }
                        return dto;
                    })
                    .filter(Objects::nonNull)
                    .filter(dto -> matchesCriteria(dto, criteria))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception ex) {
            return List.of();
        }
    }

    private List<ChatbotResultDto> fetchConsolidatedResults(SearchCriteria criteria, int limit) {
        try {
            LinkedHashMap<UUID, ConsolidatedEnrichedSection> collected = new LinkedHashMap<>();

            if (StringUtils.hasText(criteria.sectionKey())) {
                collectRows(collected, consolidatedRepo.findBySectionKey(criteria.sectionKey(), limit * 2));
                collectRows(collected, consolidatedRepo.findByContextSectionKey(criteria.sectionKey(), limit * 2));
            }

            List<ConsolidatedEnrichedSection> rows = new ArrayList<>(collected.values());
            if (rows.isEmpty() && StringUtils.hasText(criteria.message())) {
                rows = consolidatedRepo.findByMetadataQuery(criteria.message(), limit * 2);
            }

            return rows.stream()
                    .map(section -> mapSection(section, "consolidated_enriched_sections"))
                    .filter(Objects::nonNull)
                    .filter(dto -> matchesCriteria(dto, criteria))
                    .collect(Collectors.toCollection(ArrayList::new));
        } catch (Exception ex) {
            return List.of();
        }
    }

    private void collectRows(Map<UUID, ConsolidatedEnrichedSection> target,
                             List<ConsolidatedEnrichedSection> rows) {
        if (target == null || rows == null) {
            return;
        }
        for (ConsolidatedEnrichedSection row : rows) {
            if (row != null && row.getId() != null) {
                target.putIfAbsent(row.getId(), row);
            }
        }
    }

    private List<ChatbotResultDto> mergeResults(List<ChatbotResultDto> vector,
                                                List<ChatbotResultDto> consolidated,
                                                int limit) {
        LinkedHashMap<String, ChatbotResultDto> deduped = new LinkedHashMap<>();
        addResults(deduped, vector);
        addResults(deduped, consolidated);
        return deduped.values()
                .stream()
                .limit(limit)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private void addResults(LinkedHashMap<String, ChatbotResultDto> target, List<ChatbotResultDto> source) {
        if (target == null || source == null) {
            return;
        }
        for (ChatbotResultDto dto : source) {
            if (dto == null) {
                continue;
            }
            String key = (dto.getSectionPath() != null ? dto.getSectionPath() : "")
                    + "|" + (dto.getContentRole() != null ? dto.getContentRole() : "");
            target.putIfAbsent(key, dto);
        }
    }

    private void assignCfIds(List<ChatbotResultDto> results,
                             SearchCriteria criteria,
                             ChatbotRequest request) {
        if (results == null) {
            return;
        }
        for (int i = 0; i < results.size(); i++) {
            ChatbotResultDto dto = results.get(i);
            if (dto == null) {
                continue;
            }
            dto.setCfId("cf" + (i + 1));

            if (!StringUtils.hasText(dto.getSection())) {
                dto.setSection(criteria.sectionKey());
            }
            if (!StringUtils.hasText(dto.getLocale()) && StringUtils.hasText(criteria.locale())) {
                dto.setLocale(criteria.locale());
            }
            if (!StringUtils.hasText(dto.getCountry()) && StringUtils.hasText(criteria.country())) {
                dto.setCountry(criteria.country());
            }
            if (!StringUtils.hasText(dto.getLanguage()) && StringUtils.hasText(criteria.language())) {
                dto.setLanguage(criteria.language());
            }
            if (!StringUtils.hasText(dto.getPageId()) && StringUtils.hasText(criteria.pageId())) {
                dto.setPageId(criteria.pageId());
            }
            if (!StringUtils.hasText(dto.getContentRole()) && StringUtils.hasText(criteria.role())) {
                dto.setContentRole(criteria.role());
            }
            if (!StringUtils.hasText(dto.getTenant())) {
                dto.setTenant("applecom-cms");
            }

            dto.setMatchTerms(buildMatchTerms(dto, criteria, request));
        }
    }

    private List<String> buildMatchTerms(ChatbotResultDto dto,
                                         SearchCriteria criteria,
                                         ChatbotRequest request) {
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        addTerm(terms, criteria.sectionKey());
        addTerm(terms, dto.getContentRole());
        addTerm(terms, criteria.role());
        addTerm(terms, dto.getPageId());
        addTerm(terms, criteria.pageId());
        addTerm(terms, criteria.country());
        addTerm(terms, criteria.language());

        if (request != null && request.getTags() != null) {
            terms.addAll(request.getTags());
        }
        if (request != null && request.getKeywords() != null) {
            terms.addAll(request.getKeywords());
        }
        return new ArrayList<>(terms);
    }

    private void addTerm(Set<String> terms, String value) {
        if (terms == null || !StringUtils.hasText(value)) {
            return;
        }
        terms.add(value);
    }

    private boolean matchesCriteria(ChatbotResultDto dto, SearchCriteria criteria) {
        if (dto == null) {
            return false;
        }
        if (criteria == null) {
            return true;
        }
        if (StringUtils.hasText(criteria.role())
                && !matchesRole(dto.getContentRole(), criteria.role())) {
            return false;
        }
        if (StringUtils.hasText(criteria.pageId())
                && StringUtils.hasText(dto.getPageId())
                && !dto.getPageId().equalsIgnoreCase(criteria.pageId())) {
            return false;
        }
        if (StringUtils.hasText(criteria.country())
                && StringUtils.hasText(dto.getCountry())
                && !dto.getCountry().equalsIgnoreCase(criteria.country())) {
            return false;
        }
        if (StringUtils.hasText(criteria.locale())
                && StringUtils.hasText(dto.getLocale())
                && !dto.getLocale().equalsIgnoreCase(criteria.locale())) {
            return false;
        }
        return true;
    }

    private ChatbotResultDto mapSection(ConsolidatedEnrichedSection section, String source) {
        if (section == null) {
            return null;
        }
        ChatbotResultDto dto = new ChatbotResultDto();
        dto.setSectionPath(section.getSectionPath());
        dto.setSectionUri(section.getSectionUri());
        dto.setCleansedText(section.getCleansedText());
        dto.setContentRole(resolveRole(section));
        dto.setSource(source);
        dto.setLastModified(formatTimestamp(section.getSavedAt()));
        dto.setTenant(resolveTenant(section));
        dto.setPageId(resolvePageId(section));

        String locale = resolveLocale(section);
        dto.setLocale(locale);
        dto.setLanguage(resolveLanguage(section, locale));
        dto.setCountry(resolveCountry(section, locale));

        return dto;
    }

    private String formatTimestamp(OffsetDateTime timestamp) {
        return timestamp != null ? timestamp.toString() : null;
    }

    private String resolveTenant(ConsolidatedEnrichedSection section) {
        String fromEnvelope = asStringFromContext(section, "envelope", "tenant");
        if (StringUtils.hasText(fromEnvelope)) {
            return fromEnvelope;
        }
        String direct = firstString(section != null && section.getContext() != null ? section.getContext().get("tenant") : null);
        if (StringUtils.hasText(direct)) {
            return direct;
        }
        String fromUri = extractTenantFromPath(section != null ? section.getSectionUri() : null);
        if (!StringUtils.hasText(fromUri)) {
            fromUri = extractTenantFromPath(section != null ? section.getSectionPath() : null);
        }
        return StringUtils.hasText(fromUri) ? fromUri : "applecom-cms";
    }

    private String resolvePageId(ConsolidatedEnrichedSection section) {
        if (section == null) {
            return null;
        }
        String fromContext = normalizePageId(firstString(section.getContext() != null ? section.getContext().get("pageId") : null));
        if (StringUtils.hasText(fromContext)) {
            return fromContext;
        }
        String fromEnvelope = normalizePageId(asStringFromContext(section, "envelope", "pageId"));
        if (StringUtils.hasText(fromEnvelope)) {
            return fromEnvelope;
        }
        Map<String, Object> facets = asMap(section.getContext() != null ? section.getContext().get("facets") : null);
        if (facets != null) {
            String fromFacets = normalizePageId(firstString(facets.get("pageId")));
            if (StringUtils.hasText(fromFacets)) {
                return fromFacets;
            }
        }
        String fromPath = normalizePageId(extractPageIdFromPath(section.getSectionUri()));
        if (!StringUtils.hasText(fromPath)) {
            fromPath = normalizePageId(extractPageIdFromPath(section.getSectionPath()));
        }
        return fromPath;
    }

    private String resolveLocale(ConsolidatedEnrichedSection section) {
        if (section == null) {
            return null;
        }
        String fromEnvelope = normalizeLocale(asStringFromContext(section, "envelope", "locale"));
        if (StringUtils.hasText(fromEnvelope)) {
            return fromEnvelope;
        }
        String direct = normalizeLocale(firstString(section.getContext() != null ? section.getContext().get("locale") : null));
        if (StringUtils.hasText(direct)) {
            return direct;
        }
        Map<String, Object> facets = asMap(section.getContext() != null ? section.getContext().get("facets") : null);
        if (facets != null) {
            String facetLocale = normalizeLocale(firstString(facets.get("locale")));
            if (StringUtils.hasText(facetLocale)) {
                return facetLocale;
            }
        }
        String fromUri = normalizeLocale(extractLocaleFromPath(section.getSectionUri()));
        if (!StringUtils.hasText(fromUri)) {
            fromUri = normalizeLocale(extractLocaleFromPath(section.getSectionPath()));
        }
        return fromUri;
    }

    private String resolveLanguage(ConsolidatedEnrichedSection section, String locale) {
        String direct = normalizeLanguage(firstString(section != null && section.getContext() != null
                ? section.getContext().get("language") : null));
        if (StringUtils.hasText(direct)) {
            return direct;
        }
        String fromEnvelope = normalizeLanguage(asStringFromContext(section, "envelope", "language"));
        if (StringUtils.hasText(fromEnvelope)) {
            return fromEnvelope;
        }
        if (StringUtils.hasText(locale) && locale.contains("_")) {
            return locale.substring(0, 2).toLowerCase(Locale.ROOT);
        }
        return null;
    }

    private String resolveRole(ConsolidatedEnrichedSection section) {
        if (section == null) {
            return null;
        }
        if (StringUtils.hasText(section.getOriginalFieldName())) {
            return section.getOriginalFieldName();
        }
        Map<String, Object> context = section.getContext();
        String role = firstString(context != null ? context.get("sectionName") : null);
        if (!StringUtils.hasText(role)) {
            role = firstString(context != null ? context.get("elementName") : null);
        }
        Map<String, Object> envelope = asMap(context != null ? context.get("envelope") : null);
        if (!StringUtils.hasText(role) && envelope != null) {
            role = firstString(envelope.get("sectionName"));
        }
        Map<String, Object> facets = asMap(context != null ? context.get("facets") : null);
        if (!StringUtils.hasText(role) && facets != null) {
            role = firstString(facets.get("sectionName"));
        }
        return role;
    }

    private boolean matchesRole(String value, String desired) {
        if (!StringUtils.hasText(desired)) {
            return true;
        }
        String normalizedDesired = desired.trim().toLowerCase(Locale.ROOT);
        if (!StringUtils.hasText(value)) {
            return false;
        }
        String normalizedValue = value.trim().toLowerCase(Locale.ROOT);
        return normalizedValue.contains(normalizedDesired);
    }

    private String resolveCountry(ConsolidatedEnrichedSection section, String locale) {
        String fromEnvelope = mapCountryAlias(asStringFromContext(section, "envelope", "country"));
        if (StringUtils.hasText(fromEnvelope)) {
            return fromEnvelope;
        }
        String direct = mapCountryAlias(firstString(section != null && section.getContext() != null
                ? section.getContext().get("country") : null));
        if (StringUtils.hasText(direct)) {
            return direct;
        }
        Map<String, Object> facets = asMap(section != null && section.getContext() != null ? section.getContext().get("facets") : null);
        if (facets != null) {
            String facetCountry = mapCountryAlias(firstString(facets.get("country")));
            if (StringUtils.hasText(facetCountry)) {
                return facetCountry;
            }
        }
        if (StringUtils.hasText(locale) && locale.contains("_")) {
            return locale.substring(3).toUpperCase(Locale.ROOT);
        }
        return null;
    }

    private SearchCriteria buildCriteria(ChatbotRequest request) {
        String message = request != null ? request.getMessage() : null;

        SearchCriteriaBuilder builder = new SearchCriteriaBuilder()
                .message(message);

        String sectionKey = slugify(request != null ? request.getSectionKey() : null);
        String inferredSection = extractSectionKey(message);
        builder.sectionKey(firstNonBlank(sectionKey, inferredSection));

        String explicitRole = normalizeRole(request != null ? request.getOriginal_field_name() : null);
        String inferredRole = extractRole(message);
        builder.role(firstNonBlank(explicitRole, inferredRole));

        LocaleParts localeHints = parseLocaleHints(message);
        builder.locale(localeHints.locale());
        builder.language(localeHints.language());
        builder.country(localeHints.country());

        builder.pageId(extractPageId(message));

        if (request != null && request.getContext() != null) {
            Map<String, Object> context = request.getContext();
            builder.sectionKey(firstNonBlank(builder.sectionKey(), slugify(firstString(context.get("sectionKey")))));
            builder.locale(firstNonBlank(builder.locale(), normalizeLocale(firstString(context.get("locale")))));
            builder.country(firstNonBlank(builder.country(), mapCountryAlias(firstString(context.get("country")))));
            builder.language(firstNonBlank(builder.language(), normalizeLanguage(firstString(context.get("language")))));
            builder.pageId(firstNonBlank(builder.pageId(), normalizePageId(firstString(context.get("pageId")))));

            Map<String, Object> envelope = asMap(context.get("envelope"));
            if (envelope != null) {
                builder.sectionKey(firstNonBlank(builder.sectionKey(), slugify(firstString(envelope.get("sectionKey")))));
                builder.locale(firstNonBlank(builder.locale(), normalizeLocale(firstString(envelope.get("locale")))));
                builder.country(firstNonBlank(builder.country(), mapCountryAlias(firstString(envelope.get("country")))));
                builder.language(firstNonBlank(builder.language(), normalizeLanguage(firstString(envelope.get("language")))));
                builder.pageId(firstNonBlank(builder.pageId(), normalizePageId(firstString(envelope.get("pageId")))));
            }

            Map<String, Object> facets = asMap(context.get("facets"));
            if (facets != null) {
                builder.sectionKey(firstNonBlank(builder.sectionKey(), slugify(firstString(facets.get("sectionKey")))));
                builder.locale(firstNonBlank(builder.locale(), normalizeLocale(firstString(facets.get("locale")))));
                builder.country(firstNonBlank(builder.country(), mapCountryAlias(firstString(facets.get("country")))));
                builder.pageId(firstNonBlank(builder.pageId(), normalizePageId(firstString(facets.get("pageId")))));
            }
        }

        if (!StringUtils.hasText(builder.language()) && StringUtils.hasText(builder.locale())) {
            builder.language(builder.locale().substring(0, 2).toLowerCase(Locale.ROOT));
        }
        if (!StringUtils.hasText(builder.country()) && StringUtils.hasText(builder.locale())) {
            builder.country(builder.locale().substring(3).toUpperCase(Locale.ROOT));
        }

        return builder.build();
    }

    private Map<String, Object> buildDerivedContext(SearchCriteria criteria) {
        if (criteria == null) {
            return Collections.emptyMap();
        }
        Map<String, Object> context = new LinkedHashMap<>();
        Map<String, Object> envelope = new LinkedHashMap<>();
        if (StringUtils.hasText(criteria.locale())) {
            envelope.put("locale", criteria.locale());
        }
        if (StringUtils.hasText(criteria.country())) {
            envelope.put("country", criteria.country());
        }
        if (StringUtils.hasText(criteria.language())) {
            envelope.put("language", criteria.language());
        }
        if (!envelope.isEmpty()) {
            context.put("envelope", envelope);
        }
        if (StringUtils.hasText(criteria.pageId())) {
            context.put("pageId", criteria.pageId());
        }
        return context.isEmpty() ? Collections.emptyMap() : context;
    }

    private Map<String, Object> mergeContext(Map<String, Object> base,
                                             Map<String, Object> addition) {
        if ((base == null || base.isEmpty()) && (addition == null || addition.isEmpty())) {
            return Collections.emptyMap();
        }
        Map<String, Object> merged = new LinkedHashMap<>();
        if (base != null) {
            base.forEach((key, value) -> merged.put(key, copyValue(value)));
        }
        if (addition != null) {
            addition.forEach((key, value) ->
                    merged.merge(key, copyValue(value), this::mergeContextValue));
        }
        return merged;
    }

    private Object mergeContextValue(Object existing, Object addition) {
        if (existing instanceof Map<?, ?> existingMap && addition instanceof Map<?, ?> additionMap) {
            Map<String, Object> result = new LinkedHashMap<>();
            existingMap.forEach((key, value) -> result.put(String.valueOf(key), copyValue(value)));
            additionMap.forEach((key, value) -> result.put(String.valueOf(key), copyValue(value)));
            return result;
        }
        if (existing instanceof Collection<?> || addition instanceof Collection<?>) {
            LinkedHashSet<Object> combined = new LinkedHashSet<>();
            addToSet(combined, existing);
            addToSet(combined, addition);
            return new ArrayList<>(combined);
        }
        return addition;
    }

    private void addToSet(Set<Object> target, Object value) {
        if (value instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (item != null) {
                    target.add(item);
                }
            }
        } else if (value != null) {
            target.add(value);
        }
    }

    private Object copyValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, val) -> copy.put(String.valueOf(key), copyValue(val)));
            return copy;
        }
        if (value instanceof Collection<?> collection) {
            List<Object> copy = new ArrayList<>();
            for (Object item : collection) {
                copy.add(copyValue(item));
            }
            return copy;
        }
        return value;
    }

    private String extractSectionKey(String message) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        Matcher matcher = SECTION_PATTERN.matcher(message);
        if (matcher.find()) {
            return slugify(matcher.group(1));
        }
        return null;
    }

    private String extractRole(String message) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        Matcher matcher = ROLE_PATTERN.matcher(message);
        if (matcher.find()) {
            return normalizeRole(matcher.group(1));
        }
        String lower = message.toLowerCase(Locale.ROOT);
        for (String keyword : KNOWN_ROLE_KEYWORDS) {
            if (lower.contains(keyword)) {
                return keyword;
            }
        }
        return null;
    }

    private String extractPageId(String message) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        Matcher matcher = PAGE_ID_PATTERN.matcher(message);
        if (matcher.find()) {
            return normalizePageId(matcher.group(1));
        }
        return null;
    }

    private LocaleParts parseLocaleHints(String message) {
        if (!StringUtils.hasText(message)) {
            return LocaleParts.EMPTY;
        }
        Matcher localeMatcher = MESSAGE_LOCALE_PATTERN.matcher(message);
        if (localeMatcher.find()) {
            String language = localeMatcher.group(1).toLowerCase(Locale.ROOT);
            String country = localeMatcher.group(2).toUpperCase(Locale.ROOT);
            return new LocaleParts(language + "_" + country, language, country);
        }
        String lower = message.toLowerCase(Locale.ROOT);
        for (String phrase : COUNTRY_PHRASES) {
            if (lower.contains(phrase)) {
                String code = COUNTRY_TOKEN_TO_CODE.get(phrase.replaceAll("[^a-z]", ""));
                if (code != null) {
                    return new LocaleParts(null, null, code);
                }
            }
        }
        String[] tokens = lower.split("[^a-z0-9]+");
        for (String token : tokens) {
            if (!StringUtils.hasText(token)) {
                continue;
            }
            String sanitized = token.replaceAll("[^a-z]", "");
            String country = COUNTRY_TOKEN_TO_CODE.get(sanitized);
            if (country != null) {
                return new LocaleParts(null, null, country);
            }
            String language = LANGUAGE_ALIASES.get(sanitized);
            if (language != null) {
                return new LocaleParts(null, language, null);
            }
        }
        return LocaleParts.EMPTY;
    }

    private String slugify(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String slug = value.trim().toLowerCase(Locale.ROOT)
                .replaceAll("[^a-z0-9]+", "-")
                .replaceAll("-{2,}", "-");
        if (slug.startsWith("-")) {
            slug = slug.substring(1);
        }
        if (slug.endsWith("-")) {
            slug = slug.substring(0, slug.length() - 1);
        }
        return StringUtils.hasText(slug) ? slug : null;
    }

    private String normalizeRole(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizePageId(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private String normalizeLocale(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String trimmed = value.trim().replace('-', '_');
        Matcher matcher = NORMALIZE_LOCALE_PATTERN.matcher(trimmed);
        if (matcher.matches()) {
            return matcher.group(1).toLowerCase(Locale.ROOT) + "_" + matcher.group(2).toUpperCase(Locale.ROOT);
        }
        return null;
    }

    private String normalizeLanguage(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String trimmed = value.trim().toLowerCase(Locale.ROOT);
        if (trimmed.length() == 2) {
            return trimmed;
        }
        return LANGUAGE_ALIASES.get(trimmed);
    }

    private String mapCountryAlias(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String trimmed = value.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        String sanitized = lower.replaceAll("[^a-z]", "");
        String mapped = COUNTRY_TOKEN_TO_CODE.get(sanitized);
        if (mapped != null) {
            return mapped;
        }
        if (sanitized.length() == 2) {
            return sanitized.toUpperCase(Locale.ROOT);
        }
        return null;
    }

    private String extractTenantFromPath(String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        Matcher matcher = Pattern.compile("/content/dam/([^/]+)/").matcher(path);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String extractPageIdFromPath(String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        Matcher matcher = Pattern.compile("/[a-z]{2}_[A-Z]{2}/([^/]+)/").matcher(path);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String extractLocaleFromPath(String path) {
        if (!StringUtils.hasText(path)) {
            return null;
        }
        Matcher matcher = Pattern.compile("/([a-z]{2}_[A-Z]{2})/").matcher(path);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }

    private String firstNonBlank(String... values) {
        if (values == null) {
            return null;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                return value;
            }
        }
        return null;
    }

    private String asStringFromContext(ConsolidatedEnrichedSection section, String topLevelKey, String nestedKey) {
        if (section == null || section.getContext() == null) {
            return null;
        }
        Map<String, Object> parent = asMap(section.getContext().get(topLevelKey));
        if (parent == null) {
            return null;
        }
        return firstString(parent.get(nestedKey));
    }

    private Map<String, Object> asMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            map.forEach((key, val) -> copy.put(String.valueOf(key), val));
            return copy;
        }
        return null;
    }

    private String firstString(Object value) {
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
        return null;
    }

    private record LocaleParts(String locale, String language, String country) {
        private static final LocaleParts EMPTY = new LocaleParts(null, null, null);
    }

    private record SearchCriteria(String sectionKey,
                                  String role,
                                  String locale,
                                  String language,
                                  String country,
                                  String pageId,
                                  String message) {
        String embeddingQuery() {
            return StringUtils.hasText(message) ? message : sectionKey;
        }
    }

    private static final class SearchCriteriaBuilder {
        private String sectionKey;
        private String role;
        private String locale;
        private String language;
        private String country;
        private String pageId;
        private String message;

        SearchCriteriaBuilder sectionKey(String sectionKey) {
            if (StringUtils.hasText(sectionKey)) {
                this.sectionKey = sectionKey;
            }
            return this;
        }

        SearchCriteriaBuilder role(String role) {
            if (StringUtils.hasText(role)) {
                this.role = role;
            }
            return this;
        }

        SearchCriteriaBuilder locale(String locale) {
            if (StringUtils.hasText(locale)) {
                this.locale = locale;
            }
            return this;
        }

        SearchCriteriaBuilder language(String language) {
            if (StringUtils.hasText(language)) {
                this.language = language;
            }
            return this;
        }

        SearchCriteriaBuilder country(String country) {
            if (StringUtils.hasText(country)) {
                this.country = country;
            }
            return this;
        }

        SearchCriteriaBuilder pageId(String pageId) {
            if (StringUtils.hasText(pageId)) {
                this.pageId = pageId;
            }
            return this;
        }

        SearchCriteriaBuilder message(String message) {
            this.message = message;
            return this;
        }

        String sectionKey() {
            return sectionKey;
        }

        String locale() {
            return locale;
        }

        String language() {
            return language;
        }

        String country() {
            return country;
        }

        String pageId() {
            return pageId;
        }

        SearchCriteria build() {
            return new SearchCriteria(sectionKey, role, locale, language, country, pageId, message);
        }
    }
}
