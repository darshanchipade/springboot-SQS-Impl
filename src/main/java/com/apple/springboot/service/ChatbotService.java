package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.QueryInterpretation;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ChatbotService {
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final QueryInterpretationService queryInterpretationService;

    private static final Pattern SECTION_PATTERN = Pattern.compile("(?i)\\b([a-z0-9][a-z0-9\\s-]*section)\\b");
    private static final Pattern ROLE_PATTERN = Pattern.compile("(?i)\\bgive\\s+me\\s+([a-z0-9\\s-]+?)\\s+(?:for|of)\\b");
    private static final Pattern ROLE_FOR_SECTION_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9_-]{3,})\\s+(?:for|of|in|on)\\s+([a-z0-9][a-z0-9\\s-]*section)\\b");
    private static final Pattern ROLE_BEFORE_SECTION_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9_-]{3,})\\s+([a-z0-9][a-z0-9\\s-]*section)\\b");
    private static final Pattern MESSAGE_LOCALE_PATTERN = Pattern.compile("(?i)\\b([a-z]{2})[_-]([a-z]{2})\\b");
    private static final Pattern NORMALIZE_LOCALE_PATTERN = Pattern.compile("(?i)^([a-z]{2})[_-]([a-z]{2})$");
    private static final Pattern TOKEN_PATTERN = Pattern.compile("[A-Za-z0-9_-]+");

    private static final Set<String> DEFAULT_ROLE_KEYWORDS = Set.of("headline", "title", "copy", "body", "content", "description");
    private static final Set<String> ROLE_STOP_WORDS = Set.of(
            "for",
            "section",
            "the",
            "and",
            "with",
            "of",
            "in",
            "on"
    );
    private static final Set<String> PAGE_STOP_WORDS = Set.of(
            "for",
            "section",
            "sections",
            "copy",
            "content",
            "headline",
            "with",
            "and",
            "the"
    );
    private static final Set<String> PREPOSITION_WORDS = Set.of("for", "of", "in", "on", "to", "at", "with");

    private final Map<String, String> languageIndex;
    private final Set<String> isoLanguageCodes;
    private final Map<String, String> countryIndex;
    private final Map<String, String> iso3ToIso2;
    private final Set<String> isoCountryCodes;

    public ChatbotService(VectorSearchService vectorSearchService,
                          ConsolidatedEnrichedSectionRepository consolidatedRepo,
                          QueryInterpretationService queryInterpretationService) {
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
        this.queryInterpretationService = queryInterpretationService;
        this.languageIndex = buildLanguageIndex();
        this.isoLanguageCodes = buildIsoLanguageCodes();
        this.isoCountryCodes = buildIsoCountryCodes();
        this.iso3ToIso2 = buildIso3ToIso2Index();
        this.countryIndex = buildCountryIndex();
    }

    public List<ChatbotResultDto> query(ChatbotRequest request) {
        String userMessage = request != null ? request.getMessage() : null;
        Map<String, Object> rawRequestContext = request != null ? request.getContext() : null;
        Map<String, Object> requestContext = sanitizeContext(rawRequestContext);

        QueryInterpretation interpretation = queryInterpretationService
                .interpret(userMessage, requestContext)
                .orElse(null);

        SearchCriteria criteria = buildCriteria(request, interpretation);
        if (!StringUtils.hasText(criteria.sectionKey())) {
            return List.of();
        }

        LinkedHashSet<String> tagSet = new LinkedHashSet<>();
        LinkedHashSet<String> keywordSet = new LinkedHashSet<>();
        if (interpretation != null) {
            addNormalizedStrings(tagSet, interpretation.tags());
            addNormalizedStrings(keywordSet, interpretation.keywords());
        }
        if (request != null) {
            addNormalizedStrings(tagSet, request.getTags());
            addNormalizedStrings(keywordSet, request.getKeywords());
        }
        List<String> tagFilters = new ArrayList<>(tagSet);
        List<String> keywordFilters = new ArrayList<>(keywordSet);

        Map<String, Object> interpretationContext = sanitizeContext(interpretation != null ? interpretation.context() : null);

        int limit = determineLimit(request);

        List<ChatbotResultDto> combined = runSearch(criteria, requestContext, interpretationContext, tagFilters, keywordFilters, limit);

        if (combined.isEmpty() && StringUtils.hasText(criteria.role())) {
            SearchCriteria relaxed = criteria.withoutRole();
            if (!relaxed.equals(criteria)) {
                List<ChatbotResultDto> retry = runSearch(relaxed, requestContext, interpretationContext, tagFilters, keywordFilters, limit);
                if (!retry.isEmpty()) {
                    combined = retry;
                    criteria = relaxed;
                }
            }
        }

        SearchCriteria fallbackCriteria = criteria;
        Map<String, Object> baseRequestContext = requestContext;
        Map<String, Object> baseInterpretationContext = interpretationContext;

        if (!combined.isEmpty()) {
            assignCfIds(combined, criteria, tagFilters, keywordFilters);
            return combined;
        }

        if (StringUtils.hasText(criteria.role())) {
            SearchCriteria relaxed = criteria.withoutRole();
            if (!relaxed.equals(criteria)) {
                List<ChatbotResultDto> retry = runSearch(relaxed, requestContext, interpretationContext, tagFilters, keywordFilters, limit);
                if (!retry.isEmpty()) {
                    combined = retry;
                    fallbackCriteria = relaxed;
                }
            }
        }

        if (combined.isEmpty()) {
            SearchCriteria relaxedContext = fallbackCriteria.withoutContext();
            if (!relaxedContext.equals(fallbackCriteria)) {
                List<ChatbotResultDto> retry = runSearch(relaxedContext, Collections.emptyMap(), Collections.emptyMap(), tagFilters, keywordFilters, limit);
                if (!retry.isEmpty()) {
                    combined = retry;
                    fallbackCriteria = relaxedContext;
                }
            }
        }

        if (!combined.isEmpty()) {
            assignCfIds(combined, fallbackCriteria, tagFilters, keywordFilters);
            return combined;
        }

        // Final fallback: attempt search using the original user message only, no structured filters.
        if (StringUtils.hasText(userMessage)) {
            SearchCriteria bareCriteria = new SearchCriteria(
                    slugify(request != null ? request.getSectionKey() : null),
                    null,
                    null,
                    null,
                    null,
                    null,
                    userMessage
            );
            List<ChatbotResultDto> retry = runSearch(bareCriteria, Collections.emptyMap(), Collections.emptyMap(), tagFilters, keywordFilters, limit);
            if (!retry.isEmpty()) {
                assignCfIds(retry, bareCriteria, tagFilters, keywordFilters);
                return retry;
            }
        }

        return List.of();
    }

    private int determineLimit(ChatbotRequest request) {
        if (request == null || request.getLimit() == null || request.getLimit() <= 0) {
            return 15;
        }
        return Math.min(request.getLimit(), 200);
    }

    private List<ChatbotResultDto> fetchVectorResults(SearchCriteria criteria,
                                                      Map<String, Object> context,
                                                      int limit,
                                                      List<String> tags,
                                                      List<String> keywords) {
        if (!StringUtils.hasText(criteria.embeddingQuery())) {
            return List.of();
        }
        try {
            List<ContentChunkWithDistance> rows = vectorSearchService.search(
                    criteria.embeddingQuery(),
                    null,
                    limit,
                    tags == null || tags.isEmpty() ? null : tags,
                    keywords == null || keywords.isEmpty() ? null : keywords,
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

    private List<ChatbotResultDto> runSearch(SearchCriteria criteria,
                                             Map<String, Object> requestContext,
                                             Map<String, Object> interpretationContext,
                                             List<String> tags,
                                             List<String> keywords,
                                             int limit) {
        Map<String, Object> derivedContext = buildDerivedContext(criteria);
        Map<String, Object> combinedContext = mergeContext(
                requestContext,
                interpretationContext
        );
        Map<String, Object> effectiveContext = sanitizeContext(
                mergeContext(
                        combinedContext,
                        derivedContext
                )
        );

        List<ChatbotResultDto> vectorResults = fetchVectorResults(criteria, effectiveContext, limit, tags, keywords);
        List<ChatbotResultDto> consolidatedResults = fetchConsolidatedResults(criteria, limit);

        return mergeResults(vectorResults, consolidatedResults, limit);
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
                             List<String> tags,
                             List<String> keywords) {
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

            dto.setMatchTerms(buildMatchTerms(dto, criteria, tags, keywords));
        }
    }

    private List<String> buildMatchTerms(ChatbotResultDto dto,
                                         SearchCriteria criteria,
                                         List<String> tags,
                                         List<String> keywords) {
        LinkedHashSet<String> terms = new LinkedHashSet<>();
        addTerm(terms, criteria.sectionKey());
        addTerm(terms, dto.getContentRole());
        addTerm(terms, criteria.role());
        addTerm(terms, dto.getPageId());
        addTerm(terms, criteria.pageId());
        addTerm(terms, criteria.country());
        addTerm(terms, criteria.language());

        if (tags != null) {
            terms.addAll(tags);
        }
        if (keywords != null) {
            terms.addAll(keywords);
        }
        return new ArrayList<>(terms);
    }

    private void addTerm(Set<String> terms, String value) {
        if (terms == null || !StringUtils.hasText(value)) {
            return;
        }
        terms.add(value);
    }

    private void addNormalizedStrings(Set<String> target, List<String> values) {
        if (target == null || values == null) {
            return;
        }
        for (String value : values) {
            if (StringUtils.hasText(value)) {
                target.add(value.trim().toLowerCase(Locale.ROOT));
            }
        }
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

    private SearchCriteria buildCriteria(ChatbotRequest request, QueryInterpretation interpretation) {
        String originalMessage = request != null ? request.getMessage() : null;
        String interpretedQuery = interpretation != null ? interpretation.rawQuery() : null;
        String message = StringUtils.hasText(interpretedQuery) ? interpretedQuery : originalMessage;

        SearchCriteriaBuilder builder = new SearchCriteriaBuilder()
                .message(message);

        String interpretedSectionKey = normalizeKey(interpretation != null ? interpretation.sectionKey() : null);
        String requestSectionKey = slugify(request != null ? request.getSectionKey() : null);
        String inferredSection = extractSectionKey(originalMessage);
        builder.sectionKey(firstNonBlank(interpretedSectionKey, requestSectionKey, inferredSection));

        String interpretedRole = normalizeRole(interpretation != null ? interpretation.role() : null);
        String explicitRole = normalizeRole(request != null ? request.getOriginal_field_name() : null);
        String inferredRole = extractRole(originalMessage);
        builder.role(firstNonBlank(interpretedRole, explicitRole, inferredRole));

        LocaleHints localeHints = parseLocaleHints(originalMessage);
        String interpretedLocale = normalizeLocale(interpretation != null ? interpretation.locale() : null);
        String interpretedLanguage = normalizeLanguageToken(interpretation != null ? interpretation.language() : null);
        String interpretedCountry = mapCountryCode(interpretation != null ? interpretation.country() : null);

        builder.locale(firstNonBlank(interpretedLocale, firstOf(localeHints.locales())));
        builder.language(firstNonBlank(interpretedLanguage, firstOf(localeHints.languages())));
        builder.country(firstNonBlank(interpretedCountry, firstOf(localeHints.countries())));

        builder.pageId(firstNonBlank(
                normalizePageId(interpretation != null ? interpretation.pageId() : null),
                extractPageId(originalMessage, localeHints)
        ));

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

    private Map<String, Object> sanitizeContext(Map<String, Object> context) {
        if (context == null || context.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, Object> cleaned = new LinkedHashMap<>();
        context.forEach((key, value) -> {
            Object sanitized = sanitizeContextValue(value);
            if (sanitized != null) {
                cleaned.put(key, sanitized);
            }
        });
        return cleaned.isEmpty() ? Collections.emptyMap() : cleaned;
    }

    private Object sanitizeContextValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String str) {
            String trimmed = str.trim();
            return StringUtils.hasText(trimmed) ? trimmed : null;
        }
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> nested = new LinkedHashMap<>();
            map.forEach((key, val) -> {
                Object sanitized = sanitizeContextValue(val);
                if (sanitized != null) {
                    nested.put(String.valueOf(key), sanitized);
                }
            });
            return nested.isEmpty() ? null : nested;
        }
        if (value instanceof Collection<?> collection) {
            List<Object> list = new ArrayList<>();
            for (Object item : collection) {
                Object sanitized = sanitizeContextValue(item);
                if (sanitized != null) {
                    list.add(sanitized);
                }
            }
            return list.isEmpty() ? null : list;
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
            String candidate = sanitizeRoleCandidate(matcher.group(1));
            if (candidate != null) {
                return candidate;
            }
        }
        String lower = message.toLowerCase(Locale.ROOT);
        for (String keyword : DEFAULT_ROLE_KEYWORDS) {
            if (lower.contains(keyword)) {
                return keyword;
            }
        }
        Matcher roleForSection = ROLE_FOR_SECTION_PATTERN.matcher(message);
        if (roleForSection.find()) {
            String candidate = sanitizeRoleCandidate(roleForSection.group(1));
            if (candidate != null) {
                return candidate;
            }
        }
        Matcher roleBeforeSection = ROLE_BEFORE_SECTION_PATTERN.matcher(message);
        while (roleBeforeSection.find()) {
            String candidate = sanitizeRoleCandidate(roleBeforeSection.group(1));
            if (candidate != null) {
                return candidate;
            }
        }
        return null;
    }

    private String sanitizeRoleCandidate(String candidate) {
        if (!StringUtils.hasText(candidate)) {
            return null;
        }
        String cleaned = candidate.trim()
                .replaceAll("^[^A-Za-z0-9_-]+", "")
                .replaceAll("[^A-Za-z0-9_-]+$", "");
        if (!StringUtils.hasText(cleaned)) {
            return null;
        }
        String lower = cleaned.toLowerCase(Locale.ROOT);
        if (lower.length() < 3) {
            return null;
        }
        if (ROLE_STOP_WORDS.contains(lower)) {
            return null;
        }
        return lower;
    }

    private String extractPageId(String message) {
        return extractPageId(message, LocaleHints.EMPTY);
    }

    private String extractPageId(String message, LocaleHints hints) {
        if (!StringUtils.hasText(message)) {
            return null;
        }
        List<String> tokens = TOKEN_PATTERN.matcher(message)
                .results()
                .map(MatchResult::group)
                .collect(Collectors.toList());
        if (tokens.isEmpty()) {
            return null;
        }
        Set<String> knownCountries = hints != null ? hints.countries() : Collections.emptySet();

        for (int i = 0; i < tokens.size(); i++) {
            String tokenLower = tokens.get(i).toLowerCase(Locale.ROOT);
            if (!PREPOSITION_WORDS.contains(tokenLower)) {
                continue;
            }
            for (int j = i + 1; j < tokens.size(); j++) {
                String candidate = tokens.get(j);
                String normalized = candidate.toLowerCase(Locale.ROOT);
                if (PREPOSITION_WORDS.contains(normalized) || PAGE_STOP_WORDS.contains(normalized)) {
                    continue;
                }
                if (normalized.contains("section") || normalized.contains("locale")) {
                    continue;
                }
                if (MESSAGE_LOCALE_PATTERN.matcher(candidate).matches()) {
                    continue;
                }
                if (isCountryToken(normalized, knownCountries)) {
                    continue;
                }
                if (normalized.length() < 3) {
                    continue;
                }
                return normalizePageId(normalized);
            }
        }

        for (int i = tokens.size() - 1; i >= 0; i--) {
            String candidate = tokens.get(i).toLowerCase(Locale.ROOT);
            if (PREPOSITION_WORDS.contains(candidate) || PAGE_STOP_WORDS.contains(candidate)) {
                continue;
            }
            if (candidate.contains("section") || candidate.contains("locale")) {
                continue;
            }
            if (MESSAGE_LOCALE_PATTERN.matcher(candidate).matches()) {
                continue;
            }
            if (isCountryToken(candidate, knownCountries)) {
                continue;
            }
            if (candidate.length() < 3) {
                continue;
            }
            return normalizePageId(candidate);
        }

        return null;
    }

    private LocaleHints parseLocaleHints(String message) {
        if (!StringUtils.hasText(message)) {
            return LocaleHints.EMPTY;
        }
        LinkedHashSet<String> locales = new LinkedHashSet<>();
        LinkedHashSet<String> languages = new LinkedHashSet<>();
        LinkedHashSet<String> countries = new LinkedHashSet<>();

        Matcher localeMatcher = MESSAGE_LOCALE_PATTERN.matcher(message);
        while (localeMatcher.find()) {
            String language = localeMatcher.group(1).toLowerCase(Locale.ROOT);
            String country = localeMatcher.group(2).toUpperCase(Locale.ROOT);
            locales.add(language + "_" + country);
            languages.add(language);
            countries.add(country);
        }

        List<String> tokens = TOKEN_PATTERN.matcher(message)
                .results()
                .map(MatchResult::group)
                .collect(Collectors.toList());

        for (int i = 0; i < tokens.size(); i++) {
            String token = tokens.get(i);
            String lower = token.toLowerCase(Locale.ROOT);

            String language = normalizeLanguageToken(lower);
            if (language != null) {
                languages.add(language);
            }

            String country = mapCountryCode(lower);
            if (country != null) {
                countries.add(country);
            }

            if (i + 1 < tokens.size()) {
                String bigram = (lower + " " + tokens.get(i + 1).toLowerCase(Locale.ROOT)).trim();
                String bigramCountry = mapCountryCode(bigram);
                if (bigramCountry != null) {
                    countries.add(bigramCountry);
                }
            }
        }

        LinkedHashSet<String> normalizedLocales = new LinkedHashSet<>(locales);
        if (normalizedLocales.isEmpty() && !languages.isEmpty() && !countries.isEmpty()) {
            for (String lang : languages) {
                for (String country : countries) {
                    normalizedLocales.add(lang + "_" + country);
                }
            }
        }

        return new LocaleHints(
                Collections.unmodifiableSet(normalizedLocales),
                Collections.unmodifiableSet(languages),
                Collections.unmodifiableSet(countries)
        );
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) {
            return null;
        }
        return key.trim().toLowerCase(Locale.ROOT);
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
        String trimmed = value.trim();
        int separator = Math.max(trimmed.lastIndexOf('_'), trimmed.lastIndexOf('-'));
        if (separator > 0 && separator + 1 < trimmed.length()) {
            String prefix = trimmed.substring(0, separator);
            String candidate = normalizeLanguageToken(prefix);
            if (candidate != null) {
                return candidate;
            }
        }
        return normalizeLanguageToken(trimmed);
    }

    private String mapCountryAlias(String value) {
        return mapCountryCode(value);
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

    private boolean isCountryToken(String value, Set<String> knownCountries) {
        if (!StringUtils.hasText(value)) {
            return false;
        }
        String candidate = value.toUpperCase(Locale.ROOT);
        if (knownCountries != null && knownCountries.contains(candidate)) {
            return true;
        }
        return mapCountryCode(value) != null;
    }

    private String normalizeLanguageToken(String token) {
        if (!StringUtils.hasText(token)) {
            return null;
        }
        String lower = token.toLowerCase(Locale.ROOT);
        if (isoLanguageCodes.contains(lower)) {
            return lower;
        }
        String sanitized = sanitizeLanguageKey(lower);
        return languageIndex.get(sanitized);
    }

    private String sanitizeLanguageKey(String value) {
        return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z]", "");
    }

    private Set<String> buildIsoLanguageCodes() {
        LinkedHashSet<String> codes = Arrays.stream(Locale.getISOLanguages())
                .map(code -> code.toLowerCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableSet(codes);
    }

    private Map<String, String> buildLanguageIndex() {
        Map<String, String> index = new LinkedHashMap<>();
        for (String iso : Locale.getISOLanguages()) {
            Locale locale = new Locale(iso);
            addLanguageMapping(index, locale.getDisplayLanguage(Locale.ENGLISH), iso);
            addLanguageMapping(index, locale.getDisplayLanguage(locale), iso);
        }
        return Collections.unmodifiableMap(index);
    }

    private void addLanguageMapping(Map<String, String> index, String value, String code) {
        if (!StringUtils.hasText(value) || !StringUtils.hasText(code)) {
            return;
        }
        String sanitized = sanitizeLanguageKey(value);
        if (!StringUtils.hasText(sanitized)) {
            return;
        }
        index.putIfAbsent(sanitized, code.toLowerCase(Locale.ROOT));
    }

    private Set<String> buildIsoCountryCodes() {
        LinkedHashSet<String> codes = Arrays.stream(Locale.getISOCountries())
                .map(code -> code.toUpperCase(Locale.ROOT))
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return Collections.unmodifiableSet(codes);
    }

    private Map<String, String> buildIso3ToIso2Index() {
        Map<String, String> index = new LinkedHashMap<>();
        for (String iso2 : Locale.getISOCountries()) {
            Locale locale = new Locale("", iso2);
            try {
                String iso3 = locale.getISO3Country();
                if (StringUtils.hasText(iso3)) {
                    index.putIfAbsent(iso3.toUpperCase(Locale.ROOT), iso2.toUpperCase(Locale.ROOT));
                }
            } catch (MissingResourceException ignored) {
                // ignore locales without ISO3 representation
            }
        }
        return Collections.unmodifiableMap(index);
    }

    private Map<String, String> buildCountryIndex() {
        Map<String, String> index = new LinkedHashMap<>();
        for (String iso2 : Locale.getISOCountries()) {
            Locale locale = new Locale("", iso2);
            addCountryMapping(index, locale.getDisplayCountry(Locale.ENGLISH), iso2);
            addCountryMapping(index, locale.getDisplayCountry(locale), iso2);
            addCountryMapping(index, iso2, iso2);
            try {
                String iso3 = locale.getISO3Country();
                if (StringUtils.hasText(iso3)) {
                    addCountryMapping(index, iso3, iso2);
                }
            } catch (MissingResourceException ignored) {
                // ignore
            }
        }
        return Collections.unmodifiableMap(index);
    }

    private void addCountryMapping(Map<String, String> index, String value, String iso2) {
        if (!StringUtils.hasText(value) || !StringUtils.hasText(iso2)) {
            return;
        }
        String sanitized = sanitizeCountryKey(value);
        if (!StringUtils.hasText(sanitized)) {
            return;
        }
        index.putIfAbsent(sanitized, iso2.toUpperCase(Locale.ROOT));
    }

    private String sanitizeCountryKey(String value) {
        return value.toLowerCase(Locale.ROOT).replaceAll("[^a-z]", "");
    }

    private String mapCountryCode(String value) {
        if (!StringUtils.hasText(value)) {
            return null;
        }
        String trimmed = value.trim();
        int separator = Math.max(trimmed.lastIndexOf('_'), trimmed.lastIndexOf('-'));
        if (separator > 0 && separator + 1 < trimmed.length()) {
            String suffix = trimmed.substring(separator + 1);
            String mappedSuffix = mapCountryCode(suffix);
            if (mappedSuffix != null) {
                return mappedSuffix;
            }
        }
        String sanitized = sanitizeCountryKey(trimmed);
        if (!StringUtils.hasText(sanitized)) {
            return null;
        }
        if (sanitized.length() == 2) {
            String iso2 = sanitized.toUpperCase(Locale.ROOT);
            if (isoCountryCodes.contains(iso2)) {
                return iso2;
            }
        }
        String mapped = countryIndex.get(sanitized);
        if (mapped != null) {
            return mapped;
        }
        if (sanitized.length() == 3) {
            String iso3 = sanitized.toUpperCase(Locale.ROOT);
            String iso2 = iso3ToIso2.get(iso3);
            if (iso2 != null) {
                return iso2;
            }
        }
        return null;
    }

    private String firstOf(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return values.iterator().next();
    }

    private record LocaleHints(Set<String> locales, Set<String> languages, Set<String> countries) {
        private static final LocaleHints EMPTY = new LocaleHints(Set.of(), Set.of(), Set.of());
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

        SearchCriteria withoutRole() {
            if (role == null || role.isBlank()) {
                return this;
            }
            return new SearchCriteria(sectionKey, null, locale, language, country, pageId, message);
        }

        SearchCriteria withoutContext() {
            if ((locale == null || locale.isBlank())
                    && (language == null || language.isBlank())
                    && (country == null || country.isBlank())
                    && (pageId == null || pageId.isBlank())) {
                return this;
            }
            return new SearchCriteria(sectionKey, role, null, null, null, null, message);
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
