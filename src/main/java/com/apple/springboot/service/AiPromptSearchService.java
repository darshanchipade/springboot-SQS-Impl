package com.apple.springboot.service;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.repository.ConsolidatedEnrichedSectionRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class AiPromptSearchService {

    private static final Pattern SECTION_KEY_PATTERN =
            Pattern.compile("(?i)\\b([a-z0-9]+(?:-[a-z0-9]+)*)-section(?:-[a-z0-9]+)*\\b");

    private final BedrockEnrichmentService bedrockEnrichmentService;
    private final VectorSearchService vectorSearchService;
    private final ConsolidatedEnrichedSectionRepository consolidatedRepo;
    private final ObjectMapper objectMapper;

    public AiPromptSearchService(BedrockEnrichmentService bedrockEnrichmentService,
                                 VectorSearchService vectorSearchService,
                                 ConsolidatedEnrichedSectionRepository consolidatedRepo,
                                 ObjectMapper objectMapper) {
        this.bedrockEnrichmentService = bedrockEnrichmentService;
        this.vectorSearchService = vectorSearchService;
        this.consolidatedRepo = consolidatedRepo;
        this.objectMapper = objectMapper;
    }

    public List<ChatbotResultDto> aiSearch(ChatbotRequest request) {
        String userMessage = request != null ? request.getMessage() : null;
        if (!StringUtils.hasText(userMessage)) return List.of();

        // Build prompt -> AI JSON
        String prompt = loadPromptTemplate()
                .replace("{conversation_history}", "")
                .replace("{user_message}", userMessage);

        JsonNode aiJson = null;
        try {
            String raw = bedrockEnrichmentService.invokeChatForText(prompt, null);
            aiJson = objectMapper.readTree(stripJsonFences(raw));
        } catch (ThrottledException te) {
            throw te;
        } catch (Exception ignore) {
            // Fallback to using user message
        }

        // Extract filters
        String query = userMessage;
        List<String> tags = new ArrayList<>();
        List<String> keywords = new ArrayList<>();
        String roleHint = null;
        Map<String, Object> context = new HashMap<>();

        if (aiJson != null) {
            if (aiJson.hasNonNull("query") && aiJson.get("query").isTextual()) {
                query = aiJson.get("query").asText(query);
            }
            tags.addAll(readStrings(aiJson.get("tags")));
            keywords.addAll(readStrings(aiJson.get("keywords")));
            if (aiJson.hasNonNull("original_field_name") && aiJson.get("original_field_name").isTextual()) {
                roleHint = aiJson.get("original_field_name").asText();
            }
            if (aiJson.hasNonNull("context") && aiJson.get("context").isObject()) {
                // Convert AI context to Map, preserving structure including envelope.locale, envelope.country, envelope.language
                @SuppressWarnings("unchecked")
                Map<String, Object> ctx = objectMapper.convertValue(aiJson.get("context"), Map.class);
                if (ctx != null) context.putAll(ctx);
            }
        }
        if (request != null) {
            if (request.getTags() != null) tags.addAll(request.getTags());
            if (request.getKeywords() != null) keywords.addAll(request.getKeywords());
            if (request.getContext() != null) {
                // Merge request context, ensuring envelope fields are preserved
                Map<String, Object> reqCtx = request.getContext();
                if (reqCtx.containsKey("envelope")) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> reqEnvelope = (Map<String, Object>) reqCtx.get("envelope");
                    if (reqEnvelope != null) {
                        Map<String, Object> envelopeMap = new HashMap<>();
                        if (context.containsKey("envelope") && context.get("envelope") instanceof Map) {
                            @SuppressWarnings("unchecked")
                            Map<String, Object> existing = (Map<String, Object>) context.get("envelope");
                            envelopeMap.putAll(existing);
                        }
                        envelopeMap.putAll(reqEnvelope);
                        context.put("envelope", envelopeMap);
                    }
                } else {
                    context.putAll(reqCtx);
                }
            }
            if (StringUtils.hasText(request.getOriginal_field_name())) roleHint = request.getOriginal_field_name();
        }

        // Derive country and language from locale if not explicitly provided
        Set<List<String>> derivedLocalePaths = enrichContextWithLocale(context);

        tags = tags.stream().filter(StringUtils::hasText).map(String::trim).distinct().collect(Collectors.toList());
        keywords = keywords.stream().filter(StringUtils::hasText).map(String::trim).distinct().collect(Collectors.toList());

        Map<List<String>, Set<String>> contextFilterIndex = buildContextFilterIndex(context);

        // Derive section key (from AI context or user text)
        String sectionKey = null;
        if (aiJson != null && aiJson.has("context")) {
            JsonNode facets = aiJson.get("context").path("facets");
            if (facets.hasNonNull("sectionKey") && facets.get("sectionKey").isTextual()) {
                sectionKey = normalizeKey(facets.get("sectionKey").asText());
            }
        }
        if (!StringUtils.hasText(sectionKey)) {
            sectionKey = extractSectionKey(userMessage);
        }

        // Heuristic role extraction from user message when AI didn't provide one.
        // Note: we'll only apply role filtering if the user explicitly asked for a role.
        boolean userExplicitRole = userExplicitlyRequestedRole(userMessage, sectionKey)
                || (request != null && StringUtils.hasText(request.getOriginal_field_name()));
        if (!StringUtils.hasText(roleHint)) {
            String inferred = inferRoleHint(userMessage, sectionKey);
            if (StringUtils.hasText(inferred)) {
                roleHint = inferred;
            }
        }

        // Default to a large limit to support open-ended results; callers can still pass an explicit limit
        int limit = (request != null && request.getLimit() != null && request.getLimit() > 0)
                ? request.getLimit() : 1000;

        try {
            // Vector search constrained by section; no strict role in SQL
            List<ContentChunkWithDistance> vectorResults =
                    (sectionKey != null)
                            ? vectorSearchService.search(
                            query,
                            null,
                            limit,
                            tags.isEmpty() ? null : tags,
                            keywords.isEmpty() ? null : keywords,
                            context.isEmpty() ? null : context,
                            null,
                            sectionKey)
                            : vectorSearchService.search(
                            query,
                            null,
                            limit,
                            tags.isEmpty() ? null : tags,
                            keywords.isEmpty() ? null : keywords,
                            context.isEmpty() ? null : context,
                            null);

            List<ChatbotResultDto> vectorDtos = vectorResults.stream()
                    .map(r -> toDto(r.getContentChunk().getConsolidatedEnrichedSection(), "content_chunks"))
                    .collect(Collectors.toList());

            // Optional role post-filter (contains, case-insensitive)
            if (StringUtils.hasText(roleHint)) {
                String rf = roleHint.toLowerCase();
                vectorDtos = vectorDtos.stream()
                        .filter(d -> d.getContentRole() != null && d.getContentRole().toLowerCase().contains(rf))
                        .collect(Collectors.toList());
            }

            // Metadata search: prefer section-constrained when available
            List<ConsolidatedEnrichedSection> metaMatches =
                    (sectionKey != null)
                            ? consolidatedRepo.findBySectionKey(sectionKey, Math.max(limit * 2, 50))
                            : consolidatedRepo.findByMetadataQuery(query, Math.max(limit * 2, 50));
            metaMatches = filterByContext(metaMatches, contextFilterIndex, derivedLocalePaths);

            if (org.springframework.util.StringUtils.hasText(sectionKey)) {
                // Aggregate results from multiple strategies and dedupe
                java.util.LinkedHashMap<java.util.UUID, ConsolidatedEnrichedSection> agg = new java.util.LinkedHashMap<>();

                List<ConsolidatedEnrichedSection> broad = consolidatedRepo.findBySectionKey(
                        sectionKey,
                        Math.max(limit * 4, 2000));
                if (broad != null) broad.forEach(s -> { if (s.getId() != null) agg.putIfAbsent(s.getId(), s); });

                List<ConsolidatedEnrichedSection> exactCtx = consolidatedRepo.findByContextSectionKey(
                        sectionKey,
                        Math.max(limit * 4, 2000));
                if (exactCtx != null) exactCtx.forEach(s -> { if (s.getId() != null) agg.putIfAbsent(s.getId(), s); });

                String base = sectionKey;
                if (base.endsWith("-section")) {
                    base = base.substring(0, base.length() - "-section".length());
                }
                String[] candidates = new String[] {
                        sectionKey,
                        sectionKey + "-items",
                        base,
                        base + "-section-items"
                };
                for (String cand : candidates) {
                    if (cand == null || cand.isBlank()) continue;
                    List<ConsolidatedEnrichedSection> alt = consolidatedRepo.findBySectionKey(
                            cand.toLowerCase(Locale.ROOT), Math.max(limit * 4, 2000));
                    if (alt != null) alt.forEach(s -> { if (s.getId() != null) agg.putIfAbsent(s.getId(), s); });
                }

                List<ConsolidatedEnrichedSection> rows = new java.util.ArrayList<>(agg.values());
                rows = filterByContext(rows, contextFilterIndex, derivedLocalePaths);

                // Apply a role only if the user explicitly requested it; ignore AI-suggested roles otherwise
                if (userExplicitRole && org.springframework.util.StringUtils.hasText(roleHint)) {
                    final String rf = roleHint.toLowerCase();
                    boolean anyMatch = rows.stream().anyMatch(s -> s.getOriginalFieldName() != null && s.getOriginalFieldName().toLowerCase().contains(rf));
                    if (anyMatch) {
                        rows = rows.stream()
                                .filter(s -> s.getOriginalFieldName() != null
                                        && s.getOriginalFieldName().toLowerCase().contains(rf))
                                .limit(limit)
                                .toList();
                    } else {
                        roleHint = null; // avoid applying non-existent role like "content"
                        rows = rows.stream().limit(limit).toList();
                    }
                } else {
                    // all content for the section
                    roleHint = null; // ensure match_terms does not include unintended role
                    rows = rows.stream().limit(limit).toList();
                }

                List<ChatbotResultDto> out = rows.stream()
                        .map(s -> toDto(s, "consolidated_enriched_sections"))
                        .collect(java.util.stream.Collectors.toList());

                // annotate section + match_terms
                for (int i = 0; i < out.size(); i++) {
                    ChatbotResultDto item = out.get(i);
                    item.setCfId("cf" + (i + 1));
                    item.setSection(sectionKey);
                    java.util.LinkedHashSet<String> terms = new java.util.LinkedHashSet<>();
                    if (userExplicitRole && org.springframework.util.StringUtils.hasText(roleHint)) {
                        terms.add(roleHint);
                    }
                    terms.add(sectionKey);
                    item.setMatchTerms(new java.util.ArrayList<>(terms));
                }

                return out;
            }

            // Fallback path when no sectionKey-driven consolidated results were returned
            List<ChatbotResultDto> fallback = !vectorDtos.isEmpty()
                    ? vectorDtos
                    : metaMatches.stream()
                    .map(s -> toDto(s, "consolidated_enriched_sections"))
                    .collect(Collectors.toList());
            return fallback;
        } catch(Exception e){
            return List.of();
        }
    }

    // Helpers

    private ChatbotResultDto toDto(ConsolidatedEnrichedSection section, String source) {
        ChatbotResultDto dto = new ChatbotResultDto();
        dto.setSection("ai-search");
        dto.setSectionPath(section.getSectionPath());
        dto.setSectionUri(section.getSectionUri());
        dto.setCleansedText(section.getCleansedText());
        dto.setSource(source);
        dto.setContentRole(section.getOriginalFieldName());
        dto.setLastModified(section.getSavedAt() != null ? section.getSavedAt().toString() : null);
        dto.setMatchTerms(List.of("ai-search"));
        // enrich with page_id, tenant, locale, country, language
        String locale = extractLocale(section);
        dto.setLocale(locale);
        dto.setTenant(extractTenant(section));
        dto.setPageId(extractPageId(section));

        // Derive country and language from locale if not explicitly in context
        String country = extractCountry(section, locale);
        String language = extractLanguage(section, locale);
        dto.setCountry(country);
        dto.setLanguage(language);

        return dto;
    }

    private String loadPromptTemplate() {
        try {
            ClassPathResource resource = new ClassPathResource("prompts/interactive_search_prompt.txt");
            byte[] bytes = resource.getInputStream().readAllBytes();
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return "Generate only one JSON object with fields among: query, tags, keywords, original_field_name, context. No text outside JSON.\n\n"
                    + "<conversation_history>\n{conversation_history}\n</conversation_history>\n"
                    + "<user_message>\n{user_message}\n</user_message>";
        }
    }

    private List<String> readStrings(JsonNode node) {
        if (node == null || node.isNull()) return List.of();
        if (node.isTextual()) return List.of(node.asText());
        if (node.isArray()) {
            List<String> out = new ArrayList<>();
            node.forEach(n -> { if (n.isTextual()) out.add(n.asText()); });
            return out;
        }
        return List.of();
    }

    private String stripJsonFences(String text) {
        if (text == null) return null;
        String trimmed = text.trim();
        if (trimmed.startsWith("```") && trimmed.endsWith("```")) {
            trimmed = trimmed.substring(3, trimmed.length() - 3).trim();
            int nl = trimmed.indexOf('\n');
            if (nl > 0 && trimmed.substring(0, nl).matches("[a-zA-Z0-9_-]+")) {
                trimmed = trimmed.substring(nl + 1).trim();
            }
        }
        return trimmed;
    }

    private String extractSectionKey(String text) {
        if (!StringUtils.hasText(text)) return null;
        var m = SECTION_KEY_PATTERN.matcher(text);
        if (m.find()) return normalizeKey(m.group(0));
        for (String token : text.split("\\s+")) {
            long hyphens = token.chars().filter(ch -> ch == '-').count();
            if (hyphens >= 2 && token.toLowerCase().endsWith("-section")) {
                return normalizeKey(token);
            }
        }
        return null;
    }

    private String normalizeKey(String key) {
        if (!StringUtils.hasText(key)) return null;
        return key.trim().toLowerCase();
    }

    private String inferRoleHint(String text, String sectionKey) {
        if (!StringUtils.hasText(text)) return null;
        String t = text.toLowerCase(Locale.ROOT).trim();

        // Try to capture phrase before "for/of <sectionKey>" or just before "for/of"
        String roleCandidate = null;
        if (StringUtils.hasText(sectionKey)) {
            String sk = Pattern.quote(sectionKey.toLowerCase(Locale.ROOT));
            var mFor = Pattern.compile("(?i)\\bgive\\s+me\\s+(.+?)\\s+(?:for|of)\\s+.*" + sk + "\\b").matcher(t);
            if (mFor.find()) roleCandidate = mFor.group(1);
            if (roleCandidate == null) {
                var mGeneric = Pattern.compile("(?i)\\b(.+?)\\s+(?:for|of)\\s+.*" + sk + "\\b").matcher(t);
                if (mGeneric.find()) roleCandidate = mGeneric.group(1);
            }
        }
        if (roleCandidate == null) {
            var mFor = Pattern.compile("(?i)\\bgive\\s+me\\s+(.+?)\\s+(?:for|of)\\s+").matcher(t);
            if (mFor.find()) roleCandidate = mFor.group(1);
            if (roleCandidate == null) {
                var mGeneric = Pattern.compile("(?i)\\b([a-z0-9_-]{3,})\\b\\s+(?:for|of)\\b").matcher(t);
                if (mGeneric.find()) roleCandidate = mGeneric.group(1);
            }
        }

        if (!StringUtils.hasText(roleCandidate)) return null;

        // Tokenize and pick a specific token without any hardcoded stopword list
        List<String> tokens = Arrays.stream(roleCandidate.split("\\s+"))
                .map(s -> s.replaceAll("[^a-z0-9_-]", ""))
                .filter(StringUtils::hasText)
                .toList();
        if (tokens.isEmpty()) return null;

        // Choose the last significant token, which often corresponds to the field name
        String chosen = tokens.get(tokens.size() - 1);
        return StringUtils.hasText(chosen) ? chosen : null;
    }

    private boolean userExplicitlyRequestedRole(String text, String sectionKey) {
        if (!StringUtils.hasText(text)) return false;
        String t = text.toLowerCase(Locale.ROOT);
        if (StringUtils.hasText(sectionKey)) {
            String sk = Pattern.quote(sectionKey.toLowerCase(Locale.ROOT));
            // only treat explicit single-token role names as role requests to avoid phrases like "all content"
            if (Pattern.compile("(?i)\\b(?:give\\s+me\\s+)?([a-z0-9_-]+)\\s+(?:for|of)\\s+.*" + sk + "\\b").matcher(t).find()) return true;
        }
        // generic: also only a single token before for/of
        return Pattern.compile("(?i)\\b(?:give\\s+me\\s+)?([a-z0-9_-]+)\\s+(?:for|of)\\b").matcher(t).find();
    }

    private String pickBestRole(String roleCandidate, List<ConsolidatedEnrichedSection> rows) {
        List<String> available = rows.stream()
                .map(ConsolidatedEnrichedSection::getOriginalFieldName)
                .filter(Objects::nonNull)
                .map(s -> s.toLowerCase(Locale.ROOT))
                .distinct()
                .toList();
        if (available.isEmpty()) return null;
        String cand = roleCandidate != null ? roleCandidate.toLowerCase(Locale.ROOT) : null;

        if (cand == null || cand.isBlank()) return null;
        // 1) Exact match
        if (cand != null && available.contains(cand)) return cand;

        // 2) Contains either way with boundaries relaxed
        if (cand != null) {
            for (String a : available) {
                if (a.contains(cand) || cand.contains(a)) return a;
            }
        }
        // 4) Fallback: no confident role
        return null;
    }

    private String extractLocale(ConsolidatedEnrichedSection s) {
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof Map<?,?> m) {
                Object loc = m.get("locale");
                if (loc instanceof String str && StringUtils.hasText(str)) return str;
            }
        }
        String fromPath = extractLocaleFromPath(s.getSectionUri());
        if (fromPath == null) fromPath = extractLocaleFromPath(s.getSectionPath());
        return fromPath;
    }

    private String extractLanguage(ConsolidatedEnrichedSection s, String localeFallback) {
        // First try to get from context.envelope.language
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof Map<?,?> m) {
                Object lang = m.get("language");
                if (lang instanceof String str && StringUtils.hasText(str)) {
                    return str.toLowerCase(Locale.ROOT);
                }
            }
        }
        // Derive from locale if available
        if (StringUtils.hasText(localeFallback)) {
            String normalized = localeFallback.replace('-', '_').trim();
            int idx = normalized.indexOf('_');
            if (idx > 0) {
                return normalized.substring(0, idx).toLowerCase(Locale.ROOT);
            } else if (normalized.length() >= 2) {
                return normalized.toLowerCase(Locale.ROOT);
            }
        }
        return null;
    }

    private String extractCountry(ConsolidatedEnrichedSection s, String localeFallback) {
        // First try to get from context.envelope.country
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof Map<?,?> m) {
                Object country = m.get("country");
                if (country instanceof String str && StringUtils.hasText(str)) {
                    return str.toUpperCase(Locale.ROOT);
                }
            }
        }
        // Derive from locale if available
        if (StringUtils.hasText(localeFallback)) {
            String normalized = localeFallback.replace('-', '_').trim();
            int idx = normalized.indexOf('_');
            if (idx >= 0 && idx + 1 < normalized.length()) {
                return normalized.substring(idx + 1).toUpperCase(Locale.ROOT);
            }
        }
        return null;
    }

    private String extractTenant(ConsolidatedEnrichedSection s) {
        if (s.getContext() != null) {
            Object env = s.getContext().get("envelope");
            if (env instanceof Map<?,?> m) {
                Object ten = m.get("tenant");
                if (ten instanceof String str && StringUtils.hasText(str)) return str;
            }
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
        var m = Pattern.compile("/([a-z]{2}_[A-Z]{2})/").matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractTenantFromPath(String path) {
        if (!StringUtils.hasText(path)) return null;
        var m = Pattern.compile("/content/dam/([^/]+)/").matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extractPageIdFromPath(String path) {
        if (!StringUtils.hasText(path)) return null;
        var m = Pattern.compile("/[a-z]{2}_[A-Z]{2}/([^/]+)/").matcher(path);
        if (m.find()) return m.group(1);
        return null;
    }

    private Map<List<String>, Set<String>> buildContextFilterIndex(Map<String, Object> context) {
        Map<List<String>, Set<String>> index = new LinkedHashMap<>();
        if (context == null || context.isEmpty()) return index;
        buildContextFilterIndexRecursive(context, new ArrayDeque<>(), index);
        return index;
    }

    private void buildContextFilterIndexRecursive(Object value,
                                                  Deque<String> path,
                                                  Map<List<String>, Set<String>> index) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (entry.getKey() == null) continue;
                path.addLast(entry.getKey().toString());
                buildContextFilterIndexRecursive(entry.getValue(), path, index);
                path.removeLast();
            }
            return;
        }

        if (path.isEmpty()) {
            return;
        }

        if (value instanceof Collection<?> collection) {
            Set<String> normalized = collection.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .map(this::normalizeContextValue)
                    .filter(str -> !str.isBlank())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            if (!normalized.isEmpty()) {
                index.merge(new ArrayList<>(path), normalized, (existing, add) -> {
                    existing.addAll(add);
                    return existing;
                });
            }
        } else if (value != null) {
            String normalized = normalizeContextValue(value.toString());
            if (!normalized.isBlank()) {
                index.merge(new ArrayList<>(path),
                        new LinkedHashSet<>(List.of(normalized)),
                        (existing, add) -> {
                            existing.addAll(add);
                            return existing;
                        });
            }
        }
    }

    private List<ConsolidatedEnrichedSection> filterByContext(List<ConsolidatedEnrichedSection> sections,
                                                              Map<List<String>, Set<String>> contextFilterIndex,
                                                              Set<List<String>> optionalPaths) {
        if (contextFilterIndex == null || contextFilterIndex.isEmpty() || sections == null || sections.isEmpty()) {
            return sections;
        }
        return sections.stream()
                .filter(section -> matchesContextFilters(section, contextFilterIndex, optionalPaths))
                .collect(Collectors.toList());
    }

    private boolean matchesContextFilters(ConsolidatedEnrichedSection section,
                                          Map<List<String>, Set<String>> filterIndex,
                                          Set<List<String>> optionalPaths) {
        if (filterIndex == null || filterIndex.isEmpty()) {
            return true;
        }
        Map<String, Object> sectionContext = section.getContext();
        if (sectionContext == null || sectionContext.isEmpty()) {
            return false;
        }
        for (Map.Entry<List<String>, Set<String>> entry : filterIndex.entrySet()) {
            if (entry.getValue().isEmpty()) {
                continue;
            }
            Object actual = resolveContextValue(sectionContext, entry.getKey());
            if (!contextValueMatches(section, actual, entry.getValue(), entry.getKey(), optionalPaths)) {
                return false;
            }
        }
        return true;
    }

    private Object resolveContextValue(Map<String, Object> context, List<String> path) {
        Object current = context;
        for (String key : path) {
            if (!(current instanceof Map<?, ?> map)) {
                return null;
            }
            current = map.get(key);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private boolean contextValueMatches(ConsolidatedEnrichedSection section,
                                        Object actual,
                                        Set<String> expected,
                                        List<String> path,
                                        Set<List<String>> optionalPaths) {
        if (actual == null) {
            if (optionalPaths != null && optionalPaths.contains(path)) {
                return true;
            }
            if (isLocalePath(path)) {
                String derivedLocale = extractLocale(section);
                if (StringUtils.hasText(derivedLocale)) {
                    String normalized = normalizeContextValue(derivedLocale);
                    if (expected.contains(normalized)) {
                        return true;
                    }
                }
            }
            return false;
        }
        if (actual instanceof Collection<?> collection) {
            for (Object item : collection) {
                if (item == null) continue;
                String normalized = normalizeContextValue(item.toString());
                if (expected.contains(normalized)) {
                    return true;
                }
            }
            return false;
        }
        if (actual instanceof Map<?, ?>) {
            return false;
        }
        String normalized = normalizeContextValue(actual.toString());
        return expected.contains(normalized);
    }

    private String normalizeContextValue(String raw) {
        if (raw == null) {
            return "";
        }
        return raw.trim().toLowerCase(Locale.ROOT);
    }

    private boolean isLocalePath(List<String> path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        if (path.size() == 2) {
            return "envelope".equals(path.get(0)) && "locale".equals(path.get(1));
        }
        if (path.size() == 1) {
            return "locale".equals(path.get(0));
        }
        return false;
    }

    private Set<List<String>> enrichContextWithLocale(Map<String, Object> context) {
        Set<List<String>> derivedPaths = new java.util.HashSet<>();
        if (context == null || !context.containsKey("envelope")) return derivedPaths;

        Object envelopeObj = context.get("envelope");
        if (!(envelopeObj instanceof Map)) return derivedPaths;

        @SuppressWarnings("unchecked")
        Map<String, Object> envelope = (Map<String, Object>) envelopeObj;

        Object localeObj = envelope.get("locale");
        if (!(localeObj instanceof String)) return derivedPaths;

        String locale = ((String) localeObj).trim();
        if (!StringUtils.hasText(locale)) return derivedPaths;

        // Normalize locale: handle formats like "en_US", "en-US"
        String normalized = locale.replace('-', '_');
        String[] parts = normalized.split("_");

        if (parts.length >= 2) {
            String language = parts[0].toLowerCase(Locale.ROOT);
            String country = parts[1].toUpperCase(Locale.ROOT);

            if (!envelope.containsKey("language") || envelope.get("language") == null) {
                envelope.put("language", language);
                derivedPaths.add(java.util.List.of("envelope", "language"));
            }
            if (!envelope.containsKey("country") || envelope.get("country") == null) {
                envelope.put("country", country);
                derivedPaths.add(java.util.List.of("envelope", "country"));
            }
        } else if (parts.length == 1) {
            String language = parts[0].toLowerCase(Locale.ROOT);
            if (!envelope.containsKey("language") || envelope.get("language") == null) {
                envelope.put("language", language);
                derivedPaths.add(java.util.List.of("envelope", "language"));
            }
        }

        return derivedPaths;
    }
}