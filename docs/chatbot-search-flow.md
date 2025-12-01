# Chatbot & AI Prompt Search Reference

This document explains how a query moves through `ChatbotService` and `AiPromptSearchService`, and provides a per-method reference to make future maintenance easier.

---

## ChatbotService Query Flow

1. **Entry** – `ChatbotService.query(...)` receives the `ChatbotRequest`, sanitizes context, and calls `QueryInterpretationService` for LLM-assisted hints.
2. **Criteria building** – `buildCriteria(...)` merges explicit request parameters, interpretation output, and regex-derived hints (section, role, locale, country, page).
3. **Context prep** – `sanitizeContext(...)`, `buildDerivedContext(...)`, and `mergeContext(...)` align request/interpretation context with derived metadata (locale/language/country/page).
4. **Primary search** – `runSearch(...)` launches:
   - `fetchVectorResults(...)` → embedding-based similarity via `VectorSearchService`.
   - `fetchConsolidatedResults(...)` → metadata search via `ConsolidatedEnrichedSectionRepository`.
5. **Fallback discovery** – if no matches, `trySectionDiscovery(...)` expands section keys from related page IDs.
6. **Combination & ranking** – `mergeResults(...)` + `addResults(...)` dedupe vector and metadata results using `buildResultKey(...)`.
7. **DTO enrichment** – `assignCfIds(...)` adds `cfId`, match terms, derived locale/language/country, etc., via helper methods (`deriveLocale`, `resolveTenant`, `mapSection`, ...).
8. **Final filtering** – `matchesCriteria(...)` ensures locale/role/ country/page filters still hold; if empty, the flow progressively relaxes role and context constraints before returning an empty list.

---

## ChatbotService Method Reference

### Entry, Search & Combination

| Method | Responsibility | Key Notes |
| --- | --- | --- |
| `query` | Top-level orchestration of the entire chatbot search flow. | Handles interpretation, criteria construction, retries, and fallbacks. |
| `determineLimit` | Sanitizes requested limits. | Caps at 200, defaults to 15. |
| `fetchVectorResults` | Runs vector similarity search and adapts chunks to DTOs. | Applies section filters directly in SQL and post-filters roles. |
| `runSearch` | Executes vector + metadata searches under combined context. | Calls both search paths and merges results. |
| `trySectionDiscovery` | Discovers additional section keys from page IDs. | Allows “ipad page” queries to yield multiple sections. |
| `fetchConsolidatedResults` | Fetches metadata results with optional section scoping. | Utilizes multiple repository helpers for section/context hits. |
| `collectRows` | Adds consolidated rows to a de-duped map. | Preserves order by insertion. |
| `mergeResults` | Joins vector + consolidated DTOs and enforces limit. | Delegates dedupe logic to `addResults`. |
| `addResults` | Inserts DTOs into a LinkedHashMap without duplicates. | Uses `buildResultKey` to prevent collisions. |
| `buildResultKey` / `safeKey` / `textSignature` | Creates a deterministic dedupe key per DTO. | Section path + content role + hash of cleansed text. |

### DTO Post-processing

| Method | Responsibility |
| --- | --- |
| `assignCfIds` | Applies sequential IDs and backfills inferred metadata. |
| `buildMatchTerms`, `addTerm` | Builds exposure metadata for UI highlighting. |
| `addNormalizedStrings` | Normalizes tag/keyword lists before filtering. |
| `matchesCriteria` | Filters DTOs by requested role, locale, country, and page. |
| `deriveLocale`, `deriveCountry`, `extractCountryFromPath` | Infer locale/country from DTO context or paths. |
| `mapSection` | Converts a `ConsolidatedEnrichedSection` into `ChatbotResultDto`. |
| `formatTimestamp` | ISO formatting helper. |
| `resolveTenant`, `resolvePageId`, `resolveLocale`, `resolveLanguage`, `resolveRole`, `resolveCountry`, `matchesRole` | Populate DTO metadata from stored context/path data. |

### Criteria & Context Handling

| Method | Responsibility |
| --- | --- |
| `buildCriteria` | Consolidates hints from request, interpretation, and regex parsing. |
| `buildDerivedContext` | Generates implied context (locale/country/page) from criteria. |
| `mergeContext`, `mergeContextValue`, `addToSet`, `copyValue` | Safe deep-merge utilities for JSON context. |
| `sanitizeContext`, `sanitizeContextValue` | Remove empty/blank context tokens. |

### Section & Role Parsing

| Method | Responsibility |
| --- | --- |
| `extractSectionKeyFromSection`, `extractSectionKey` | Determine section keys from context or user text. |
| `extractRole`, `sanitizeRoleCandidate`, `matchesRole` | Parse and evaluate requested roles. |
| `extractPageId` (overloads), `parseLocaleHints` | Pull page IDs from user text with locale-aware exclusions. |

### Normalization Helpers

| Method | Responsibility |
| --- | --- |
| `normalizeKey`, `slugify`, `normalizeRole`, `normalizePageId`, `normalizeLocale`, `normalizeLanguage`, `mapCountryAlias` | Standardize tokens for indexing/filtering. |
| `extractTenantFromPath`, `extractPageIdFromPath`, `extractLocaleFromPath` | Path parsing utilities. |
| `firstNonBlank`, `asStringFromContext`, `asMap`, `firstString` | Generic null-safe helpers for nested structures. |
| `isCountryToken`, `normalizeLanguageToken`, `sanitizeLanguageKey` | Recognize locale/country references. |
| `buildIsoLanguageCodes`, `buildLanguageIndex`, `addLanguageMapping` | Maintain language lookup tables. |
| `buildIsoCountryCodes`, `buildIso3ToIso2Index`, `buildCountryIndex`, `addCountryMapping`, `sanitizeCountryKey`, `mapCountryCode` | Maintain country lookup tables. |
| `firstOf` | Retrieves the first element from a set. |
| `LocaleHints` record | Stores parsed locale/language/country hints. |

### SearchCriteria Utilities

| Method | Responsibility |
| --- | --- |
| `SearchCriteria.embeddingQuery` | Chooses the string to embed (message or section). |
| `withoutRole`, `withSectionKey`, `withPageId`, `withoutContext` | Produce tweaked copies when constraints change. |
| `SearchCriteriaBuilder.sectionKey/role/locale/.../message` | Incrementally assemble criteria components. |
| `SearchCriteriaBuilder.build` | Emits the immutable record instance. |

---

## AiPromptSearchService Method Reference

| Method | Responsibility | Key Notes |
| --- | --- | --- |
| `aiSearch` | Orchestrates the AI prompt flow, calling Bedrock, vector search, and metadata search. | Applies AI-derived filters + heuristics before fallback logic. |
| `toDto` | Builds DTOs tagged as `ai-search`. | Adds tenant/page/locale metadata. |
| `loadPromptTemplate` | Loads `prompts/interactive_search_prompt.txt`. | Provides literal fallback if missing. |
| `readStrings` | Converts JSON nodes (scalar/array) to string lists. | Used for tags/keywords extraction. |
| `stripJsonFences` | Normalizes model output by removing ```json fences. | Prevents parse errors. |
| `extractSectionKey`, `normalizeKey` | Parse/normalize section keys from user text or AI JSON. |
| `inferRoleHint` | Guesses role names from phrases such as “headline for video section”. |
| `userExplicitlyRequestedRole` | Determines whether strict role filtering is warranted. |
| `pickBestRole` | Aligns requested roles with actual section roles. |
| `extractLocale`, `extractTenant`, `extractPageId` | High-level metadata extraction from consolidated sections. |
| `extractLocaleFromPath`, `extractTenantFromPath`, `extractPageIdFromPath` | Path parsing utilities reused by AI prompt search. |

---

## High-Level Comparison of Services

| Concern | ChatbotService | AiPromptSearchService |
| --- | --- | --- |
| Primary input | Structured `ChatbotRequest` + interpretation record. | Free-form prompt processed via Bedrock template. |
| Search strategy | Vector + metadata search with iterative relaxation. | AI filters → vector search (optional section scope) → metadata fallback. |
| Role filtering | Applies only when explicitly requested or derived. | Honors explicit user role; AI role hints are advisory unless confirmed. |
| Context handling | Deep merge between request, interpretation, and derived data. | Context is mostly provided by AI JSON with optional user overrides. |
| Deduplication | Section path + content role + cleansed text hash. | Vector vs. consolidated reconciliation happens before returning. |

Use this document alongside the inline Javadoc comments when adding new filters or debugging search behavior.
