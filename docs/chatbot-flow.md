## Chatbot End-to-End Flow

### 1. High-Level Overview
- **Entry point**: `POST /api/chatbot/query` handled by `ChatbotController.chat(...)`.
- **AI orchestration**: `AiPromptSearchService.aiSearch(...)` uses the Bedrock prompt in `prompts/interactive_search_prompt.txt` to transform the user utterance into structured search hints (query, tags, keywords, role, context).
- **Retrieval layer**: semantic search via `VectorSearchService` (Postgres + pgvector) plus metadata lookups via `ConsolidatedEnrichedSectionRepository`.
- **Response shaping**: results are normalized into `ChatbotResultDto`, enriched with locale/country/language metadata, deduplicated, and ordered before being returned to the caller.

The legacy route `POST /api/chatbot/query-legacy` uses `ChatbotService` and skips the AI prompt step. It is useful for regression comparisons or during incidents when Bedrock is unavailable.

### 2. Request Lifecycle
1. **Validation**  
   Incoming requests must contain a non-empty `message`. Optional fields include `tags`, `keywords`, `context`, `limit`, and `original_field_name`.
2. **Prompt construction**  
   `AiPromptSearchService` loads `interactive_search_prompt.txt`, fills in the conversation history placeholders (empty in this service) and the `user_message`, and calls `BedrockEnrichmentService.invokeChatForText`.
3. **AI response parsing**  
   The Bedrock response is stripped of any Markdown fences and parsed into JSON. Failures to parse are ignored so the user message still drives the search.
4. **Filter synthesis**  
   - `query` defaults to the user message unless Bedrock refines it.  
   - `tags` / `keywords` merge AI output with request-provided filters.  
   - `context` merges AI-derived hints with `request.context`, preserving nested `envelope` values (locale, country, language).  
   - A `sectionKey` is inferred from AI context or directly from the user message via regex (`*-section*`).  
   - `roleHint` is taken from Bedrock (`original_field_name`) or inferred heuristically. The role filter is only enforced if the user explicitly requested it.
   - The effective `limit` defaults to 1000 unless the caller provides a lower number.
5. **Context enrichment**  
   `enrichContextWithLocale` derives `language` and `country` when `context.envelope.locale` is present but the other facets are absent.

### 3. Retrieval Strategies
#### 3.1 Vector Search (semantic content retrieval)
- `VectorSearchService.search(...)` generates an embedding with `BedrockEnrichmentService.generateEmbedding(...)` and executes `ContentChunkRepository.findSimilar(...)`.
- Filters applied:
  - `tags` / `keywords`: wrapped into Postgres array predicates `@>`.
  - `context`: translated into JSONB path filters.
  - `sectionKey` (when available): fuzzy match across `section_path`, `section_uri`, and context usage paths.
- Returns `ContentChunkWithDistance`. The distance is only used internally; the API returns cleansed content text.
- Post-processing:
  - Map each chunk’s parent `ConsolidatedEnrichedSection` into `ChatbotResultDto` with source `"content_chunks"`.
  - Optional role filter if `roleHint` was explicitly requested.

#### 3.2 Metadata Search (consolidated section retrieval)
- Section-aware branch (`sectionKey` present and results exist):
    1. Fetch by section key (`findBySectionKey` and `findByContextSectionKey`).
    2. Expand to related keys (`sectionKey + "-items"`, trimmed suffix variations).
    3. Aggregate in insertion order and dedupe by UUID.
    4. If the user explicitly asked for a role, filter to matching `original_field_name`; otherwise drop the role hint to avoid excluding data.
    5. Filter rows by the merged context hints (locale, country, language, etc.) so locale-specific queries only surface matching sections.
    6. Produce `ChatbotResultDto` with source `"consolidated_enriched_sections"`, assign sequential `cf_id`, and annotate `match_terms` with the section key and optionally the role.
- Fallback branch (no section key or no metadata hits):
  - Use the vector results; if none exist, fall back to metadata search by full-text (`findByMetadataQuery`).

### 4. Response Structure (`ChatbotResultDto`)
- `section`: section identifier or `"ai-search"` when no explicit key is available.
- `cf_id`: sequential ID (`cf1`, `cf2`, …) for UI referencing.
- `cleansed_text`: human-readable content.
- `section_path` / `section_uri`: original location in the CMS hierarchy.
- `content_role`: mirrors `original_field_name`.
- `source`: `"content_chunks"` or `"consolidated_enriched_sections"`.
- `match_terms`: ordered hints showing why the row matched (section keys, tags, roles, page IDs).
- `tenant`, `page_id`, `locale`, `country`, `language`: derived from context, path analysis, or locale inference.
- `last_modified`: ISO timestamp sourced from `saved_at`.

### 5. Search Variations & Expected Behaviour

| Variation | Trigger | Retrieval Behaviour | Result Highlights |
|-----------|---------|---------------------|-------------------|
| **Section-focused** | User says “Show chapter-nav-section content” | Section key extracted → metadata aggregation wins | `section` = detected key; `match_terms` includes the key; broad coverage across the section |
| **General question** | No section key, e.g., “What navigation copy exists for iPad?” | Vector search using embeddings + tags/keywords; fallback to metadata if vectors empty | `source` often `"content_chunks"`; `match_terms` include AI inferred keywords |
| **Role-specific** | User explicitly asks for a field, e.g., “Give me headline for hero-section” or `original_field_name` in request | Role filter applied post-retrieval when matches exist; otherwise role is dropped to avoid empty responses | `content_role` equals user hint; `match_terms` contains the role |
| **Locale / country constrained** | User mentions `en_CA` or Canada, or supplies context | Context map adds `envelope.locale` and derived `language`/`country`; both vector and metadata queries honour the filters | Results restricted to matching locale; response fields populated |
| **Keyword / tag filtered** | Request includes `tags` or `keywords` | Postgres `@>` filters restrict both vector and metadata hits | `match_terms` includes supplied tags/keywords |
| **No Bedrock response** | AI call fails or returns invalid JSON | Flow falls back to raw user message for all hints | Retrieval still works, albeit without AI refinements |
| **Legacy mode** | Call `POST /api/chatbot/query-legacy` | Skips Bedrock prompt; relies on regex inference inside `ChatbotService` | Lower recall for free-form questions; good for direct section key lookups |

### 6. Error Handling & Throttling
- Bedrock throttling raises `ThrottledException` so upstream callers can decide retry semantics.
- Any other exceptions inside `AiPromptSearchService` or repository layers return an empty list to the client. Consider surfacing detailed error codes if richer diagnostics are needed.

### 7. Related Services
- **Data ingestion** (`DataExtractionController`, `DataIngestionService`, `EnrichmentPipelineService`): populates the `consolidated_enriched_sections` and `content_chunks` tables consumed by the chatbot.
- **Standalone semantic search** (`POST /api/search`): exposes raw vector results via `SearchController` for tooling and QA. It shares `VectorSearchService` with the chatbot.
- **Refinement chips** (`GET /api/refine`): produces tag/keyword suggestions from top semantic matches to help build structured filters.

### 8. Implementation Touchpoints
- `ChatbotController` → `AiPromptSearchService` (`src/main/java/com/apple/springboot/controller/ChatbotController.java`)
- AI orchestration & retrieval logic (`src/main/java/com/apple/springboot/service/AiPromptSearchService.java`)
- Bedrock integration (`src/main/java/com/apple/springboot/service/BedrockEnrichmentService.java`)
- Semantic search (`src/main/java/com/apple/springboot/service/VectorSearchService.java`, `repository/ContentChunkRepositoryImpl.java`)
- Metadata search (`repository/ConsolidatedEnrichedSectionRepositoryImpl.java`)
- Result DTO (`src/main/java/com/apple/springboot/model/ChatbotResultDto.java`)

For troubleshooting, enable SQL logging to inspect the generated Postgres queries and verify filters (`spring.jpa.show-sql=true` in `application.properties`).
