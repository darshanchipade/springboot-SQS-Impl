## Chatbot End-to-End Flow

### 1. High-Level Overview
- **Entry point**: `POST /api/chatbot/query` handled by `ChatbotController`, delegating directly to `ChatbotService.query(...)`.
- **Retrieval layer**: semantic search via `VectorSearchService` (Postgres + pgvector) combined with metadata lookups through `ConsolidatedEnrichedSectionRepository`.
- **Response shaping**: results are normalized into `ChatbotResultDto`, enriched with locale/country/language metadata, deduplicated, and ordered before being returned to the caller.
- **Optional AI flow**: `AiPromptSearchService` and the Bedrock prompt remain in the codebase, but are off the main path. They can be re-enabled if automatic tagging/query enrichment is needed again.

### 2. Request Lifecycle (`ChatbotService.query`)
1. **Validation**  
   The controller rejects requests with a missing or blank `message`. Optional fields include `tags`, `keywords`, `context`, `limit`, and `original_field_name`.
2. **Filter synthesis from user input**  
   - `sectionKey`: taken from `request.sectionKey` or inferred from the `message` via regex (`*-section*`).  
   - `tags` / `keywords`: taken verbatim from the request when present; otherwise remain empty.  
   - `context`: deep-copied from the request (e.g., `{ "envelope": { "locale": "en_US" } }`).  
   - `original_field_name`: used as a role filter when supplied.
3. **Locale enrichment**  
   `enrichContextWithLocale` derives `language` and `country` fields when only a locale is present. These derived fields are marked optional so the search still matches rows that encode locale information on the path instead of the JSON context.
4. **Embedding construction**  
   The message (plus any explicit section key) forms the text passed to `VectorSearchService`, which generates an embedding via `BedrockEnrichmentService.generateEmbedding(...)`.
5. **Vector search**  
   `ContentChunkRepository.findSimilar(...)` retrieves the top content chunks within the limit, honoring tags, keywords, JSON context filters, and optional section-key fuzzy matches.
6. **Metadata search**  
   In parallel, `ConsolidatedEnrichedSectionRepository` is queried:
   - Section-aware branch fetches by section key, context section key, and related key variants.
   - Fallback branch performs metadata full-text search when no section key is available.
   Results are filtered against the same context/tag/role criteria used for vector hits.
7. **Result merging**  
   Vector-derived rows are merged with consolidated rows, deduplicated by `(section_path, content_role)`, limited to the requested size, and assigned sequential `cf_id` values.

### 3. Retrieval Strategies in Detail
#### 3.1 Semantic Content (`content_chunks`)
- Embedding search uses pgvector similarity (`<=>`) to rank chunks.
- Filters applied:
  - `tags` / `keywords`: Postgres array containment (`@>`).
  - `context`: translated into JSONB path comparisons.
  - `sectionKey`: fuzzy LIKE checks against section path, URI, and usage paths.
- Post-processing maps each chunk’s parent section into a `ChatbotResultDto` and optionally filters on `original_field_name`.

#### 3.2 Metadata (`consolidated_enriched_sections`)
- Section-aware branch:
  1. Fetch by section key and usage-path matches.
  2. Expand to related keys (`sectionKey + "-items"`, base names, etc.).
  3. Deduplicate by UUID while preserving insertion order.
  4. Apply role filter only when the caller explicitly requested one.
  5. Re-check locale/language/country against the enriched context.
- Fallback branch:
  - When no section key is detected, fall back to metadata full-text search (`findByMetadataQuery`) for summaries, tags, keywords, and usage paths.

### 4. Response Structure (`ChatbotResultDto`)
- `section`: the section key or identifier associated with the match.
- `cf_id`: sequential ID (`cf1`, `cf2`, …) for UI referencing.
- `cleansed_text`: human-readable content.
- `section_path` / `section_uri`: original CMS location.
- `content_role`: mirrors `original_field_name`.
- `source`: `"content_chunks"` or `"consolidated_enriched_sections"`.
- `match_terms`: ordered hints showing why the row matched (section keys, tags, roles, page IDs).
- `tenant`, `page_id`, `locale`, `country`, `language`: derived from context, path analysis, or locale inference.
- `last_modified`: ISO timestamp sourced from `saved_at`.

### 5. Search Variations & Expected Behaviour

| Variation | Trigger | Retrieval Behaviour | Result Highlights |
|-----------|---------|---------------------|-------------------|
| **Section-focused** | “Show chapter-nav-section content” | Section key extracted → metadata aggregation wins | `section` = detected key; `match_terms` includes the key; broad coverage |
| **General question** | No section key | Embedding search using the message, with metadata fallback | `source` often `"content_chunks"`; `match_terms` reflect section key inference |
| **Role-specific** | `original_field_name` provided | Post-filter both vectors and metadata on the role | `content_role` equals the requested role |
| **Locale / country constrained** | Context provides locale/country | JSONB filters and derived locale checks restrict results | Response fields populated with locale metadata |
| **Keyword / tag filtered** | Request includes tags/keywords | Array filters apply to both vector and metadata paths | `match_terms` includes supplied tags/keywords |

### 6. Related Services
- **Data ingestion** (`DataExtractionController`, `DataIngestionService`, `EnrichmentPipelineService`): populates the tables used by the chatbot.
- **Standalone semantic search** (`POST /api/search`): exposes raw vector matches via `SearchController`, sharing `VectorSearchService`.
- **Refinement chips** (`GET /api/refine`): generates tags/keywords from top semantic matches to help users craft structured filters.

### 7. Implementation Touchpoints
- `ChatbotController` → `ChatbotService` (`src/main/java/com/apple/springboot/controller/ChatbotController.java`)
- Semantic retrieval logic (`src/main/java/com/apple/springboot/service/ChatbotService.java`)
- Embedding generation & vector search (`src/main/java/com/apple/springboot/service/VectorSearchService.java`, `repository/ContentChunkRepositoryImpl.java`)
- Metadata search (`repository/ConsolidatedEnrichedSectionRepositoryImpl.java`)
- Result DTO (`src/main/java/com/apple/springboot/model/ChatbotResultDto.java`)
- Optional AI helpers (`AiPromptSearchService`, `BedrockEnrichmentService`) remain available for future experimentation.

For troubleshooting, enable SQL logging to inspect generated queries and verify filters (`spring.jpa.show-sql=true` in `application.properties`).
