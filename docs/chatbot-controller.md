## ChatbotController Flow Details

Namespace: `com.apple.springboot.controller.ChatbotController`

### 1. Responsibility
- Exposes the REST entry point for chatbot interactions under `/api/chatbot`.
- Validates incoming requests and delegates to the appropriate service:
  - **AI-driven path** (`AiPromptSearchService.aiSearch`) for production traffic.
  - **Legacy path** (`ChatbotService.query`) kept for compatibility and fallback scenarios (currently commented out).

### 2. Constructor Injection
- Dependencies:
  - `ChatbotService` – legacy retrieval logic.
  - `AiPromptSearchService` – orchestrates Bedrock prompt + vector/meta search pipeline.
- Spring injects both services, but only `aiPromptSearchService` is used in the default route; `chatbotService` exists so the legacy endpoint can be toggled on quickly if needed.

### 3. Active Endpoint
`POST /api/chatbot/query`

Flow:
1. **Request binding**  
   Body is deserialized into `ChatbotRequest`. The parameter is marked `required = false` so null bodies don’t cause 400 before custom validation.
2. **Validation guard**  
   If the body is `null` or `request.getMessage()` is blank, the controller returns `400 Bad Request` with an empty list payload.
3. **Delegation**  
   On success, the controller forwards the request to `aiPromptSearchService.aiSearch(request)` and returns `200 OK` with the resulting `List<ChatbotResultDto>`.

### 4. Optional / Commented Routes
- `POST /api/chatbot/query-legacy`: invokes `chatbotService.query(request)` after the same validation guard. Useful for regression comparisons or when Bedrock is unavailable. Currently commented out; uncommenting re-enables the endpoint.
- `POST /api/chatbot/ai-search`: alternate alias that also delegates to `aiPromptSearchService`. Commented out because `/query` already drives AI behaviour.

### 5. Error Handling
- The controller does not wrap downstream exceptions; it trusts the service layer:
  - `AiPromptSearchService` already handles Bedrock throttling (`ThrottledException`) and general failures (returns an empty list).
  - Any uncaught runtime exception propagates as a 500 unless Spring advice handles it elsewhere.

### 6. Request / Response Contracts
- **ChatbotRequest** (`src/main/java/com/apple/springboot/model/ChatbotRequest.java`)
  - `message` (required): user utterance.
  - `sectionKey`, `original_field_name`, `limit`, `tags`, `keywords`, `context`: optional filters that the downstream services understand.
- **ChatbotResultDto** (`src/main/java/com/apple/springboot/model/ChatbotResultDto.java`)
  - Contains section metadata, cleansed text, locale info, and diagnostic fields (`source`, `match_terms`, etc.).

### 7. Extension Points
- To add authentication/authorization: wrap the controller with Spring Security or custom interceptors.
- To add request-level logging or tracing: use `@Slf4j` inside the controller or register a `HandlerInterceptor`.
- To expose both AI and legacy flows side-by-side, uncomment the legacy route and adjust routing logic (e.g., new query parameters or headers) as needed.
