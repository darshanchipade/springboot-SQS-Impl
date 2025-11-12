## ChatbotController Flow Details

Namespace: `com.apple.springboot.controller.ChatbotController`

### 1. Responsibility
- Exposes the REST entry point for chatbot interactions under `/api/chatbot`.
- Validates incoming requests and delegates to `ChatbotService` for semantic + metadata retrieval.
- Keeps an optional placeholder for reintroducing AI-assisted search, but that path is no longer active.

### 2. Constructor Injection
- Only `ChatbotService` is injected. Removing `AiPromptSearchService` from the signature ensures `/api/chatbot/query` is AI-free by default.

### 3. Active Endpoint
`POST /api/chatbot/query`

Flow:
1. **Request binding**  
   Body is deserialized into `ChatbotRequest`. The parameter is marked `required = false` so the controller can return a friendly 400 instead of letting Spring reject the call.
2. **Validation guard**  
   If the body is `null` or `request.getMessage()` is blank, the controller returns `400 Bad Request` with an empty list payload.
3. **Delegation**  
   On success, the controller forwards the request to `chatbotService.query(request)` and returns `200 OK` with the resulting `List<ChatbotResultDto>`.

### 4. Optional / Future Routes
- A commented `@PostMapping("/query-ai")` stub is kept as a reminder of where an AI-enhanced route could live if the Bedrock prompt is reintroduced.

### 5. Error Handling
- The controller relies on `ChatbotService` to catch and normalise downstream issues. Any uncaught runtime exception propagates as a 500 unless a global exception handler overrides it.

### 6. Request / Response Contracts
- **ChatbotRequest** (`src/main/java/com/apple/springboot/model/ChatbotRequest.java`)
  - `message` (required): user utterance.
  - `sectionKey`, `original_field_name`, `limit`, `tags`, `keywords`, `context`: optional filters that the downstream services understand.
- **ChatbotResultDto** (`src/main/java/com/apple/springboot/model/ChatbotResultDto.java`)
  - Contains section metadata, cleansed text, locale info, and diagnostic fields (`source`, `match_terms`, etc.).

### 7. Extension Points
- To add authentication/authorization: wrap the controller with Spring Security or custom interceptors.
- To add request-level logging or tracing: use `@Slf4j` inside the controller or register a `HandlerInterceptor`.
- To expose both AI and semantic-only flows side-by-side, reintroduce a dedicated `/query-ai` mapping and inject `AiPromptSearchService` as needed.
