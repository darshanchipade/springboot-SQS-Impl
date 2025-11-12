package com.apple.springboot.controller;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.service.AiPromptSearchService;
import com.apple.springboot.service.ChatbotService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api/chatbot")
@Tag(name = "Chatbot", description = "Chatbot API endpoints for AI-driven queries and search operations")
public class ChatbotController {

    private final ChatbotService chatbotService;              // keep if you want a legacy path
    private final AiPromptSearchService aiPromptSearchService;

    public ChatbotController(ChatbotService chatbotService,
                             AiPromptSearchService aiPromptSearchService) {
        this.chatbotService = chatbotService;
        this.aiPromptSearchService = aiPromptSearchService;
    }

    @Operation(
            summary = "AI-driven chatbot query",
            description = "Route existing clients to the AI-driven flow using interactive_search_prompt.txt. " +
                    "Performs an AI-powered search based on the user's message and optional filters."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved search results",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = ChatbotResultDto.class))),
            @ApiResponse(responseCode = "400", description = "Bad request - message is null or empty",
                    content = @Content)
    })
    @PostMapping("/query")
    public ResponseEntity<List<ChatbotResultDto>> chat(@RequestBody(required = false) ChatbotRequest request) {
        if (request == null || !StringUtils.hasText(request.getMessage())) {
            return ResponseEntity.badRequest().body(Collections.emptyList());
        }
        return ResponseEntity.ok(aiPromptSearchService.aiSearch(request));
    }

    // Optional: explicit AI endpoint (same behavior)
//    @PostMapping("/ai-search")
//    public ResponseEntity<List<ChatbotResultDto>> aiSearch(@RequestBody ChatbotRequest request) {
//        if (request == null || !StringUtils.hasText(request.getMessage())) {
//            return ResponseEntity.badRequest().body(Collections.emptyList());
//        }
//        return ResponseEntity.ok(aiPromptSearchService.aiSearch(request));
//    }

    // Optional: legacy behavior if you need it for comparison
//    @PostMapping("/query-legacy")
//    public ResponseEntity<List<ChatbotResultDto>> legacy(@RequestBody ChatbotRequest request) {
//        if (request == null || !StringUtils.hasText(request.getMessage())) {
//            return ResponseEntity.badRequest().body(Collections.emptyList());
//        }
//        return ResponseEntity.ok(chatbotService.query(request));
//    }
}