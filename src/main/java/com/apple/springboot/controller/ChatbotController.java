package com.apple.springboot.controller;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.service.AiPromptSearchService;
import com.apple.springboot.service.ChatbotService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api/chatbot")
public class ChatbotController {

    private final ChatbotService chatbotService;              // keep if you want a legacy path
    private final AiPromptSearchService aiPromptSearchService;

    public ChatbotController(ChatbotService chatbotService,
                             AiPromptSearchService aiPromptSearchService) {
        this.chatbotService = chatbotService;
        this.aiPromptSearchService = aiPromptSearchService;
    }

    // Route existing clients to the AI-driven flow (uses interactive_search_prompt.txt)
    @PostMapping("/query")
    public ResponseEntity<List<ChatbotResultDto>> chat(@RequestBody(required = false) ChatbotRequest request) {
        if (request == null || !StringUtils.hasText(request.getMessage())) {
            return ResponseEntity.badRequest().body(Collections.emptyList());
        }
        return ResponseEntity.ok(aiPromptSearchService.aiSearch(request));
    }

    // Optional: explicit AI endpoint (same behavior)
    @PostMapping("/ai-search")
    public ResponseEntity<List<ChatbotResultDto>> aiSearch(@RequestBody ChatbotRequest request) {
        if (request == null || !StringUtils.hasText(request.getMessage())) {
            return ResponseEntity.badRequest().body(Collections.emptyList());
        }
        return ResponseEntity.ok(aiPromptSearchService.aiSearch(request));
    }

    // Optional: legacy behavior if you need it for comparison
    @PostMapping("/query-legacy")
    public ResponseEntity<List<ChatbotResultDto>> legacy(@RequestBody ChatbotRequest request) {
        if (request == null || !StringUtils.hasText(request.getMessage())) {
            return ResponseEntity.badRequest().body(Collections.emptyList());
        }
        return ResponseEntity.ok(chatbotService.query(request));
    }
}