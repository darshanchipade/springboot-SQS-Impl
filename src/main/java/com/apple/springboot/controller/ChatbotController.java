package com.apple.springboot.controller;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.service.ChatbotService;
import com.apple.springboot.service.AiPromptSearchService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/chatbot")
public class ChatbotController {
    private final ChatbotService chatbotService;
    private final AiPromptSearchService aiPromptSearchService;

    public ChatbotController(ChatbotService chatbotService, AiPromptSearchService aiPromptSearchService) {
        this.chatbotService = chatbotService;
        this.aiPromptSearchService = aiPromptSearchService;
    }

    @PostMapping("/query")
    public List<ChatbotResultDto> query(@RequestBody ChatbotRequest request) {
        return chatbotService.query(request);
    }

    @PostMapping("/ai-search")
    public List<ChatbotResultDto> aiSearch(@RequestBody ChatbotRequest request) {
        return aiPromptSearchService.aiSearch(request);
    }
}