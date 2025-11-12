package com.apple.springboot.controller;

import com.apple.springboot.model.ChatbotRequest;
import com.apple.springboot.model.ChatbotResultDto;
import com.apple.springboot.service.ChatbotService;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping("/api/chatbot")
public class ChatbotController {

    private final ChatbotService chatbotService;

    public ChatbotController(ChatbotService chatbotService) {
        this.chatbotService = chatbotService;
    }

    @PostMapping("/query")
    public ResponseEntity<List<ChatbotResultDto>> chat(@RequestBody(required = false) ChatbotRequest request) {
        if (request == null || !StringUtils.hasText(request.getMessage())) {
            return ResponseEntity.badRequest().body(Collections.emptyList());
        }
        return ResponseEntity.ok(chatbotService.query(request));
    }

    // Placeholder: keep the bean wiring flexible if AI flow returns in the future.
//    @PostMapping("/query-ai")
//    public ResponseEntity<List<ChatbotResultDto>> aiQuery(@RequestBody ChatbotRequest request) {
//        ...
//    }
}