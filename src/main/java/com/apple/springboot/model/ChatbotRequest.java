package com.apple.springboot.model;

import lombok.Data;

@Data
public class ChatbotRequest {
    private String message;     // Full user message, e.g., "Hello... Need ... video-section-header"
    private String sectionKey;  // Optional explicit key, e.g., "video-section-header"
    private Integer limit;      // Optional max results
}
