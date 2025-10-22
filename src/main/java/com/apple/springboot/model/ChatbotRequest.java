package com.apple.springboot.model;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class ChatbotRequest {
    private String message;                 // Full user message
    private String sectionKey;              // Optional explicit section key
    private Integer limit;                  // Optional max results
    private String original_field_name;     // Role filter (strict in vector? we post-filter instead)
    private List<String> tags;              // Optional tag filters
    private List<String> keywords;          // Optional keyword filters
    private Map<String, Object> context;    // Optional JSONB context filters
}