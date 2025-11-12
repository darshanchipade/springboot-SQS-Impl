package com.apple.springboot.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
@Schema(description = "Request model for chatbot queries with optional filters")
public class ChatbotRequest {
    @Schema(description = "Full user message/query", required = true, example = "How do I set up my iPad?")
    private String message;                 // Full user message
    
    @Schema(description = "Optional explicit section key", example = "video-section-header")
    private String sectionKey;              // Optional explicit section key
    
    @Schema(description = "Optional maximum number of results to return", example = "10")
    private Integer limit;                  // Optional max results
    
    @Schema(description = "Optional role filter (original field name)", example = "copy")
    private String original_field_name;     // Role filter (strict in vector? we post-filter instead)
    
    @Schema(description = "Optional list of tag filters")
    private List<String> tags;              // Optional tag filters
    
    @Schema(description = "Optional list of keyword filters")
    private List<String> keywords;          // Optional keyword filters
    
    @Schema(description = "Optional JSONB context filters")
    private Map<String, Object> context;    // Optional JSONB context filters
}