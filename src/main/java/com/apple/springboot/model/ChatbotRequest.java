
package com.apple.springboot.model;

import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class ChatbotRequest {
    private String message;     // Full user message, e.g., "Hello... Need ... video-section-header"
    private String sectionKey;  // Optional explicit key, e.g., "video-section-header"
    private Integer limit;      // Optional max results
    private String original_field_name; // Optional filter to restrict to a field
    private List<String> tags;      // Optional tag filters
    private List<String> keywords;  // Optional keyword filters
    private Map<String, Object> context; // Optional JSONB context filters
}
