package com.apple.springboot.model;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Schema(description = "Request model for vector search with optional filters")
public class SearchRequest {
    // Getters and setters
    @Setter
    @Getter
    @Schema(description = "Search query string", required = true, example = "iPad setup")
    private String query;
    
    @Getter
    @Setter
    @Schema(description = "Optional list of tag filters")
    private List<String> tags;
    
    @Getter
    @Setter
    @Schema(description = "Optional list of keyword filters")
    private List<String> keywords;
    
    @Setter
    @Getter
    @Schema(description = "Optional context filters as key-value pairs")
    private Map<String, Object> context;
    
    @Getter
    @Setter
    @Schema(description = "Optional original field name filter", example = "copy")
    private String original_field_name;
    
    @Schema(description = "Optional section filter")
    private String sectionFilter;

}