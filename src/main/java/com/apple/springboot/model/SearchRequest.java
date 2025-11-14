package com.apple.springboot.model;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;
import java.util.Map;

@Schema(description = "Request model for vector search with optional filters")
public class SearchRequest {
    @Schema(description = "Search query string", required = true, example = "iPad setup")
    private String query;
    
    @Schema(description = "Optional list of tag filters")
    private List<String> tags;
    
    @Schema(description = "Optional list of keyword filters")
    private List<String> keywords;
    
    @Schema(description = "Optional context filters as key-value pairs")
    private Map<String, Object> context;
    
    @Schema(description = "Optional original field name filter", example = "copy")
    private String original_field_name;
    
    @Schema(description = "Optional section filter")
    private String sectionFilter;

    // Getters and setters
    public String getQuery() { return query; }
    public void setQuery(String query) { this.query = query; }
    public List<String> getTags() { return tags; }
    public void setTags(List<String> tags) { this.tags = tags; }
    public List<String> getKeywords() { return keywords; }
    public void setKeywords(List<String> keywords) { this.keywords = keywords; }
    public Map<String, Object> getContext() { return context; }
    public void setContext(Map<String, Object> context) { this.context = context; }
    public String getOriginal_field_name() { return original_field_name; }
    public void setOriginal_field_name(String original_field_name) { this.original_field_name = original_field_name; }
}