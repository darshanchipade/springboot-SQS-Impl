package com.apple.springboot.model;

import java.util.List;
import java.util.Map;

public class SearchRequest {
    private String query;
    private List<String> tags;
    private List<String> keywords;
    private Map<String, Object> context;
    private String original_field_name;
    private  String sectionFilter;

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