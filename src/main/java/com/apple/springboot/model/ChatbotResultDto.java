package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChatbotResultDto {
    @JsonProperty("section")
    private String section; // e.g., "video-section-header"

    @JsonProperty("cf_id")
    private String cfId; // e.g., "cf1", assigned sequentially

    @JsonProperty("section_path")
    private String sectionPath;

    @JsonProperty("section_uri")
    private String sectionUri;

    @JsonProperty("cleansed_text")
    private String cleansedText;

    // Also expose a "text" alias for clients that expect it
    @JsonProperty("text")
    public String getText() { return cleansedText; }

    @JsonProperty("source")
    private String source; // "consolidated_enriched_sections" or "content_chunks"

    @JsonProperty("content_role")
    private String contentRole; // maps from original_field_name

    // Optionals removed per requirements simplification

    @JsonProperty("last_modified")
    private String lastModified; // ISO timestamp if available

    @JsonProperty("match_terms")
    private java.util.List<String> matchTerms;
}
