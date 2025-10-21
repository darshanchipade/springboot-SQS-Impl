package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
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

    @JsonProperty("source")
    private String source; // "v_consolidated_sections" or "v_content_chunks"
}
