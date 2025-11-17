package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Schema(description = "Chatbot search result containing matched content and metadata")
public class ChatbotResultDto {
    @JsonProperty("section")
    @Schema(description = "Section identifier", example = "video-section-header")
    private String section; // e.g., "video-section-header"

    @JsonProperty("cf_id")
    @Schema(description = "Content fragment ID", example = "cf1")
    private String cfId; // e.g., "cf1", assigned sequentially

    @JsonProperty("section_path")
    @Schema(description = "Path to the section", example = "/en_US/ipad")
    private String sectionPath;

    @JsonProperty("section_uri")
    @Schema(description = "URI of the section", example = "https://example.com/section")
    private String sectionUri;

    @JsonProperty("cleansed_text")
    @Schema(description = "Cleansed text content of the matched section", example = "Learn how to set up your iPad")
    private String cleansedText;

    @JsonProperty("page_id")
    private String pageId; // derived from path/locale segment (e.g., ipad)

    @JsonProperty("tenant")
    private String tenant; // derived from path (e.g., applecom-cms)

    @JsonProperty("locale")
    private String locale; // e.g., en_US

    @JsonProperty("country")
    private String country;

    @JsonProperty("language")
    private String language;

    public <T> ChatbotResultDto(String originalFieldName, String s, String sectionPath, String sectionUri, String chunkText, String source, String originalFieldName1, int rank, double distance, String string, String lastModified, List<T> ts) {
    }

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

    @JsonProperty("context")
    private java.util.Map<String, Object> context;
}