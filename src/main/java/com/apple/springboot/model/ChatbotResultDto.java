package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ChatbotResultDto {
    @JsonProperty("text")
    private String text;

    @JsonProperty("section_uri")
    private String sectionUri;

    @JsonProperty("section_path")
    private String sectionPath;
}
