package com.apple.springboot.model;

import java.util.Map;
import java.util.Objects;
import lombok.Data;
import lombok.AllArgsConstructor;
import java.util.Map;

@Data // Includes Getters, Setters, toString, equals, hashCode
@AllArgsConstructor // Creates the all-arguments constructor
public class EnrichedExtractedContentOutput {
    private String sourcePath;
    private String originalFieldName;
    private String cleansedContent;
    private String model;
    private Map<String, Object> bedrockEnrichments;
}