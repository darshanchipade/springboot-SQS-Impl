package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;
import java.util.Map;

@Data
public class Envelope {
    @JsonProperty("usagePath")
    private String usagePath;

    @JsonProperty("pathHierarchy")
    private List<String> pathHierarchy;

    @JsonProperty("sourcePath")
    private String sourcePath;

    @JsonProperty("model")
    private String model;

    @JsonProperty("locale")
    private String locale;

    @JsonProperty("language")
    private String language;

    @JsonProperty("country")
    private String country;

    @JsonProperty("provenance")
    private Map<String, String> provenance;

    @JsonProperty("sectionName")
    private String sectionName;

}
