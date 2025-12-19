package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CleansedItemDetail {
    public final String sourcePath;
    public final String usagePath;
    public final String originalFieldName;
    public final String cleansedContent;
    public final String model;
    public final EnrichmentContext context;
    public final boolean skipEnrichment;

    @JsonCreator
    public CleansedItemDetail(@JsonProperty("sourcePath") String sourcePath,
                             @JsonProperty("usagePath") String usagePath,
                             @JsonProperty("originalFieldName") String originalFieldName,
                             @JsonProperty("cleansedContent") String cleansedContent,
                             @JsonProperty("model") String model,
                             @JsonProperty("context") EnrichmentContext context,
                             @JsonProperty("skipEnrichment") boolean skipEnrichment) {
        this.sourcePath = sourcePath;
        this.usagePath = usagePath;
        this.originalFieldName = originalFieldName;
        this.cleansedContent = cleansedContent;
        this.model = model;
        this.context = context;
        this.skipEnrichment = skipEnrichment;
    }
}
