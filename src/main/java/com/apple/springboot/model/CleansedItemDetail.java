package com.apple.springboot.model;

public class CleansedItemDetail {
    public final String sourcePath;
    public final String originalFieldName;
    public final String cleansedContent;
    public final String model;
    public final EnrichmentContext context;
    public final boolean skipEnrichment;

    /**
     * Builds a typed detail object for a cleansed item.
     */
    public CleansedItemDetail(String sourcePath, String originalFieldName, String cleansedContent, String model, EnrichmentContext context, boolean skipEnrichment) {
        this.sourcePath = sourcePath;
        this.originalFieldName = originalFieldName;
        this.cleansedContent = cleansedContent;
        this.model = model;
        this.context = context;
        this.skipEnrichment = skipEnrichment;
    }
}
