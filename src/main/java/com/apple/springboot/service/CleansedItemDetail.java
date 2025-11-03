package com.apple.springboot.service;

import com.apple.springboot.model.EnrichmentContext;

public class CleansedItemDetail {
    public final String sourcePath;
    public final String originalFieldName;
    public final String cleansedContent;
    public final String model;
    public final EnrichmentContext context;
    public final String usagePath;
    public final String contentHash;
    public final String contextHash;

    public CleansedItemDetail(String sourcePath,
                              String originalFieldName,
                              String cleansedContent,
                              String model,
                              EnrichmentContext context,
                              String usagePath,
                              String contentHash,
                              String contextHash) {
        this.sourcePath = sourcePath;
        this.originalFieldName = originalFieldName;
        this.cleansedContent = cleansedContent;
        this.model = model;
        this.context = context;
        this.usagePath = usagePath;
        this.contentHash = contentHash;
        this.contextHash = contextHash;
    }
}
