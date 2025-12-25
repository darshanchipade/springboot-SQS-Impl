package com.apple.springboot.dto;

/**
 * Request payload for updating a single cleansed item inside a CleansedDataStore record.
 *
 * NOTE: Enrichment change detection is based on stored {@code cleansedContent}, so updates must
 * refresh {@code cleansedContent} (and hashes) to be detected.
 */
public record CleansedItemUpdateRequest(
        String sourcePath,
        String fieldName,
        String originalValue,
        Boolean triggerEnrichment
) {
}

