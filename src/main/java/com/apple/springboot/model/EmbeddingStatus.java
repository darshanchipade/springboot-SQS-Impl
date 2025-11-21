package com.apple.springboot.model;

/**
 * Centralized status constants for the asynchronous embedding workflow.
 */
public final class EmbeddingStatus {

    private EmbeddingStatus() {
    }

    public static final String SECTION_PENDING = "PENDING_EMBEDDING";
    public static final String SECTION_IN_PROGRESS = "EMBEDDING_IN_PROGRESS";
    public static final String SECTION_EMBEDDED = "EMBEDDED";

}
