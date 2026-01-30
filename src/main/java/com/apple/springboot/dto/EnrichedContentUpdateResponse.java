package com.apple.springboot.dto;

import com.apple.springboot.model.EnrichedContentElement;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public class EnrichedContentUpdateResponse {

    private EnrichedContentElement element;
    private RevisionSnapshot revision;
    private Map<String, Object> preview;

    /**
     * Default constructor for serialization frameworks.
     */
    public EnrichedContentUpdateResponse() {
    }

    /**
     * Creates a response with the updated element and revision snapshot.
     */
    public EnrichedContentUpdateResponse(EnrichedContentElement element, RevisionSnapshot revision) {
        this.element = element;
        this.revision = revision;
    }

    /**
     * Returns the updated enriched content element.
     */
    public EnrichedContentElement getElement() {
        return element;
    }

    /**
     * Sets the updated enriched content element.
     */
    public void setElement(EnrichedContentElement element) {
        this.element = element;
    }

    /**
     * Returns the revision snapshot associated with this response.
     */
    public RevisionSnapshot getRevision() {
        return revision;
    }

    /**
     * Sets the revision snapshot for this response.
     */
    public void setRevision(RevisionSnapshot revision) {
        this.revision = revision;
    }

    /**
     * Returns preview content when generation is run in draft mode.
     */
    public Map<String, Object> getPreview() {
        return preview;
    }

    /**
     * Sets preview content when generation is run in draft mode.
     */
    public void setPreview(Map<String, Object> preview) {
        this.preview = preview;
    }

    public static class RevisionSnapshot {
        private UUID id;
        private Integer revision;
        private String source;
        private String modelUsed;
        private OffsetDateTime createdAt;

        /**
         * Default constructor for serialization frameworks.
         */
        public RevisionSnapshot() {
        }

        /**
         * Creates a revision snapshot payload.
         */
        public RevisionSnapshot(UUID id, Integer revision, String source, String modelUsed, OffsetDateTime createdAt) {
            this.id = id;
            this.revision = revision;
            this.source = source;
            this.modelUsed = modelUsed;
            this.createdAt = createdAt;
        }

        /**
         * Returns the revision ID.
         */
        public UUID getId() {
            return id;
        }

        /**
         * Sets the revision ID.
         */
        public void setId(UUID id) {
            this.id = id;
        }

        /**
         * Returns the revision number.
         */
        public Integer getRevision() {
            return revision;
        }

        /**
         * Sets the revision number.
         */
        public void setRevision(Integer revision) {
            this.revision = revision;
        }

        /**
         * Returns the revision source (AI/USER/REGENERATE).
         */
        public String getSource() {
            return source;
        }

        /**
         * Sets the revision source (AI/USER/REGENERATE).
         */
        public void setSource(String source) {
            this.source = source;
        }

        /**
         * Returns the model identifier used for this revision.
         */
        public String getModelUsed() {
            return modelUsed;
        }

        /**
         * Sets the model identifier used for this revision.
         */
        public void setModelUsed(String modelUsed) {
            this.modelUsed = modelUsed;
        }

        /**
         * Returns the timestamp when the revision was created.
         */
        public OffsetDateTime getCreatedAt() {
            return createdAt;
        }

        /**
         * Sets the timestamp when the revision was created.
         */
        public void setCreatedAt(OffsetDateTime createdAt) {
            this.createdAt = createdAt;
        }
    }
}
