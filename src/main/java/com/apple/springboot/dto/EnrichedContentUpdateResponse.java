package com.apple.springboot.dto;

import com.apple.springboot.model.EnrichedContentElement;

import java.time.OffsetDateTime;
import java.util.UUID;

public class EnrichedContentUpdateResponse {

    private EnrichedContentElement element;
    private RevisionSnapshot revision;

    public EnrichedContentUpdateResponse() {
    }

    public EnrichedContentUpdateResponse(EnrichedContentElement element, RevisionSnapshot revision) {
        this.element = element;
        this.revision = revision;
    }

    public EnrichedContentElement getElement() {
        return element;
    }

    public void setElement(EnrichedContentElement element) {
        this.element = element;
    }

    public RevisionSnapshot getRevision() {
        return revision;
    }

    public void setRevision(RevisionSnapshot revision) {
        this.revision = revision;
    }

    public static class RevisionSnapshot {
        private UUID id;
        private Integer revision;
        private String source;
        private String modelUsed;
        private OffsetDateTime createdAt;

        public RevisionSnapshot() {
        }

        public RevisionSnapshot(UUID id, Integer revision, String source, String modelUsed, OffsetDateTime createdAt) {
            this.id = id;
            this.revision = revision;
            this.source = source;
            this.modelUsed = modelUsed;
            this.createdAt = createdAt;
        }

        public UUID getId() {
            return id;
        }

        public void setId(UUID id) {
            this.id = id;
        }

        public Integer getRevision() {
            return revision;
        }

        public void setRevision(Integer revision) {
            this.revision = revision;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getModelUsed() {
            return modelUsed;
        }

        public void setModelUsed(String modelUsed) {
            this.modelUsed = modelUsed;
        }

        public OffsetDateTime getCreatedAt() {
            return createdAt;
        }

        public void setCreatedAt(OffsetDateTime createdAt) {
            this.createdAt = createdAt;
        }
    }
}