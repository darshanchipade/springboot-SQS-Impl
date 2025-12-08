package com.apple.springboot.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Lightweight payload returned by /api/cleansed-context/{id}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CleansedContextResponse {

    private Metadata metadata;
    private Long startedAt;
    private List<StatusEntry> statusHistory = new ArrayList<>();
    private List<Map<String, Object>> items = Collections.emptyList();
    private String status;
    private String fallbackReason;

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Long getStartedAt() {
        return startedAt;
    }

    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    public List<StatusEntry> getStatusHistory() {
        return statusHistory;
    }

    public void setStatusHistory(List<StatusEntry> statusHistory) {
        this.statusHistory = statusHistory;
    }

    public List<Map<String, Object>> getItems() {
        return items;
    }

    public void setItems(List<Map<String, Object>> items) {
        this.items = items;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getFallbackReason() {
        return fallbackReason;
    }

    public void setFallbackReason(String fallbackReason) {
        this.fallbackReason = fallbackReason;
    }

    public static Metadata buildMetadata(UUID cleansedId,
                                         String sourceUri,
                                         String sourceType,
                                         Long uploadedAt,
                                         Integer version,
                                         Long sizeBytes) {
        Metadata metadata = new Metadata();
        metadata.setCleansedId(cleansedId != null ? cleansedId.toString() : null);
        metadata.setSource(sourceUri);
        metadata.setSourceType(sourceType);
        metadata.setUploadedAt(uploadedAt);
        metadata.setVersion(version);
        metadata.setSize(sizeBytes);
        return metadata;
    }

    public static StatusEntry buildStatusEntry(String status, OffsetDateTime timestamp) {
        StatusEntry entry = new StatusEntry();
        entry.setStatus(status);
        entry.setTimestamp(timestamp != null ? timestamp.toInstant().toEpochMilli() : System.currentTimeMillis());
        return entry;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Metadata {
        private String cleansedId;
        private String source;
        private String sourceType;
        private Long uploadedAt;
        private Integer version;
        private Long size;

        public String getCleansedId() {
            return cleansedId;
        }

        public void setCleansedId(String cleansedId) {
            this.cleansedId = cleansedId;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getSourceType() {
            return sourceType;
        }

        public void setSourceType(String sourceType) {
            this.sourceType = sourceType;
        }

        public Long getUploadedAt() {
            return uploadedAt;
        }

        public void setUploadedAt(Long uploadedAt) {
            this.uploadedAt = uploadedAt;
        }

        public Integer getVersion() {
            return version;
        }

        public void setVersion(Integer version) {
            this.version = version;
        }

        public Long getSize() {
            return size;
        }

        public void setSize(Long size) {
            this.size = size;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StatusEntry {
        private String status;
        private Long timestamp;

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
