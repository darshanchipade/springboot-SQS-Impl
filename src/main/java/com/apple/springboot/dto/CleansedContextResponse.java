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

    /**
     * Returns metadata for the cleansed record.
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Sets metadata for the cleansed record.
     */
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    /**
     * Returns the start timestamp in epoch milliseconds.
     */
    public Long getStartedAt() {
        return startedAt;
    }

    /**
     * Sets the start timestamp in epoch milliseconds.
     */
    public void setStartedAt(Long startedAt) {
        this.startedAt = startedAt;
    }

    /**
     * Returns the status history entries.
     */
    public List<StatusEntry> getStatusHistory() {
        return statusHistory;
    }

    /**
     * Sets the status history entries.
     */
    public void setStatusHistory(List<StatusEntry> statusHistory) {
        this.statusHistory = statusHistory;
    }

    /**
     * Returns the cleansed items payload.
     */
    public List<Map<String, Object>> getItems() {
        return items;
    }

    /**
     * Sets the cleansed items payload.
     */
    public void setItems(List<Map<String, Object>> items) {
        this.items = items;
    }

    /**
     * Returns the current processing status.
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the current processing status.
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * Returns the fallback reason, if any.
     */
    public String getFallbackReason() {
        return fallbackReason;
    }

    /**
     * Sets the fallback reason when a fallback is used.
     */
    public void setFallbackReason(String fallbackReason) {
        this.fallbackReason = fallbackReason;
    }

    /**
     * Builds a metadata payload for the cleansed response.
     */
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

    /**
     * Builds a status entry from a status string and timestamp.
     */
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

        /**
         * Returns the cleansed record ID.
         */
        public String getCleansedId() {
            return cleansedId;
        }

        /**
         * Sets the cleansed record ID.
         */
        public void setCleansedId(String cleansedId) {
            this.cleansedId = cleansedId;
        }

        /**
         * Returns the source URI.
         */
        public String getSource() {
            return source;
        }

        /**
         * Sets the source URI.
         */
        public void setSource(String source) {
            this.source = source;
        }

        /**
         * Returns the source type descriptor.
         */
        public String getSourceType() {
            return sourceType;
        }

        /**
         * Sets the source type descriptor.
         */
        public void setSourceType(String sourceType) {
            this.sourceType = sourceType;
        }

        /**
         * Returns the upload timestamp in epoch milliseconds.
         */
        public Long getUploadedAt() {
            return uploadedAt;
        }

        /**
         * Sets the upload timestamp in epoch milliseconds.
         */
        public void setUploadedAt(Long uploadedAt) {
            this.uploadedAt = uploadedAt;
        }

        /**
         * Returns the version number.
         */
        public Integer getVersion() {
            return version;
        }

        /**
         * Sets the version number.
         */
        public void setVersion(Integer version) {
            this.version = version;
        }

        /**
         * Returns the payload size in bytes.
         */
        public Long getSize() {
            return size;
        }

        /**
         * Sets the payload size in bytes.
         */
        public void setSize(Long size) {
            this.size = size;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class StatusEntry {
        private String status;
        private Long timestamp;

        /**
         * Returns the status string.
         */
        public String getStatus() {
            return status;
        }

        /**
         * Sets the status string.
         */
        public void setStatus(String status) {
            this.status = status;
        }

        /**
         * Returns the status timestamp in epoch milliseconds.
         */
        public Long getTimestamp() {
            return timestamp;
        }

        /**
         * Sets the status timestamp in epoch milliseconds.
         */
        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }
}
