package com.apple.springboot.model;

import jakarta.persistence.*;
import lombok.Getter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Entity
@Table(name = "raw_data_store")
public class RawDataStore {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "source_uri", nullable = false, columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "raw_content_text", columnDefinition = "TEXT")
    private String rawContentText;

    @Column(name = "raw_content_binary", columnDefinition = "bytea")
    private byte[] rawContentBinary;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "source_metadata", columnDefinition = "jsonb")
    private String sourceMetadata; // Store as JSON string

    @Column(name = "received_at", nullable = false, updatable = false)
    private OffsetDateTime receivedAt;

    @Column(name = "status", nullable = false, columnDefinition = "TEXT")
    private String status;

    @Column(name = "content_hash", columnDefinition = "TEXT")
    private String contentHash;

    @Column(name = "source_content_type", columnDefinition = "TEXT")
    private String sourceContentType;

    @Column(name = "version")
    private Integer version = 1;

    @Column(name = "latest")
    private Boolean latest = true;

    /**
     * Default constructor for JPA.
     */
    public RawDataStore() {}

    /**
     * Sets the primary key ID.
     */
    public void setId(UUID id) { this.id = id; }

    /**
     * Sets the source URI associated with this payload.
     */
    public void setSourceUri(String sourceUri) { this.sourceUri = sourceUri; }

    /**
     * Sets the raw text payload.
     */
    public void setRawContentText(String rawContentText) { this.rawContentText = rawContentText; }

    /**
     * Sets the raw binary payload.
     */
    public void setRawContentBinary(byte[] rawContentBinary) { this.rawContentBinary = rawContentBinary; }

    /**
     * Sets the extracted source metadata JSON.
     */
    public void setSourceMetadata(String sourceMetadata) { this.sourceMetadata = sourceMetadata; }

    /**
     * Sets the timestamp when the payload was received.
     */
    public void setReceivedAt(OffsetDateTime receivedAt) { this.receivedAt = receivedAt; }

    /**
     * Sets the ingestion status for this payload.
     */
    public void setStatus(String status) { this.status = status; }

    /**
     * Returns the payload content hash.
     */
    public String getContentHash() { return contentHash; }

    /**
     * Sets the payload content hash.
     */
    public void setContentHash(String contentHash) { this.contentHash = contentHash; }

    /**
     * Returns the content type of the stored payload.
     */
    public String getSourceContentType() { return sourceContentType; }

    /**
     * Sets the content type of the stored payload.
     */
    public void setSourceContentType(String sourceContentType) { this.sourceContentType = sourceContentType; }


    /**
     * Returns the version number for this source URI.
     */
    public Integer getVersion() {
        return version;
    }

    /**
     * Updates the version number for this source URI.
     */
    public void setVersion(Integer version) {
        this.version = version;
    }

    /**
     * Returns whether this is the latest version.
     */
    public Boolean isLatest() {
        return latest;
    }

    /**
     * Marks whether this record is the latest version.
     */
    public void setLatest(Boolean latest) {
        this.latest = latest;
    }
}