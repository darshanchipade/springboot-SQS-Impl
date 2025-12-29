package com.apple.springboot.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

/**
 * Persisted per-item hashes per sourceUri version.
 *
 * This is intentionally additive (does not replace {@code content_hashes}) so existing
 * consumers/pipelines remain unchanged.
 */
@Setter
@Getter
@Entity
@Table(name = "item_version_hashes")
@IdClass(ItemVersionHashId.class)
public class ItemVersionHash {

    @Id
    @Column(name = "source_uri", nullable = false, columnDefinition = "TEXT")
    private String sourceUri;

    @Id
    @Column(name = "version", nullable = false)
    private Integer version;

    @Id
    @Column(name = "source_path", nullable = false, columnDefinition = "TEXT")
    private String sourcePath;

    @Id
    @Column(name = "item_type", nullable = false, columnDefinition = "TEXT")
    private String itemType;

    @Id
    @Column(name = "usage_path", nullable = false, columnDefinition = "TEXT")
    private String usagePath;

    @Column(name = "content_hash", nullable = false, columnDefinition = "TEXT")
    private String contentHash;

    @Column(name = "context_hash", columnDefinition = "TEXT")
    private String contextHash;

    @Column(name = "created_at", nullable = false)
    private OffsetDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    public ItemVersionHash() {
    }

    public ItemVersionHash(String sourceUri,
                           Integer version,
                           String sourcePath,
                           String itemType,
                           String usagePath,
                           String contentHash,
                           String contextHash) {
        this.sourceUri = sourceUri;
        this.version = version;
        this.sourcePath = sourcePath;
        this.itemType = itemType;
        this.usagePath = usagePath;
        this.contentHash = contentHash;
        this.contextHash = contextHash;
    }

    @PrePersist
    void onCreate() {
        OffsetDateTime now = OffsetDateTime.now();
        if (createdAt == null) createdAt = now;
        updatedAt = now;
    }

    @PreUpdate
    void onUpdate() {
        updatedAt = OffsetDateTime.now();
    }
}

