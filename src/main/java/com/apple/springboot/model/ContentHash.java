package com.apple.springboot.model;

import lombok.Getter;
import lombok.Setter;

import jakarta.persistence.*;

@Setter
@Getter
@Entity
@Table(name = "content_hashes")
@IdClass(ContentHashId.class)
public class ContentHash {

    @Id
    @Column(name = "source_path", nullable = false, columnDefinition = "TEXT")
    private String sourcePath;

    @Id
    @Column(name = "item_type", nullable = false, columnDefinition = "TEXT")
    private String itemType;

    // Not part of the PK to avoid intrusive DB PK changes; uniqueness enforced via index (see SQL below)
    @Column(name = "usage_path", nullable = false, columnDefinition = "TEXT")
    private String usagePath;

    @Column(name = "content_hash", nullable = false, columnDefinition = "TEXT")
    private String contentHash;

    @Column(name = "context_hash", columnDefinition = "TEXT")
    private String contextHash;

    public ContentHash() {
    }

    public ContentHash(String sourcePath, String itemType, String usagePath, String contentHash, String contextHash) {
        this.sourcePath = sourcePath;
        this.itemType = itemType;
        this.usagePath = usagePath;
        this.contentHash = contentHash;
        this.contextHash = contextHash;
    }

    // Backward-compat constructor if needed elsewhere
    public ContentHash(String sourcePath, String itemType, String contentHash, String contextHash) {
        this(sourcePath, itemType, null, contentHash, contextHash);
    }
}