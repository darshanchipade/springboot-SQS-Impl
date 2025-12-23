package com.apple.springboot.model;

import jakarta.persistence.*;


import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Setter
@Getter
@Entity
@Table(name = "enriched_content_elements")
public class EnrichedContentElement {

    // Getters and Setters
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private java.util.UUID id;

    @Column(name = "cleansed_data_id")
    private java.util.UUID cleansedDataId;

    @Column(name = "source_uri", columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "item_source_path", columnDefinition = "TEXT")
    private String itemSourcePath;

    @Column(name = "item_original_field_name", columnDefinition = "TEXT")
    private String itemOriginalFieldName;

    @Column(name = "item_model_hint", columnDefinition = "TEXT")
    private String itemModelHint;

    @Column(name = "cleansed_text", columnDefinition = "TEXT")
    private String cleansedText;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "context", columnDefinition = "jsonb")
    private Map<String, Object> context;

    @Column(name = "summary", columnDefinition = "TEXT")
    private String summary;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "keywords")
    private List<String> keywords;

    @Column(name = "sentiment", columnDefinition = "TEXT")
    private String sentiment;

    @Column(name = "classification", columnDefinition = "TEXT")
    private String classification;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags")
    private List<String> tags;

    @Column(name = "bedrock_model_used", columnDefinition = "TEXT")
    private String bedrockModelUsed;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "enrichment_metadata", columnDefinition = "jsonb")
    private String enrichmentMetadata; // Store as String, expecting JSON formatted string

    @Column(name = "enriched_at")
    private OffsetDateTime enrichedAt;

    @Column(name = "status", columnDefinition = "TEXT")
    private String status;

    @Column(name = "version")
    private Integer version;

    // @Column(name = "content_hash")
    // private String contentHash;

    // Constructors
    public EnrichedContentElement() {
    }

}