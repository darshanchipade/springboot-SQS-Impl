package com.apple.springboot.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

import jakarta.persistence.ElementCollection;
import jakarta.persistence.FetchType;

@Entity
@Setter
@Getter
@Table(name = "consolidated_enriched_sections")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConsolidatedEnrichedSection {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "cleansed_data_id", nullable = false)
    private UUID cleansedDataId;

    @Column(name = "original_field_name")
    private String originalFieldName;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "enrichment_metadata", columnDefinition = "jsonb")
    private String enrichmentMetadata;

    @Column(name = "enriched_at")
    private OffsetDateTime enrichedAt;

    @Column(name = "source_uri", columnDefinition = "TEXT")
    private String sourceUri;

    @Column(name = "section_path")
    private String sectionPath;

    @Column(name = "summary", columnDefinition = "TEXT")
    private String summary;

    @Column(name = "classification")
    private String classification;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags")
    private java.util.List<String> tags;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "keywords")
    private java.util.List<String> keywords;

    @Column(name = "cleansed_text", columnDefinition = "TEXT")
    private String cleansedText;

    @Column(name = "sentiment")
    private String sentiment;

    @Column(name = "section_uri", columnDefinition = "TEXT")
    private String sectionUri;

    @Column(name = "model_used")
    private String modelUsed;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "context", columnDefinition = "jsonb")
    private Map<String, Object> context;

    @Column(name = "status")
    private String status;

    @Column(name = "saved_at")
    private OffsetDateTime savedAt;

    @Column(name = "version")
    private Integer version;

    @Column(name = "content_hash")
    private String contentHash;


}
