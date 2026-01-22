package com.apple.springboot.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "enriched_content_revisions")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EnrichedContentRevision {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "enriched_content_element_id", nullable = false)
    private UUID enrichedContentElementId;

    @Column(name = "cleansed_data_id")
    private UUID cleansedDataId;

    @Column(name = "revision", nullable = false)
    private Integer revision;

    @Column(name = "summary", columnDefinition = "TEXT")
    private String summary;

    @Column(name = "classification", columnDefinition = "TEXT")
    private String classification;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "keywords")
    private List<String> keywords;

    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "tags")
    private List<String> tags;

    @Column(name = "source", nullable = false)
    private String source;

    @Column(name = "model_used")
    private String modelUsed;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata", columnDefinition = "jsonb")
    private Map<String, Object> metadata;

    @Column(name = "created_at")
    private OffsetDateTime createdAt;

    @PrePersist
    void onCreate() {
        if (createdAt == null) {
            createdAt = OffsetDateTime.now();
        }
    }
}
