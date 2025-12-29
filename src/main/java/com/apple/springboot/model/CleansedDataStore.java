package com.apple.springboot.model;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@Entity
@Table(name = "cleansed_data_store")
public class CleansedDataStore {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(updatable = false, nullable = false)
    private UUID id;

    @Column(name = "raw_data_id", nullable = false)
    private UUID rawDataId;

    @Column(name = "source_uri", nullable = false, columnDefinition = "TEXT")
    private String sourceUri;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "cleansed_items", nullable = false, columnDefinition = "jsonb")
    private List<Map<String, Object>> cleansedItems;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "context", columnDefinition = "jsonb")
    private Map<String, Object> context;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "cleansing_errors", columnDefinition = "jsonb")
    private Map<String, Object> cleansingErrors;

    @Column(name = "cleansed_at", nullable = false, updatable = false)
    private OffsetDateTime cleansedAt;

    @Column(name = "status", nullable = false, columnDefinition = "TEXT")
    private String status;

    @Column(name = "version")
    private Integer version;

    /**
     * Expected number of items for enrichment completion for this CleansedDataStore run.
     *
     * This is persisted so that if the service restarts and loses in-memory completion tracking,
     * we can still determine when to finalize (especially important for delta runs where only a
     * subset of the total cleansed items are queued).
     */
    @Column(name = "enrichment_expected_count")
    private Integer enrichmentExpectedCount;

//    @Column(name = "content_hash")
//    private String contentHash;

    public CleansedDataStore() {}

}