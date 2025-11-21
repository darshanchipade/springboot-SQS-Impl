package com.apple.springboot.model;

import jakarta.persistence.*;
import jakarta.persistence.SqlResultSetMapping;
import jakarta.persistence.EntityResult;
import jakarta.persistence.ColumnResult;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.time.OffsetDateTime;
import java.util.UUID;

@SqlResultSetMapping(
        name = "ContentChunkWithDistanceMapping",
        entities = @EntityResult(
                entityClass = ContentChunk.class
        ),
        columns = @ColumnResult(name = "distance", type = Double.class)
)
@Entity
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(
        name = "content_chunks",
        uniqueConstraints = @UniqueConstraint(
                name = "uk_content_chunk_section_text",
                columnNames = {"consolidated_enriched_section_id", "chunk_text"}
        )
)
public class ContentChunk {

    @Id
    @GeneratedValue
    private UUID id;

//    @Version
//    @Column(name = "row_version", nullable = false)
//    private long version;
//@Builder.Default @Column(name = "row_version", nullable = false) private Long rowVersion = 0L;

    @ManyToOne
    @JoinColumn(name = "consolidated_enriched_section_id", nullable = false)
    private ConsolidatedEnrichedSection consolidatedEnrichedSection;

    @Column(name = "chunk_text", columnDefinition = "TEXT")
    private String chunkText;

    private OffsetDateTime createdAt;

    private String createdBy;

    @Column(name = "source_field")
    private String sourceField;

    @Column(name = "section_path")
    private String sectionPath;

    @JdbcTypeCode(SqlTypes.VECTOR)
    @Array(length = 1024)
    @Column(name = "vector", columnDefinition = "vector(1024)")
    private float[] vector;
}