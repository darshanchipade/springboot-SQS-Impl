package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.ConsolidatedEnrichedSection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ContentChunkRepository extends JpaRepository<ContentChunk, UUID>, ContentChunkRepositoryCustom {
    /**
     * Finds a content chunk by section and chunk text.
     */
    Optional<ContentChunk> findByConsolidatedEnrichedSectionAndChunkText(ConsolidatedEnrichedSection section, String chunkText);

    /**
     * Deletes content chunks belonging to a cleansed record.
     */
    @Transactional
    @Modifying
    @Query("delete from ContentChunk cc where cc.consolidatedEnrichedSection.cleansedDataId = :cleansedDataId")
    void deleteByCleansedDataId(@Param("cleansedDataId") UUID cleansedDataId);
}
