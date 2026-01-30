package com.apple.springboot.repository;

import com.apple.springboot.model.EnrichedContentElement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface EnrichedContentElementRepository extends JpaRepository<EnrichedContentElement, UUID> {
    /**
     * Counts elements for a cleansed data record.
     */
    long countByCleansedDataId(UUID cleansedDataId);
    /**
     * Loads all elements for a cleansed data record.
     */
    List<EnrichedContentElement> findAllByCleansedDataId(UUID cleansedDataId);
    /**
     * Checks for an element by logical key and cleansed text.
     */
    boolean existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(String sourcePath, String fieldName, String cleansedText);
    /**
     * Checks for an element by logical key and content hash.
     */
    boolean existsByItemSourcePathAndItemOriginalFieldNameAndContentHash(String sourcePath, String fieldName, String contentHash);

    /**
     * Checks for an element by source URI, key, and content hash.
     */
    boolean existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndContentHash(String sourceUri, String sourcePath, String fieldName, String contentHash);
    /**
     * Checks for an element by source URI, key, and cleansed text.
     */
    boolean existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndCleansedText(String sourceUri, String sourcePath, String fieldName, String cleansedText);

    /**
     * Finds an existing element based on its logical key to prevent duplicates.
     * This is the correct method signature.
     */
    Optional<EnrichedContentElement> findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(UUID cleansedDataId, String sourcePath, String fieldName);
    /**
     * Loads elements ordered by enrichment time.
     */
    List<EnrichedContentElement> findByCleansedDataIdOrderByEnrichedAtAsc(UUID cleansedDataId);

    /**
     * Loads elements for a source URI and version.
     */
    List<EnrichedContentElement> findAllBySourceUriAndVersion(String sourceUri, Integer version);

}