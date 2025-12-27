package com.apple.springboot.repository;

import com.apple.springboot.model.EnrichedContentElement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface EnrichedContentElementRepository extends JpaRepository<EnrichedContentElement, UUID> {
    long countByCleansedDataId(UUID cleansedDataId);
    long countByCleansedDataIdAndStatusContaining(UUID cleansedDataId, String status);
    long countByCleansedDataIdAndStatusIgnoreCase(UUID cleansedDataId, String status);
    List<EnrichedContentElement> findAllByCleansedDataId(UUID cleansedDataId);
    boolean existsByItemSourcePathAndItemOriginalFieldNameAndStatus(String sourcePath, String fieldName, String status);
    boolean existsByItemSourcePathAndItemOriginalFieldNameAndCleansedText(String sourcePath, String fieldName, String cleansedText);
    boolean existsByItemSourcePathAndItemOriginalFieldNameAndContentHash(String sourcePath, String fieldName, String contentHash);
    boolean existsByCleansedDataIdAndItemSourcePathAndItemOriginalFieldNameAndCleansedText(UUID cleansedDataId, String sourcePath, String fieldName, String cleansedText);
    boolean existsByCleansedDataIdAndItemSourcePathAndItemOriginalFieldNameAndStatus(UUID cleansedDataId, String sourcePath, String fieldName, String status);

    boolean existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndContentHash(String sourceUri, String sourcePath, String fieldName, String contentHash);
    boolean existsBySourceUriAndItemSourcePathAndItemOriginalFieldNameAndCleansedText(String sourceUri, String sourcePath, String fieldName, String cleansedText);

    /**
     * Finds an existing element based on its logical key to prevent duplicates.
     * This is the correct method signature.
     */
    Optional<EnrichedContentElement> findByCleansedDataIdAndItemSourcePathAndItemOriginalFieldName(UUID cleansedDataId, String sourcePath, String fieldName);
    List<EnrichedContentElement> findByCleansedDataIdOrderByEnrichedAtAsc(UUID cleansedDataId);

    List<EnrichedContentElement> findAllBySourceUriAndVersion(String sourceUri, Integer version);

}