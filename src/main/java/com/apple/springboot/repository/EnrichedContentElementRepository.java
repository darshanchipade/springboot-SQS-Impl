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
    boolean existsByItemUsagePathAndItemOriginalFieldNameAndStatus(String usagePath, String fieldName, String status);
    boolean existsByItemUsagePathAndItemOriginalFieldNameAndCleansedText(String usagePath, String fieldName, String cleansedText);
    boolean existsByCleansedDataIdAndItemUsagePathAndItemOriginalFieldNameAndCleansedText(UUID cleansedDataId, String usagePath, String fieldName, String cleansedText);
    boolean existsByCleansedDataIdAndItemUsagePathAndItemOriginalFieldNameAndStatus(UUID cleansedDataId, String usagePath, String fieldName, String status);

    /**
     * Finds an existing element based on its logical key to prevent duplicates.
     * This is the correct method signature.
     */
    Optional<EnrichedContentElement> findByCleansedDataIdAndItemUsagePathAndItemOriginalFieldName(UUID cleansedDataId, String usagePath, String fieldName);
    List<EnrichedContentElement> findByCleansedDataIdOrderByEnrichedAtAsc(UUID cleansedDataId);

}