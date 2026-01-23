package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ConsolidatedEnrichedSectionRepository extends JpaRepository<ConsolidatedEnrichedSection, UUID>, ConsolidatedEnrichedSectionRepositoryCustom {
 boolean existsBySectionUriAndSectionPathAndCleansedTextAndVersion(String sectionUri, String sectionPath, String cleansedText, Integer version);


 //boolean existsBySectionUriAndSectionPathAndCleansedText(String sectionUri, String sectionPath, String cleansedText);
 boolean existsBySectionUriAndSectionPathAndOriginalFieldNameAndCleansedTextAndVersion(String sectionUri, String sectionPath, String originalFieldName, String cleansedText, Integer version);
 Optional<ConsolidatedEnrichedSection> findBySectionPathAndOriginalFieldName(String sectionPath, String originalFieldName);

 Optional<ConsolidatedEnrichedSection> findBySourceUriAndVersionAndSectionPath(String sourceUri, Integer version, String sectionPath);

 List<ConsolidatedEnrichedSection> findAllByCleansedDataId(UUID cleansedDataId);

 List<ConsolidatedEnrichedSection> findAllByCleansedDataIdAndVersion(UUID cleansedDataId, Integer version);

    List<ConsolidatedEnrichedSection> findAllByCleansedDataIdAndVersionAndOriginalFieldNameAndSectionPathAndSectionUri(
            UUID cleansedDataId,
            Integer version,
            String originalFieldName,
            String sectionPath,
            String sectionUri
    );

 @Transactional
 @Modifying
 @Query("update ConsolidatedEnrichedSection s set s.status = :status where s.cleansedDataId = :cleansedDataId and (:version is null or s.version = :version)")
 int updateStatusForCleansedData(@Param("cleansedDataId") UUID cleansedDataId,
                                 @Param("version") Integer version,
                                 @Param("status") String status);

 @Transactional
 @Modifying
 @Query(value = """
             update consolidated_enriched_sections
             set status = :status
             where cleansed_data_id = :cleansedDataId
               and (:version is null or version = :version)
               and (cleansed_text is null or btrim(cleansed_text) = '')
             """, nativeQuery = true)
 int updateBlankSectionsStatus(@Param("cleansedDataId") UUID cleansedDataId,
                               @Param("version") Integer version,
                               @Param("status") String status);

 @Transactional
 @Modifying
 @Query("update ConsolidatedEnrichedSection s set s.status = :status where s.id = :sectionId")
 int updateStatusForSection(@Param("sectionId") UUID sectionId, @Param("status") String status);

 @Transactional
 @Query(value = """
             select *
             from consolidated_enriched_sections ces
             where ces.status = 'PENDING_EMBEDDING'
               and coalesce(nullif(btrim(ces.cleansed_text), ''), '') <> ''
               and not exists (
                   select 1 from content_chunks cc
                   where cc.consolidated_enriched_section_id = ces.id
               )
             order by ces.saved_at nulls last
             limit :limit
             for update skip locked
             """, nativeQuery = true)
 List<ConsolidatedEnrichedSection> lockSectionsMissingEmbeddings(@Param("limit") int limit);

 @Query(value = """
             select count(1)
             from consolidated_enriched_sections ces
             where ces.cleansed_data_id = :cleansedDataId
               and coalesce(nullif(btrim(ces.cleansed_text), ''), '') <> ''
               and not exists (
                   select 1 from content_chunks cc
                   where cc.consolidated_enriched_section_id = ces.id
               )
             """, nativeQuery = true)
 long countSectionsMissingEmbeddings(@Param("cleansedDataId") UUID cleansedDataId);
}