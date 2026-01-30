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
 /**
  * Checks for an existing consolidated section by path, text, and version.
  */
 boolean existsBySectionUriAndSectionPathAndCleansedTextAndVersion(String sectionUri, String sectionPath, String cleansedText, Integer version);


 //boolean existsBySectionUriAndSectionPathAndCleansedText(String sectionUri, String sectionPath, String cleansedText);
 /**
  * Checks for an existing consolidated section by field name, text, and version.
  */
 boolean existsBySectionUriAndSectionPathAndOriginalFieldNameAndCleansedTextAndVersion(String sectionUri, String sectionPath, String originalFieldName, String cleansedText, Integer version);
 /**
  * Finds a consolidated section by section path and original field name.
  */
 Optional<ConsolidatedEnrichedSection> findBySectionPathAndOriginalFieldName(String sectionPath, String originalFieldName);

 /**
  * Finds a consolidated section by source URI, version, and section path.
  */
 Optional<ConsolidatedEnrichedSection> findBySourceUriAndVersionAndSectionPath(String sourceUri, Integer version, String sectionPath);

 /**
  * Loads consolidated sections for a cleansed record.
  */
 List<ConsolidatedEnrichedSection> findAllByCleansedDataId(UUID cleansedDataId);

 /**
  * Loads consolidated sections for a cleansed record and version.
  */
 List<ConsolidatedEnrichedSection> findAllByCleansedDataIdAndVersion(UUID cleansedDataId, Integer version);

    /**
     * Loads consolidated sections by cleansed record, version, and section identity.
     */
    List<ConsolidatedEnrichedSection> findAllByCleansedDataIdAndVersionAndOriginalFieldNameAndSectionPathAndSectionUri(
            UUID cleansedDataId,
            Integer version,
            String originalFieldName,
            String sectionPath,
            String sectionUri
    );

 /**
  * Updates status for all sections within a cleansed record and optional version.
  */
 @Transactional
 @Modifying
 @Query("update ConsolidatedEnrichedSection s set s.status = :status where s.cleansedDataId = :cleansedDataId and (:version is null or s.version = :version)")
 int updateStatusForCleansedData(@Param("cleansedDataId") UUID cleansedDataId,
                                 @Param("version") Integer version,
                                 @Param("status") String status);

 /**
  * Updates status for sections with blank cleansed text.
  */
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

 /**
  * Updates status for a single consolidated section.
  */
 @Transactional
 @Modifying
 @Query("update ConsolidatedEnrichedSection s set s.status = :status where s.id = :sectionId")
 int updateStatusForSection(@Param("sectionId") UUID sectionId, @Param("status") String status);

 /**
  * Locks and returns sections missing embeddings for backfill.
  */
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

 /**
  * Counts sections that are missing embeddings for a cleansed record.
  */
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