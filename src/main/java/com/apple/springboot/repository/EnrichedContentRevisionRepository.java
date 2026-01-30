package com.apple.springboot.repository;

import com.apple.springboot.model.EnrichedContentRevision;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface EnrichedContentRevisionRepository extends JpaRepository<EnrichedContentRevision, UUID> {

    /**
     * Returns the maximum revision number for an element.
     */
    @Query("select max(r.revision) from EnrichedContentRevision r where r.enrichedContentElementId = :elementId")
    Integer findMaxRevisionForElement(@Param("elementId") UUID elementId);

    /**
     * Finds the most recent revision for an element.
     */
    Optional<EnrichedContentRevision> findFirstByEnrichedContentElementIdOrderByRevisionDesc(UUID elementId);

    /**
     * Loads all revisions for an element, newest first.
     */
    List<EnrichedContentRevision> findAllByEnrichedContentElementIdOrderByRevisionDesc(UUID elementId);
}
