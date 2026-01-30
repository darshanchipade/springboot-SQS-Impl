package com.apple.springboot.repository;

import com.apple.springboot.model.CleansedDataStore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface CleansedDataStoreRepository extends JpaRepository<CleansedDataStore, UUID> {
    /**
     * Finds a cleansed record by raw data ID.
     */
    Optional<CleansedDataStore> findByRawDataId(UUID rawDataId);
    /**
     * Loads the most recent cleansed record for a raw data ID.
     */
    Optional<CleansedDataStore>findTopByRawDataIdOrderByCleansedAtDesc (UUID rawDataId);
    // Optional<CleansedDataStore> findBySourceUriAndContentHash(String sourceUri, String contentHash);

    /**
     * Updates status when the current status matches the expected value.
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("update CleansedDataStore c set c.status = :newStatus where c.id = :id and c.status = :expectedStatus")
    int updateStatusIfMatches(@Param("id") UUID id,
                              @Param("expectedStatus") String expectedStatus,
                              @Param("newStatus") String newStatus);
}