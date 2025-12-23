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
    Optional<CleansedDataStore> findByRawDataId(UUID rawDataId);
    Optional<CleansedDataStore>findTopByRawDataIdOrderByCleansedAtDesc (UUID rawDataId);
    // Optional<CleansedDataStore> findBySourceUriAndContentHash(String sourceUri, String contentHash);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("update CleansedDataStore c set c.status = :newStatus where c.id = :id and c.status = :expectedStatus")
    int updateStatusIfMatches(@Param("id") UUID id,
                              @Param("expectedStatus") String expectedStatus,
                              @Param("newStatus") String newStatus);
}