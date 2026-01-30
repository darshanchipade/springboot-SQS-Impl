package com.apple.springboot.repository;

import com.apple.springboot.model.RawDataStore;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface RawDataStoreRepository extends JpaRepository<RawDataStore, UUID> {
    /**
     * Finds raw data by source URI.
     */
    Optional<RawDataStore> findBySourceUri(String sourceUri);
    /**
     * Finds raw data by content hash.
     */
    Optional<RawDataStore> findByContentHash(String contentHash);
    /**
     * Finds raw data by source URI and content hash.
     */
    Optional<RawDataStore> findBySourceUriAndContentHash(String sourceUri, String contentHash);
    /**
     * Loads the latest raw data version for a source URI.
     */
    Optional<RawDataStore> findTopBySourceUriOrderByVersionDesc(String sourceUri);
    /**
     * Loads the previous raw data version for a source URI.
     */
    Optional<RawDataStore> findTopBySourceUriAndVersionLessThanOrderByVersionDesc(String sourceUri, Integer version);
}