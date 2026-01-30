package com.apple.springboot.repository;

import com.apple.springboot.model.ItemVersionHash;
import com.apple.springboot.model.ItemVersionHashId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ItemVersionHashRepository extends JpaRepository<ItemVersionHash, ItemVersionHashId> {
    /**
     * Loads item version hashes for a source URI and version.
     */
    List<ItemVersionHash> findAllBySourceUriAndVersion(String sourceUri, Integer version);
}
