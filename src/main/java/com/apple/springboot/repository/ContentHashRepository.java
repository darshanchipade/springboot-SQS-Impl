package com.apple.springboot.repository;

import com.apple.springboot.model.ContentHash;
import com.apple.springboot.model.ContentHashId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ContentHashRepository extends JpaRepository<ContentHash, ContentHashId> {
    /**
     * Finds a content hash by source path, item type, and usage path.
     */
    Optional<ContentHash> findBySourcePathAndItemTypeAndUsagePath(String sourcePath, String itemType, String usagePath);
    /**
     * Finds a content hash by source path and item type.
     */
    Optional<ContentHash> findBySourcePathAndItemType(String sourcePath, String itemType);
    /**
     * Finds a content hash by usage path and item type.
     */
    Optional<ContentHash> findByUsagePathAndItemType(String usagePath, String itemType);

    /**
     * Loads all content hashes for a source path and item type.
     */
    List<ContentHash> findAllBySourcePathAndItemType(String sourcePath, String itemType);
    /**
     * Loads all content hashes for a list of source paths.
     */
    List<ContentHash> findAllBySourcePathIn(List<String> sourcePaths);
}