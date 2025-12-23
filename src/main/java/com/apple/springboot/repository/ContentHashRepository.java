package com.apple.springboot.repository;

import com.apple.springboot.model.ContentHash;
import com.apple.springboot.model.ContentHashId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ContentHashRepository extends JpaRepository<ContentHash, ContentHashId> {
    Optional<ContentHash> findBySourcePathAndItemTypeAndUsagePath(String sourcePath, String itemType, String usagePath);
    Optional<ContentHash> findBySourcePathAndItemType(String sourcePath, String itemType);
    Optional<ContentHash> findByUsagePathAndItemType(String usagePath, String itemType);

    List<ContentHash> findAllBySourcePathAndItemType(String sourcePath, String itemType);
}