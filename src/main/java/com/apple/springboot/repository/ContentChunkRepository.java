package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface ContentChunkRepository extends JpaRepository<ContentChunk, UUID>, ContentChunkRepositoryCustom {
}
