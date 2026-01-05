package com.apple.springboot.service;

import com.apple.springboot.model.ItemVersionHash;
import com.apple.springboot.repository.ItemVersionHashRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;

/**
 * Best-effort persistence + retrieval for {@code item_version_hashes}.
 *
 * Key goals:
 * - Additive and safe: if the table isn't present (yet), do nothing and let callers fall back.
 * - Isolated: uses REQUIRES_NEW so failures won't poison the caller's transaction.
 */
@Service
public class ItemVersionHashStore {

    private static final Logger logger = LoggerFactory.getLogger(ItemVersionHashStore.class);

    private final ItemVersionHashRepository itemVersionHashRepository;
    private final JdbcTemplate jdbcTemplate;

    // Cached after first check; if schema changes at runtime, restart the app.
    private volatile Boolean tablePresent;

    public ItemVersionHashStore(ItemVersionHashRepository itemVersionHashRepository, JdbcTemplate jdbcTemplate) {
        this.itemVersionHashRepository = itemVersionHashRepository;
        this.jdbcTemplate = jdbcTemplate;
    }

    boolean isTablePresent() {
        Boolean cached = tablePresent;
        if (cached != null) {
            return cached;
        }
        boolean present = false;
        try {
            // PostgreSQL/Yugabyte: to_regclass returns NULL if relation doesn't exist.
            String reg = jdbcTemplate.queryForObject(
                    "select to_regclass('public.item_version_hashes')",
                    String.class
            );
            present = reg != null;
        } catch (Exception e) {
            present = false;
        }
        tablePresent = present;
        return present;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ItemVersionHash> safeLoad(String sourceUri, Integer version) {
        if (sourceUri == null || version == null) {
            return Collections.emptyList();
        }
        if (!isTablePresent()) {
            return Collections.emptyList();
        }
        try {
            return itemVersionHashRepository.findAllBySourceUriAndVersion(sourceUri, version);
        } catch (Exception e) {
            logger.debug("Failed to load item_version_hashes for {} v{} (safe fallback). Reason: {}",
                    sourceUri, version, e.getMessage());
            return Collections.emptyList();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void safeSaveAll(List<ItemVersionHash> hashes) {
        if (hashes == null || hashes.isEmpty()) {
            return;
        }
        if (!isTablePresent()) {
            return;
        }
        try {
            itemVersionHashRepository.saveAll(hashes);
        } catch (Exception e) {
            logger.debug("Failed to persist item_version_hashes (safe fallback). Reason: {}", e.getMessage());
        }
    }
}
