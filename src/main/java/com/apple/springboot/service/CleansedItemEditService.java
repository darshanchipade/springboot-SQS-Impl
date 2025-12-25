package com.apple.springboot.service;

import com.apple.springboot.model.CleansedDataStore;
import com.apple.springboot.model.ContentHash;
import com.apple.springboot.repository.CleansedDataStoreRepository;
import com.apple.springboot.repository.ContentHashRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

@Service
public class CleansedItemEditService {

    private final CleansedDataStoreRepository cleansedRepo;
    private final ContentHashRepository contentHashRepository;
    private final ContentHashingService contentHashingService;

    // Keep cleanse behavior consistent with DataIngestionService
    private static final Pattern NBSP_PATTERN = Pattern.compile("\\{%nbsp%\\}");
    private static final Pattern SOSUMI_PATTERN = Pattern.compile("\\{%sosumi type=\"[^\"]+\" metadata=\"\\d+\"%\\}");
    private static final Pattern BR_PATTERN = Pattern.compile("\\{%br%\\}");
    private static final Pattern URL_PATTERN = Pattern.compile(":\\s*\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)");
    private static final Pattern NESTED_URL_PATTERN = Pattern.compile(":\\[\\s*:\\[[^\\]]+\\]\\(\\{%url metadata=\"\\d+\" destination-type=\"[^\"]+\"%\\}\\)\\]\\(\\{%wj%\\}\\)");
    private static final Pattern METADATA_PATTERN = Pattern.compile("\\{% metadata=\"\\d+\" %\\}");

    public CleansedItemEditService(CleansedDataStoreRepository cleansedRepo,
                                  ContentHashRepository contentHashRepository,
                                  ContentHashingService contentHashingService) {
        this.cleansedRepo = cleansedRepo;
        this.contentHashRepository = contentHashRepository;
        this.contentHashingService = contentHashingService;
    }

    @Transactional
    public CleansedDataStore updateSingleItem(UUID cleansedDataId,
                                              String sourcePath,
                                              String fieldName,
                                              String newOriginalValue) {
        if (cleansedDataId == null) {
            throw new IllegalArgumentException("cleansedDataId is required");
        }
        if (sourcePath == null || sourcePath.isBlank()) {
            throw new IllegalArgumentException("sourcePath is required");
        }
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("fieldName is required");
        }

        CleansedDataStore store = cleansedRepo.findById(cleansedDataId)
                .orElseThrow(() -> new IllegalArgumentException("CleansedDataStore not found for id " + cleansedDataId));

        List<Map<String, Object>> items = store.getCleansedItems();
        if (items == null || items.isEmpty()) {
            throw new IllegalStateException("CleansedDataStore " + cleansedDataId + " has no cleansedItems to update");
        }

        Map<String, Object> match = null;
        for (Map<String, Object> item : items) {
            if (item == null) continue;
            String sp = asString(item.get("sourcePath"));
            String of = asString(item.get("originalFieldName"));
            String it = asString(item.get("itemType"));
            if (Objects.equals(sourcePath, sp) && (Objects.equals(fieldName, of) || Objects.equals(fieldName, it))) {
                match = item;
                break;
            }
        }

        if (match == null) {
            throw new IllegalArgumentException("No cleansed item found for sourcePath=" + sourcePath + " fieldName=" + fieldName);
        }

        String cleansed = cleanseCopyText(newOriginalValue);
        String contentHash = contentHashingService.hash(cleansed);

        // Update the stored map in-place so enrichment reads the new value
        match.put("originalValue", newOriginalValue);
        match.put("cleansedValue", cleansed);
        match.put("cleansedContent", cleansed);
        match.put("contentHash", contentHash);

        // Reset status so /api/enrichment/start/{id} can run again
        store.setStatus("CLEANSED_PENDING_ENRICHMENT");

        // Also update content_hashes so future delta-cleansed runs stay consistent
        String usagePath = asString(match.get("usagePath"));
        String itemType = asString(match.get("itemType"));
        if (itemType != null) {
            String effectiveUsage = usagePath != null ? usagePath : sourcePath;
            Optional<ContentHash> existing =
                    contentHashRepository.findBySourcePathAndItemTypeAndUsagePath(sourcePath, itemType, effectiveUsage);
            ContentHash ch = existing.orElseGet(() -> new ContentHash(sourcePath, itemType, effectiveUsage, null, null));
            ch.setContentHash(contentHash);
            // No context update here; item context structure is unchanged by a copy edit.
            contentHashRepository.save(ch);
        }

        // Persist
        return cleansedRepo.save(store);
    }

    private static String asString(Object v) {
        return v instanceof String s ? s : null;
    }

    private static String cleanseCopyText(String text) {
        if (text == null) return null;
        String cleansed = text;
        cleansed = NBSP_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = BR_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = SOSUMI_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = NESTED_URL_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = URL_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = METADATA_PATTERN.matcher(cleansed).replaceAll(" ");
        cleansed = cleansed.replaceAll("<[^>]+?>", " ");
        cleansed = cleansed.replace('\u00A0', ' ');
        cleansed = cleansed.replaceAll("\\s+", " ").trim();
        return cleansed;
    }
}

