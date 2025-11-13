package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import java.util.List;

public interface ConsolidatedEnrichedSectionRepositoryCustom {
    List<ConsolidatedEnrichedSection> findByFullTextSearch(String query);

    /**
     * Case-insensitive LIKE search across original_field_name, section_path, section_uri, and context usagePath.
     */
    List<ConsolidatedEnrichedSection> findBySectionKey(String sectionKey, int limit);

    /**
     * Full-text search across metadata ONLY (excludes cleansed_text content).
     * Fields included: summary, tags, keywords, original_field_name, section_path, section_uri, context usagePath.
     */
    List<ConsolidatedEnrichedSection> findByMetadataQuery(String query, int limit);
    List<ConsolidatedEnrichedSection> findByContextSectionKey(String sectionKey, int limit);
    List<ConsolidatedEnrichedSection> findByPageId(String pageId, int limit);
}