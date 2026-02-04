package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import java.util.List;

public interface ConsolidatedEnrichedSectionRepositoryCustom {
    /**
     * Performs full-text search against summary, tags, and keywords.
     */
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
    /**
     * Finds sections by a sectionKey stored in context facets.
     */
    List<ConsolidatedEnrichedSection> findByContextSectionKey(String sectionKey, int limit);
    /**
     * Finds sections by page identifier metadata.
     */
    List<ConsolidatedEnrichedSection> findByPageId(String pageId, int limit);
}