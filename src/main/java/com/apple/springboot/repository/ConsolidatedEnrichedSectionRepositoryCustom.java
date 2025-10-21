package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import java.util.List;

public interface ConsolidatedEnrichedSectionRepositoryCustom {
    List<ConsolidatedEnrichedSection> findByFullTextSearch(String query);

    /**
     * Case-insensitive LIKE search across original_field_name, section_path, and section_uri.
     * Intended for simple chatbot lookups like "video-section-header".
     */
    List<ConsolidatedEnrichedSection> findBySectionKey(String sectionKey, int limit);
}