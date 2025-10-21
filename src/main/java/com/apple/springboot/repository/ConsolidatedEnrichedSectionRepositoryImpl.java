package com.apple.springboot.repository;

import com.apple.springboot.model.ConsolidatedEnrichedSection;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ConsolidatedEnrichedSectionRepositoryImpl implements ConsolidatedEnrichedSectionRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    @Override
    public List<ConsolidatedEnrichedSection> findByFullTextSearch(String textQuery) {
        // This query uses PostgreSQL's full-text search capabilities.
        // It correctly converts array fields (tags, keywords) to strings before including them in the search vector.
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE to_tsvector('english', " +
                "COALESCE(summary, '') || ' ' || " +
                "COALESCE(cleansed_text, '') || ' ' || " +
                "COALESCE(array_to_string(tags, ' '), '') || ' ' || " +
                "COALESCE(array_to_string(keywords, ' '), '')) " +
                "@@ plainto_tsquery('english', :query)";

        Query query = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        query.setParameter("query", textQuery);
        query.setMaxResults(50); // Limit the results to a reasonable number for facet generation

        return query.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findBySectionKey(String sectionKey, int limit) {
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE LOWER(COALESCE(original_field_name, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_path, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_uri, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "ORDER BY saved_at DESC NULLS LAST";

        Query q = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        q.setParameter("key", sectionKey);
        q.setMaxResults(Math.max(1, Math.min(limit, 200)));
        return q.getResultList();
    }
}