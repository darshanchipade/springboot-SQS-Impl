
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
                "COALESCE(array_to_string(tags, ' '), '') || ' ' || " +
                "COALESCE(array_to_string(keywords, ' '), '')) " +
                "@@ plainto_tsquery('english', :query)";

        Query query = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        query.setParameter("query", textQuery);
        query.setMaxResults(50); // Limit the results to a reasonable number for facet generation

        return query.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findByMetadataQuery(String textQuery, int limit) {
        // Excludes cleansed_text on purpose; searches metadata only
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE to_tsvector('english', " +
                "COALESCE(summary, '') || ' ' || " +
                "COALESCE(array_to_string(tags, ' '), '') || ' ' || " +
                "COALESCE(array_to_string(keywords, ' '), '') || ' ' || " +
                "COALESCE(original_field_name, '') || ' ' || " +
                "COALESCE(section_path, '') || ' ' || " +
                "COALESCE(section_uri, '')) " +
                "@@ plainto_tsquery('english', :query)";

        Query query = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        query.setParameter("query", textQuery);
        query.setMaxResults(Math.max(1, Math.min(limit, 200)));
        return query.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findBySectionKey(String sectionKey, int limit) {
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE LOWER(COALESCE(original_field_name, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_path, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_uri, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                // also check common context paths that may include a usage or section identifier
                "   OR LOWER(COALESCE(context->>'usagePath', '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(context#>>'{envelope,usagePath}', '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "ORDER BY saved_at DESC NULLS LAST";

        Query q = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        q.setParameter("key", sectionKey);
        q.setMaxResults(Math.max(1, Math.min(limit, 200)));
        return q.getResultList();
    }
}
