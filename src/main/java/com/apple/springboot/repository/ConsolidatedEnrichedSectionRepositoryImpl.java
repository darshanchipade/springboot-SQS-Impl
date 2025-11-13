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
        // Deprecated in practice by findByMetadataQuery; kept for compatibility (no cleansed_text)
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE to_tsvector('english', " +
                "COALESCE(summary, '') || ' ' || " +
                "COALESCE(array_to_string(tags, ' '), '') || ' ' || " +
                "COALESCE(array_to_string(keywords, ' '), '')) " +
                "@@ websearch_to_tsquery('english', :query)";

        Query query = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        query.setParameter("query", textQuery);
        query.setMaxResults(50);
        return query.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findBySectionKey(String sectionKey, int limit) {
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE LOWER(COALESCE(original_field_name, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_path, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(section_uri, '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(context->>'usagePath', '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "   OR LOWER(COALESCE(context#>>'{envelope,usagePath}', '')) LIKE LOWER(CONCAT('%', :key, '%')) " +
                "ORDER BY saved_at DESC NULLS LAST";

        Query q = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        q.setParameter("key", sectionKey);
        q.setMaxResults(Math.max(1, limit));
        return q.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findByMetadataQuery(String textQuery, int limit) {
        // Excludes cleansed_text; searches metadata and context usagePath
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE to_tsvector('english', " +
                "COALESCE(summary, '') || ' ' || " +
                "COALESCE(array_to_string(tags, ' '), '') || ' ' || " +
                "COALESCE(array_to_string(keywords, ' '), '') || ' ' || " +
                "COALESCE(original_field_name, '') || ' ' || " +
                "COALESCE(section_path, '') || ' ' || " +
                "COALESCE(section_uri, '') || ' ' || " +
                "COALESCE(context->>'usagePath', '') || ' ' || " +
                "COALESCE(context#>>'{envelope,usagePath}', '')) " +
                "@@ websearch_to_tsquery('english', :query)";

        Query query = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        query.setParameter("query", textQuery);
        query.setMaxResults(Math.max(1, limit));
        return query.getResultList();
    }
    @Override
    public List<ConsolidatedEnrichedSection> findByContextSectionKey(String sectionKey, int limit) {
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE LOWER(COALESCE(context#>>'{facets,sectionKey}', '')) = LOWER(:key) " +
                "ORDER BY saved_at DESC NULLS LAST";
        Query q = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        q.setParameter("key", sectionKey);
        q.setMaxResults(Math.max(1, limit));
        return q.getResultList();
    }

    @Override
    public List<ConsolidatedEnrichedSection> findByPageId(String pageId, int limit) {
        String sql = "SELECT * FROM consolidated_enriched_sections " +
                "WHERE LOWER(COALESCE(context#>>'{facets,pageId}', '')) = LOWER(:pageId) " +
                "   OR LOWER(COALESCE(context->>'pageId', '')) = LOWER(:pageId) " +
                "   OR LOWER(COALESCE(context#>>'{envelope,pageId}', '')) = LOWER(:pageId) " +
                "   OR LOWER(COALESCE(section_path, '')) LIKE LOWER(CONCAT('%/', :pageId, '/%')) " +
                "ORDER BY saved_at DESC NULLS LAST";
        Query q = entityManager.createNativeQuery(sql, ConsolidatedEnrichedSection.class);
        q.setParameter("pageId", pageId);
        q.setMaxResults(Math.max(1, limit));
        return q.getResultList();
    }
}