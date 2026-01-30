package com.apple.springboot.repository;

import com.apple.springboot.model.ContentChunk;
import com.apple.springboot.model.ContentChunkWithDistance;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.stereotype.Repository;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

@Repository
public class ContentChunkRepositoryImpl implements ContentChunkRepositoryCustom {

    @PersistenceContext
    private EntityManager entityManager;

    /**
     * Builds and executes a native query for vector similarity search.
     */
    @Override
    public List<ContentChunkWithDistance> findSimilar(float[] embedding, String originalFieldName, String[] tags, String[] keywords, Map<String, Object> contextMap, Double threshold, int limit, String sectionKeyFilter) {
        StringBuilder sql = new StringBuilder("SELECT c.*");
        if (embedding != null) {
            sql.append(", (c.vector <=> CAST(:embedding AS vector)) as distance");
        }
        sql.append(" FROM content_chunks c JOIN consolidated_enriched_sections s ON c.consolidated_enriched_section_id = s.id WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (embedding != null) {
            params.put("embedding", embedding);
        }
        if (threshold != null) {
            params.put("distance_threshold", threshold);
        }
        if (originalFieldName != null && !originalFieldName.isBlank()) {
            sql.append(" AND LOWER(s.original_field_name) = LOWER(:originalFieldName)");
            params.put("originalFieldName", originalFieldName);
        }
        if (tags != null && tags.length > 0) {
            sql.append(" AND s.tags @> CAST(:tags AS text[])");
            params.put("tags", tags);
        }
        if (keywords != null && keywords.length > 0) {
            sql.append(" AND s.keywords @> CAST(:keywords AS text[])");
            params.put("keywords", keywords);
        }
        if (contextMap != null && !contextMap.isEmpty()) {
            buildJsonbQueries(contextMap, new ArrayList<>(), sql, params);
        }
        if (sectionKeyFilter != null && !sectionKeyFilter.isBlank()) {
            String loweredKey = sectionKeyFilter.toLowerCase();
            sql.append(" AND (")
                    .append("LOWER(COALESCE(s.original_field_name, '')) LIKE :sectionKey ")
                    .append("OR LOWER(COALESCE(s.section_path, '')) LIKE :sectionKey ")
                    .append("OR LOWER(COALESCE(s.section_uri, '')) LIKE :sectionKey ")
                    .append("OR LOWER(COALESCE(s.context->>'usagePath', '')) LIKE :sectionKey ")
                    .append("OR LOWER(COALESCE(s.context#>>'{envelope,usagePath}', '')) LIKE :sectionKey ")
                    .append("OR LOWER(COALESCE(s.context#>>'{sectionKey}', '')) = :sectionKeyExact ")
                    .append("OR LOWER(COALESCE(s.context#>>'{facets,sectionKey}', '')) = :sectionKeyExact ")
                    .append("OR LOWER(COALESCE(s.context#>>'{envelope,sectionKey}', '')) = :sectionKeyExact ")
                    .append("OR LOWER(COALESCE(c.source_field, '')) LIKE :sectionKey)")
            ;
            params.put("sectionKey", "%" + loweredKey + "%"); // BOTH-SIDES WILDCARD
            params.put("sectionKeyExact", loweredKey);
        }
        if (embedding != null) {
            if (params.containsKey("distance_threshold")) {
                sql.append(" AND (c.vector <=> CAST(:embedding AS vector)) < :distance_threshold");
            }
            sql.append(" ORDER BY distance");
        }
        sql.append(" LIMIT :limit");
        params.put("limit", limit);

        Query query = entityManager.createNativeQuery(sql.toString(), "ContentChunkWithDistanceMapping");
        params.forEach(query::setParameter);

        List<Object[]> results = query.getResultList();
        List<ContentChunkWithDistance> dtos = new ArrayList<>();
        for (Object[] result : results) {
            ContentChunk chunk = (ContentChunk) result[0];
            double distance = (embedding != null) ? ((Number) result[1]).doubleValue() : 0.0;
            dtos.add(new ContentChunkWithDistance(chunk, distance));
        }
        return dtos;
    }

    /**
     * Appends JSONB query predicates for context map filters.
     */
    private void buildJsonbQueries(Map<String, Object> map, List<String> path, StringBuilder sql, Map<String, Object> params) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            List<String> newPath = new ArrayList<>(path);
            newPath.add(entry.getKey());
            if (entry.getValue() instanceof Map) {
                buildJsonbQueries((Map<String, Object>) entry.getValue(), newPath, sql, params);
            } else if (entry.getValue() instanceof List) {
                String pathString = "{" + String.join(",", newPath) + "}";
                String paramName = String.join("_", newPath);
                sql.append(" AND s.context #>> '").append(pathString).append("' IN (:").append(paramName).append(")");
                params.put(paramName, (List<?>) entry.getValue());
            } else if (entry.getValue() != null) {
                // Handle string and other scalar values (locale, country, language, etc.)
                String pathString = "{" + String.join(",", newPath) + "}";
                String paramName = String.join("_", newPath);
                sql.append(" AND LOWER(COALESCE(s.context#>>'").append(pathString).append("', '')) = LOWER(:").append(paramName).append(")");
                params.put(paramName, entry.getValue().toString());
            }
        }
    }
}