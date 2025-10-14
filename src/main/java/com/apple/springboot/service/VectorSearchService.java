package com.apple.springboot.service;

import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.repository.ContentChunkRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
public class VectorSearchService {

    @Autowired
    private ContentChunkRepository contentChunkRepository;
    @Autowired
    private BedrockEnrichmentService bedrockEnrichmentService;

    @Transactional(readOnly = true)
    public List<ContentChunkWithDistance> search(String query, String original_field_name, int limit, List<String> tags, List<String> keywords, Map<String, Object> contextMap, Double threshold) throws IOException {
        float[] queryVector = bedrockEnrichmentService.generateEmbedding(query);
        String[] tagsArray = (tags != null && !tags.isEmpty()) ? tags.toArray(new String[0]) : null;
        String[] keywordsArray = (keywords != null && !keywords.isEmpty()) ? keywords.toArray(new String[0]) : null;
        String field_name = (original_field_name != null && !original_field_name.isEmpty()) ? original_field_name.toLowerCase() : null;
        return contentChunkRepository.findSimilar(queryVector, field_name, tagsArray, keywordsArray, contextMap, threshold, limit);
    }
}