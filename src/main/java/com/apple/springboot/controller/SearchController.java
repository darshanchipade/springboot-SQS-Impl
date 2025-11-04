package com.apple.springboot.controller;

import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.RefinementChip;
import com.apple.springboot.model.SearchRequest;
import com.apple.springboot.model.SearchResultDto;
import com.apple.springboot.service.RefinementService;
import com.apple.springboot.service.VectorSearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
public class SearchController {

    private final RefinementService refinementService;
    private final VectorSearchService vectorSearchService;

    @Autowired
    public SearchController(RefinementService refinementService, VectorSearchService vectorSearchService) {
        this.refinementService = refinementService;
        this.vectorSearchService = vectorSearchService;
    }

    @GetMapping("/refine")
    public List<RefinementChip> getRefinementChips(@RequestParam String query) throws IOException {
        return refinementService.getRefinementChips(query);
    }

    @PostMapping("/search")
    public List<SearchResultDto> search(@RequestBody SearchRequest request) throws IOException {
        List<ContentChunkWithDistance> results = vectorSearchService.search(
                request.getQuery(),
                request.getOriginal_field_name(),
                15,                  // limit
                request.getTags(),
                request.getKeywords(),
                request.getContext(),
                null,                // threshold
                null                 // sectionKeyFilter (e.g., "chapter-nav-section") or leave null
        );


        // Transform the results into the DTO expected by the frontend
        return results.stream().map(result -> {
            com.apple.springboot.model.ConsolidatedEnrichedSection section = result.getContentChunk().getConsolidatedEnrichedSection();
            return new SearchResultDto(
                    section.getCleansedText(),
                    section.getOriginalFieldName(),
                    section.getSectionUri(),
                    getEnvelopeValue(section, "locale"),
                    getEnvelopeValue(section, "country"),
                    getEnvelopeValue(section, "language")
            );
        }).collect(Collectors.toList());
    }

    private String getEnvelopeValue(com.apple.springboot.model.ConsolidatedEnrichedSection section, String key) {
        if (section == null || section.getContext() == null) {
            return null;
        }
        Object envelope = section.getContext().get("envelope");
        if (envelope instanceof java.util.Map<?, ?> envMap) {
            Object value = envMap.get(key);
            return value != null ? value.toString() : null;
        }
        return null;
    }
}