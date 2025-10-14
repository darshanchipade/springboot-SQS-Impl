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
                15, // limit
                request.getTags(),
                request.getKeywords(),
                request.getContext(),
                null // No threshold for filtered search
        );


        // Transform the results into the DTO expected by the frontend
        return results.stream().map(result -> {
            return new SearchResultDto(
                    result.getContentChunk().getConsolidatedEnrichedSection().getCleansedText(),
                    result.getContentChunk().getConsolidatedEnrichedSection().getOriginalFieldName(),
                    result.getContentChunk().getConsolidatedEnrichedSection().getSectionUri()
            );
        }).collect(Collectors.toList());
    }
}