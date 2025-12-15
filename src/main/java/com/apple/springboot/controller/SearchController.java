package com.apple.springboot.controller;

import com.apple.springboot.model.ContentChunkWithDistance;
import com.apple.springboot.model.RefinementChip;
import com.apple.springboot.model.SearchRequest;
import com.apple.springboot.model.SearchResultDto;
import com.apple.springboot.service.RefinementService;
import com.apple.springboot.service.VectorSearchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api")
@Tag(name = "Search", description = "Vector search and refinement API endpoints")
public class SearchController {

    private final RefinementService refinementService;
    private final VectorSearchService vectorSearchService;

    @Autowired
    public SearchController(RefinementService refinementService, VectorSearchService vectorSearchService) {
        this.refinementService = refinementService;
        this.vectorSearchService = vectorSearchService;
    }

    @Operation(
            summary = "Get refinement chips for a query",
            description = "Retrieves refinement chips (suggestions) based on the provided query. " +
                    "These chips can be used to refine search queries."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved refinement chips",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = RefinementChip.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/refine")
    public List<RefinementChip> getRefinementChips(
            @Parameter(description = "Search query to generate refinement chips for", required = true)
            @RequestParam String query) throws IOException {
        return refinementService.getRefinementChips(query);
    }

    @Operation(
            summary = "Vector search endpoint",
            description = "Performs a vector search based on the provided query and filters. " +
                    "Returns the top matching content chunks with their metadata. " +
                    "Supports filtering by tags, keywords, original field name, and context."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved search results",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = SearchResultDto.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @PostMapping("/search")
    public List<SearchResultDto> search(@RequestBody SearchRequest request) throws IOException {
        List<ContentChunkWithDistance> results = vectorSearchService.search(
                request.getQuery(),
                request.getOriginal_field_name(),
                200,                  // limit
                request.getTags(),
                request.getKeywords(),
                request.getContext(),
                null,                // threshold
                null                 // sectionKeyFilter (e.g., "chapter-nav-section") or leave null
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