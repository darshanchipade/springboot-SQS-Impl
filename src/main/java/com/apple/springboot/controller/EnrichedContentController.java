package com.apple.springboot.controller;

import com.apple.springboot.dto.EnrichedContentGenerateRequest;
import com.apple.springboot.dto.EnrichedContentUpdateRequest;
import com.apple.springboot.dto.EnrichedContentUpdateResponse;
import com.apple.springboot.model.EnrichedContentRevision;
import com.apple.springboot.service.EnrichedContentUpdateService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/enrichment/content")
public class EnrichedContentController {

    private static final Logger logger = LoggerFactory.getLogger(EnrichedContentController.class);

    private final EnrichedContentUpdateService updateService;

    /**
     * Creates a controller that delegates enrichment updates and revisions.
     */
    public EnrichedContentController(EnrichedContentUpdateService updateService) {
        this.updateService = updateService;
    }

    /**
     * Accepts manual edits and persists a new enrichment revision.
     */
    @Operation(
            summary = "Save manual edits for enriched content",
            description = "Updates summary, classification, keywords, and tags for a specific enriched content element."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Enriched content updated successfully"),
            @ApiResponse(responseCode = "404", description = "Enriched content element not found"),
            @ApiResponse(responseCode = "400", description = "Invalid update payload"),
            @ApiResponse(responseCode = "500", description = "Unexpected server error")
    })
    @PutMapping("/{elementId}")
    public ResponseEntity<?> updateEnrichedContent(
            @Parameter(description = "Enriched content element ID", required = true)
            @PathVariable UUID elementId,
            @RequestBody EnrichedContentUpdateRequest request) {
        try {
            EnrichedContentUpdateResponse response = updateService.applyManualUpdate(elementId, request);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to update enriched content {}: {}", elementId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    /**
     * Regenerates enrichment fields using Bedrock and returns the new payload.
     */
    @Operation(
            summary = "Regenerate enriched content using Bedrock",
            description = "Rebuilds summary, classification, keywords, and/or tags for an enriched content element."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Enriched content regenerated successfully"),
            @ApiResponse(responseCode = "404", description = "Enriched content element not found"),
            @ApiResponse(responseCode = "400", description = "Invalid regenerate request"),
            @ApiResponse(responseCode = "502", description = "Bedrock regeneration failed"),
            @ApiResponse(responseCode = "500", description = "Unexpected server error")
    })
    @PostMapping("/{elementId}/generate")
    public ResponseEntity<?> regenerateEnrichedContent(
            @Parameter(description = "Enriched content element ID", required = true)
            @PathVariable UUID elementId,
            @RequestBody(required = false) EnrichedContentGenerateRequest request) {
        try {
            EnrichedContentUpdateResponse response = updateService.regenerateFromBedrock(elementId, request);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        } catch (IllegalStateException e) {
            logger.warn("Bedrock regeneration failed for {}: {}", elementId, e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).body(e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to regenerate enriched content {}: {}", elementId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    /**
     * Lists all stored revisions for an enriched content element.
     */
    @Operation(
            summary = "List revision history for enriched content",
            description = "Returns all revisions stored for a specific enriched content element."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Revision history returned successfully"),
            @ApiResponse(responseCode = "404", description = "Enriched content element not found"),
            @ApiResponse(responseCode = "500", description = "Unexpected server error")
    })
    @GetMapping("/{elementId}/revisions")
    public ResponseEntity<?> listRevisions(
            @Parameter(description = "Enriched content element ID", required = true)
            @PathVariable UUID elementId) {
        try {
            List<EnrichedContentRevision> revisions = updateService.listRevisions(elementId);
            return ResponseEntity.ok(revisions);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to load revision history for {}: {}", elementId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    /**
     * Restores a previously saved enrichment revision to the consolidated row.
     */
    @Operation(
            summary = "Restore a previous enrichment revision",
            description = "Restores summary, classification, keywords, and tags from a stored revision."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Revision restored successfully"),
            @ApiResponse(responseCode = "404", description = "Revision or element not found"),
            @ApiResponse(responseCode = "400", description = "Revision mismatch"),
            @ApiResponse(responseCode = "500", description = "Unexpected server error")
    })
    @PostMapping("/{elementId}/restore/{revisionId}")
    public ResponseEntity<?> restoreRevision(
            @Parameter(description = "Enriched content element ID", required = true)
            @PathVariable UUID elementId,
            @Parameter(description = "Revision ID to restore", required = true)
            @PathVariable UUID revisionId) {
        try {
            EnrichedContentUpdateResponse response = updateService.restoreFromRevision(elementId, revisionId);
            return ResponseEntity.ok(response);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage());
        } catch (Exception e) {
            logger.error("Failed to restore revision {} for {}: {}", revisionId, elementId, e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }
}
