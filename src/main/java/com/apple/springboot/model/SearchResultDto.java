package com.apple.springboot.model;

import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Search result containing matched content and metadata")
public class SearchResultDto {
    @Schema(description = "Cleansed text content of the matched result", example = "Learn how to set up your iPad")
    private String cleansedText;
    
    @Schema(description = "Source field name where the content was found", example = "copy")
    private String sourceFieldName;
    
    @Schema(description = "Path to the section containing the result", example = "/en_US/ipad")
    private String sectionPath;

    /**
     * Creates a DTO for a single search result.
     */
    public SearchResultDto(String cleansedText, String sourceFieldName, String sectionPath) {
        this.cleansedText = cleansedText;
       this.sourceFieldName = sourceFieldName;
        this.sectionPath = sectionPath;
    }
    /**
     * Returns the cleansed text content.
     */
    public String getCleansedText() { return cleansedText; }
    /**
     * Updates the cleansed text content.
     */
    public void setCleansedText(String cleansedText) { this.cleansedText = cleansedText; }
    /**
     * Returns the source field name.
     */
   public String getSourceFieldName() { return sourceFieldName; }

    /**
     * Updates the source field name.
     */
    public void setSourceFieldName(String sourceFieldName) { this.sourceFieldName = sourceFieldName; }
    /**
     * Returns the section path.
     */
    public String getSectionPath() { return sectionPath; }
    /**
     * Updates the section path.
     */
    public void setSectionPath(String sectionPath) { this.sectionPath = sectionPath; }
}