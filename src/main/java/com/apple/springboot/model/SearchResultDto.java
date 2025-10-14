package com.apple.springboot.model;

public class SearchResultDto {
    private String cleansedText;
    private String sourceFieldName;
    private String sectionPath;

    public SearchResultDto(String cleansedText, String sourceFieldName, String sectionPath) {
        this.cleansedText = cleansedText;
       this.sourceFieldName = sourceFieldName;
        this.sectionPath = sectionPath;
    }
    public String getCleansedText() { return cleansedText; }
    public void setCleansedText(String cleansedText) { this.cleansedText = cleansedText; }
   public String getSourceFieldName() { return sourceFieldName; }

    public void setSourceFieldName(String sourceFieldName) { this.sourceFieldName = sourceFieldName; }
    public String getSectionPath() { return sectionPath; }
    public void setSectionPath(String sectionPath) { this.sectionPath = sectionPath; }
}