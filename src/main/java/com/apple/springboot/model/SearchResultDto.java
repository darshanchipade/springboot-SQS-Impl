package com.apple.springboot.model;

public class SearchResultDto {
    private String cleansedText;
    private String sourceFieldName;
    private String sectionPath;
    private String locale;
    private String country;
    private String language;

    public SearchResultDto(String cleansedText,
                           String sourceFieldName,
                           String sectionPath,
                           String locale,
                           String country,
                           String language) {
        this.cleansedText = cleansedText;
       this.sourceFieldName = sourceFieldName;
        this.sectionPath = sectionPath;
        this.locale = locale;
        this.country = country;
        this.language = language;
    }
    public String getCleansedText() { return cleansedText; }
    public void setCleansedText(String cleansedText) { this.cleansedText = cleansedText; }
   public String getSourceFieldName() { return sourceFieldName; }

    public void setSourceFieldName(String sourceFieldName) { this.sourceFieldName = sourceFieldName; }
    public String getSectionPath() { return sectionPath; }
    public void setSectionPath(String sectionPath) { this.sectionPath = sectionPath; }

    public String getLocale() {
        return locale;
    }

    public void setLocale(String locale) {
        this.locale = locale;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }
}