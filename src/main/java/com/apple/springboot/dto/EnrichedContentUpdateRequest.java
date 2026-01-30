package com.apple.springboot.dto;

import java.util.List;

public class EnrichedContentUpdateRequest {

    private String summary;
    private String classification;
    private List<String> keywords;
    private List<String> tags;
    private String editedBy;
    private String note;

    /**
     * Returns the updated summary text.
     */
    public String getSummary() {
        return summary;
    }

    /**
     * Sets the updated summary text.
     */
    public void setSummary(String summary) {
        this.summary = summary;
    }

    /**
     * Returns the updated classification.
     */
    public String getClassification() {
        return classification;
    }

    /**
     * Sets the updated classification.
     */
    public void setClassification(String classification) {
        this.classification = classification;
    }

    /**
     * Returns the updated keywords list.
     */
    public List<String> getKeywords() {
        return keywords;
    }

    /**
     * Sets the updated keywords list.
     */
    public void setKeywords(List<String> keywords) {
        this.keywords = keywords;
    }

    /**
     * Returns the updated tags list.
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * Sets the updated tags list.
     */
    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    /**
     * Returns the editor identifier when supplied.
     */
    public String getEditedBy() {
        return editedBy;
    }

    /**
     * Sets the editor identifier for auditing.
     */
    public void setEditedBy(String editedBy) {
        this.editedBy = editedBy;
    }

    /**
     * Returns the optional edit note.
     */
    public String getNote() {
        return note;
    }

    /**
     * Sets the optional edit note.
     */
    public void setNote(String note) {
        this.note = note;
    }
}
