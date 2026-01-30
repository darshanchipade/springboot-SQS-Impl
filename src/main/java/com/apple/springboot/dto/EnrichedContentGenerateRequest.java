package com.apple.springboot.dto;

import java.util.List;

public class EnrichedContentGenerateRequest {

    private List<String> fields;
    private Boolean preview;

    /**
     * Returns the list of fields requested for regeneration.
     */
    public List<String> getFields() {
        return fields;
    }

    /**
     * Sets the list of fields requested for regeneration.
     */
    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    /**
     * Returns whether generation should be preview-only.
     */
    public Boolean getPreview() {
        return preview;
    }

    /**
     * Sets whether generation should be preview-only.
     */
    public void setPreview(Boolean preview) {
        this.preview = preview;
    }
}
