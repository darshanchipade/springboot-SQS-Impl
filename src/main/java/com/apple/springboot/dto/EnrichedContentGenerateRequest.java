package com.apple.springboot.dto;

import java.util.List;

public class EnrichedContentGenerateRequest {

    private List<String> fields;
    private Boolean preview;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public Boolean getPreview() {
        return preview;
    }

    public void setPreview(Boolean preview) {
        this.preview = preview;
    }
}
