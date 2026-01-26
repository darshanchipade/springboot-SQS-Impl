package com.apple.springboot.dto;

import java.util.List;

public class EnrichedContentGenerateRequest {

    private List<String> fields;

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }
}