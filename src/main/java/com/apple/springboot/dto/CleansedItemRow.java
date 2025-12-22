package com.apple.springboot.dto;

public class CleansedItemRow {
    private String id;
    private String field;
    private String original;
    private String cleansed;
    private Boolean delta;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOriginal() {
        return original;
    }

    public void setOriginal(String original) {
        this.original = original;
    }

    public String getCleansed() {
        return cleansed;
    }

    public void setCleansed(String cleansed) {
        this.cleansed = cleansed;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getDelta() {
        return delta;
    }

    public void setDelta(Boolean delta) {
        this.delta = delta;
    }
// getters/setters omitted for brevity
}