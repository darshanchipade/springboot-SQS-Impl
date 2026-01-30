package com.apple.springboot.dto;

public class CleansedItemRow {
    private String id;
    private String field;
    private String original;
    private String cleansed;

    /**
     * Returns the field label.
     */
    public String getField() {
        return field;
    }

    /**
     * Sets the field label.
     */
    public void setField(String field) {
        this.field = field;
    }

    /**
     * Returns the original value.
     */
    public String getOriginal() {
        return original;
    }

    /**
     * Sets the original value.
     */
    public void setOriginal(String original) {
        this.original = original;
    }

    /**
     * Returns the cleansed value.
     */
    public String getCleansed() {
        return cleansed;
    }

    /**
     * Sets the cleansed value.
     */
    public void setCleansed(String cleansed) {
        this.cleansed = cleansed;
    }

    /**
     * Returns the row identifier.
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the row identifier.
     */
    public void setId(String id) {
        this.id = id;
    }
// getters/setters omitted for brevity
}