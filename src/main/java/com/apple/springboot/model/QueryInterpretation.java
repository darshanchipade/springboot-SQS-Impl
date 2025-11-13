package com.apple.springboot.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Represents the structured interpretation of a user's free-form query.
 */
public record QueryInterpretation(
        String rawQuery,
        String sectionKey,
        String sectionName,
        String pageId,
        String role,
        String locale,
        String language,
        String country,
        List<String> tags,
        List<String> keywords,
        Map<String, Object> context
) {
    public List<String> tags() {
        return tags == null ? List.of() : Collections.unmodifiableList(tags);
    }

    public List<String> keywords() {
        return keywords == null ? List.of() : Collections.unmodifiableList(keywords);
    }

    public Map<String, Object> context() {
        return context == null ? Map.of() : Collections.unmodifiableMap(context);
    }
}
