package com.apple.springboot.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchResult {
    private String cleansedText;
    private String sourceFieldName;
    private String sectionPath;
    private float score; // To hold the vector search score
}