package com.apple.springboot.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FacetResponse {
    private List<String> tags;
    private List<String> keywords;
    private Map<String, List<String>> contextFacets;
}