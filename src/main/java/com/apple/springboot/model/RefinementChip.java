package com.apple.springboot.model;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(of = {"value", "type"}) // Two chips are the same if they have the same value and type
public class RefinementChip {
    private String value;
    private String type; // e.g., "Tag", "Keyword", "Context:eventType"
    private int count;
}