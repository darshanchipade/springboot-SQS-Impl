package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EnrichmentContext {
    @JsonProperty("envelope")
    private Envelope envelope;

    @JsonProperty("facets")
    private Facets facets;
}
