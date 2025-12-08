package com.apple.springboot.dto;

import com.apple.springboot.model.EnrichedContentElement;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

/**
 * Payload returned by /api/enrichment/result/{id}.
 */
public class EnrichmentResultResponse {

    private Metrics metrics = new Metrics();

    @JsonProperty("enriched_content_elements")
    private List<EnrichedContentElement> elements = new ArrayList<>();

    public Metrics getMetrics() {
        return metrics;
    }

    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    public List<EnrichedContentElement> getElements() {
        return elements;
    }

    public void setElements(List<EnrichedContentElement> elements) {
        this.elements = elements;
    }

    public static class Metrics {
        private Integer totalFieldsTagged;
        private Double readabilityImproved;
        private Integer errorsFound;

        public Integer getTotalFieldsTagged() {
            return totalFieldsTagged;
        }

        public void setTotalFieldsTagged(Integer totalFieldsTagged) {
            this.totalFieldsTagged = totalFieldsTagged;
        }

        public Double getReadabilityImproved() {
            return readabilityImproved;
        }

        public void setReadabilityImproved(Double readabilityImproved) {
            this.readabilityImproved = readabilityImproved;
        }

        public Integer getErrorsFound() {
            return errorsFound;
        }

        public void setErrorsFound(Integer errorsFound) {
            this.errorsFound = errorsFound;
        }
    }
}
