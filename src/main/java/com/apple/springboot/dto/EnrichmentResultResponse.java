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

    /**
     * Returns summary metrics for the enrichment run.
     */
    public Metrics getMetrics() {
        return metrics;
    }

    /**
     * Sets summary metrics for the enrichment run.
     */
    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }

    /**
     * Returns the list of enriched content elements.
     */
    public List<EnrichedContentElement> getElements() {
        return elements;
    }

    /**
     * Sets the list of enriched content elements.
     */
    public void setElements(List<EnrichedContentElement> elements) {
        this.elements = elements;
    }

    public static class Metrics {
        private Integer totalFieldsTagged;
        private Double readabilityImproved;
        private Integer errorsFound;

        /**
         * Returns the total number of tagged fields.
         */
        public Integer getTotalFieldsTagged() {
            return totalFieldsTagged;
        }

        /**
         * Sets the total number of tagged fields.
         */
        public void setTotalFieldsTagged(Integer totalFieldsTagged) {
            this.totalFieldsTagged = totalFieldsTagged;
        }

        /**
         * Returns the readability improvement metric.
         */
        public Double getReadabilityImproved() {
            return readabilityImproved;
        }

        /**
         * Sets the readability improvement metric.
         */
        public void setReadabilityImproved(Double readabilityImproved) {
            this.readabilityImproved = readabilityImproved;
        }

        /**
         * Returns the error count observed during enrichment.
         */
        public Integer getErrorsFound() {
            return errorsFound;
        }

        /**
         * Sets the error count observed during enrichment.
         */
        public void setErrorsFound(Integer errorsFound) {
            this.errorsFound = errorsFound;
        }
    }
}
