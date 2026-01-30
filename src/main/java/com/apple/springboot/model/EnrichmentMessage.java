package com.apple.springboot.model;

import java.util.UUID;

public class EnrichmentMessage {

    private CleansedItemDetail cleansedItemDetail;
    private UUID cleansedDataStoreId;

    /**
     * Default constructor for serialization frameworks.
     */
    public EnrichmentMessage() {
    }

    /**
     * Creates a message for an enrichment item and its parent cleansed record.
     */
    public EnrichmentMessage(CleansedItemDetail cleansedItemDetail, UUID cleansedDataStoreId) {
        this.cleansedItemDetail = cleansedItemDetail;
        this.cleansedDataStoreId = cleansedDataStoreId;
    }

    /**
     * Returns the cleansed item to be enriched.
     */
    public CleansedItemDetail getCleansedItemDetail() {
        return cleansedItemDetail;
    }

    /**
     * Updates the cleansed item payload.
     */
    public void setCleansedItemDetail(CleansedItemDetail cleansedItemDetail) {
        this.cleansedItemDetail = cleansedItemDetail;
    }

    /**
     * Returns the parent cleansed data store ID.
     */
    public UUID getCleansedDataStoreId() {
        return cleansedDataStoreId;
    }

    /**
     * Updates the parent cleansed data store ID.
     */
    public void setCleansedDataStoreId(UUID cleansedDataStoreId) {
        this.cleansedDataStoreId = cleansedDataStoreId;
    }
}
