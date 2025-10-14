package com.apple.springboot.model;

import com.apple.springboot.service.CleansedItemDetail;

import java.util.UUID;

public class EnrichmentMessage {

    private CleansedItemDetail cleansedItemDetail;
    private UUID cleansedDataStoreId;

    public EnrichmentMessage() {
    }

    public EnrichmentMessage(CleansedItemDetail cleansedItemDetail, UUID cleansedDataStoreId) {
        this.cleansedItemDetail = cleansedItemDetail;
        this.cleansedDataStoreId = cleansedDataStoreId;
    }

    public CleansedItemDetail getCleansedItemDetail() {
        return cleansedItemDetail;
    }

    public void setCleansedItemDetail(CleansedItemDetail cleansedItemDetail) {
        this.cleansedItemDetail = cleansedItemDetail;
    }

    public UUID getCleansedDataStoreId() {
        return cleansedDataStoreId;
    }

    public void setCleansedDataStoreId(UUID cleansedDataStoreId) {
        this.cleansedDataStoreId = cleansedDataStoreId;
    }
}
