package com.apple.springboot.dto;

public record CleansedItemRow(
        String id,
        String field,
        String original,
        String cleansed
) {}
