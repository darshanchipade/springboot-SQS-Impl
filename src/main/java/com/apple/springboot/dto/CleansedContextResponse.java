package com.apple.springboot.dto;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record CleansedContextResponse(
        UUID id,
        String status,
        OffsetDateTime cleansedAt,
        CleansedContextMetadata metadata,
        Map<String, Object> context,
        int itemsCount
) {}
