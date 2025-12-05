package com.apple.springboot.dto;

import java.time.OffsetDateTime;
import java.util.UUID;

public record CleansedContextMetadata(
        UUID rawDataId,
        String sourceUri,
        String sourceLabel,
        String fileName,
        long sizeInBytes,
        OffsetDateTime uploadedAt,
        String sourceType,
        String sourceIdentifier
) {}
