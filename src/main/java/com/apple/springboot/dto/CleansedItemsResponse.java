package com.apple.springboot.dto;

import java.util.List;

public record CleansedItemsResponse(List<CleansedItemRow> items) {}
