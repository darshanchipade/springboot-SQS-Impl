package com.apple.springboot.service;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TextChunkingService {

    public List<String> chunkIfNeeded(String text) {
        if (text == null || text.trim().isEmpty()) {
            return List.of();
        }
        // Downstream search can handle full sections, so we skip sentence-level chunking.
        return List.of(text.trim());
    }
}