package com.apple.springboot.service;

import com.apple.springboot.config.ChunkingConfig;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class TextChunkingService {

    /**
     * Splits input text into chunks suitable for embedding.
     */
    public List<String> chunkIfNeeded(String text) {
        if (text == null || text.trim().isEmpty()) return List.of();

        if (text.length() <= ChunkingConfig.LENGTH_THRESHOLD) {
            return List.of(text.trim()); // short enough to embed directly
        } else {
            return chunkBySentences(text);
        }
    }

//    public List<String> chunkIfNeeded(String text) {
//        if (text == null || text.trim().isEmpty()) {
//            return List.of();
//        }
//        // Downstream search can handle full sections, so we skip sentence-level chunking.
//        return List.of(text.trim());
//    }
    /**
     * Breaks long text into sentence-based chunks with overlap.
     */
    private List<String> chunkBySentences(String text) {
        String[] rawSentences = text.split("(?<=[.!?])\\s+");
        List<String> sentences = new ArrayList<>();
        for (String sentence : rawSentences) {
            sentence = sentence.trim();
            if (!sentence.isEmpty()) sentences.add(sentence);
        }

        List<String> chunks = new ArrayList<>();
        for (int i = 0; i < sentences.size(); i += (ChunkingConfig.SENTENCES_PER_CHUNK - ChunkingConfig.SENTENCE_OVERLAP)) {
            StringBuilder chunk = new StringBuilder();
            for (int j = i; j < i + ChunkingConfig.SENTENCES_PER_CHUNK && j < sentences.size(); j++) {
                chunk.append(sentences.get(j)).append(" ");
            }
            chunks.add(chunk.toString().trim());
        }
        return chunks;
    }
}