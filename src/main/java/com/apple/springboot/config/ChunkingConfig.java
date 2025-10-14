package com.apple.springboot.config;

public class ChunkingConfig {
    public static final int LENGTH_THRESHOLD = 500; // chars
    public static final int SENTENCES_PER_CHUNK = 2;
    public static final int SENTENCE_OVERLAP = 1;
}