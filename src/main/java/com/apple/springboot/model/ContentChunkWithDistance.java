package com.apple.springboot.model;

public class ContentChunkWithDistance {
    private ContentChunk contentChunk;
    private double distance;

    /**
     * Creates a wrapper for a content chunk with distance metadata.
     */
    public ContentChunkWithDistance(ContentChunk contentChunk, double distance) {
        this.contentChunk = contentChunk;
        this.distance = distance;
    }
    /**
     * Returns the content chunk.
     */
    public ContentChunk getContentChunk() { return contentChunk; }
    /**
     * Updates the content chunk reference.
     */
    public void setContentChunk(ContentChunk contentChunk) { this.contentChunk = contentChunk; }
    /**
     * Returns the computed distance score.
     */
    public double getDistance() { return distance; }
    /**
     * Updates the distance score.
     */
    public void setDistance(double distance) { this.distance = distance; }
}