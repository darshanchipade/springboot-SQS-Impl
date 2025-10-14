package com.apple.springboot.model;

public class ContentChunkWithDistance {
    private ContentChunk contentChunk;
    private double distance;

    public ContentChunkWithDistance(ContentChunk contentChunk, double distance) {
        this.contentChunk = contentChunk;
        this.distance = distance;
    }
    public ContentChunk getContentChunk() { return contentChunk; }
    public void setContentChunk(ContentChunk contentChunk) { this.contentChunk = contentChunk; }
    public double getDistance() { return distance; }
    public void setDistance(double distance) { this.distance = distance; }
}