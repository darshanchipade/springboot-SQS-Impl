package com.apple.springboot.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Composite key for the ContentHash entity.
 * A record is used here to automatically generate equals(), hashCode(), and toString().
 */
public record ContentHashId(String sourcePath, String itemType, String usagePath) implements Serializable {
}