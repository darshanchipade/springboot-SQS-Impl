package com.apple.springboot.model;

import java.io.Serializable;

public record ContentHashId(String sourcePath, String itemType) implements Serializable {}