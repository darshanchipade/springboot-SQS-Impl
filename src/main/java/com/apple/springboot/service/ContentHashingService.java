package com.apple.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

@Service
public class ContentHashingService {

    private static final Logger logger = LoggerFactory.getLogger(ContentHashingService.class);

    private final MessageDigest digest;

    /**
     * Initializes the SHA-256 message digest used for content hashing.
     */
    public ContentHashingService() {
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            logger.error("Could not initialize SHA-256 MessageDigest", e);
            throw new RuntimeException("Failed to initialize hashing service", e);
        }
    }

    /**
     * Calculates the SHA-256 hash of a given string content.
     *
     * @param content The string content to hash.
     * @return The SHA-256 hash as a hexadecimal string.
     */
    public synchronized String hash(String content) {
        if (content == null) {
            return null;
        }
        byte[] encodedhash = digest.digest(content.getBytes(StandardCharsets.UTF_8));
        return bytesToHex(encodedhash);
    }

    /**
     * Converts a raw byte array into a lowercase hexadecimal string.
     */
    private String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (byte b : hash) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}