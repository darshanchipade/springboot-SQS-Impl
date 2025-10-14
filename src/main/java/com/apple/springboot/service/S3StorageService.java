package com.apple.springboot.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@Service
public class S3StorageService {

    private static final Logger logger = LoggerFactory.getLogger(S3StorageService.class);

    private final S3Client s3Client;
    // private final String s3Region; // s3Region stored if needed for other methods, but client is configured with it.

    public S3StorageService(@Value("${app.s3.region}") String s3Region) {
        // this.s3Region = s3Region;
        S3Client client = null;
        try {
            logger.info("Initializing S3Client for region: {}", s3Region);
            if (s3Region == null || s3Region.trim().isEmpty()) {
                logger.error("S3 region is not configured. Please set 'app.s3.region' in application properties.");
                throw new IllegalArgumentException("S3 region must be configured.");
            }
            client = S3Client.builder()
                    .region(Region.of(s3Region))
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            logger.info("S3Client initialized successfully for region: {}", s3Region);
        } catch (Exception e) {
            logger.error("Failed to initialize S3Client for region {}: {}", s3Region, e.getMessage(), e);
            // This exception will prevent the bean from being created and app startup if S3 is critical.
            throw new RuntimeException("Failed to initialize S3Client: " + e.getMessage(), e);
        }
        this.s3Client = client;
    }

    public String downloadFileContent(String bucketName, String fileKey) {
        if (this.s3Client == null) {
            logger.error("S3Client is not initialized. Cannot download file {} from bucket {}.", fileKey, bucketName);
            throw new IllegalStateException("S3Client is not available. Check S3 configuration and application startup logs.");
        }

        logger.info("Attempting to download file s3://{}/{}", bucketName, fileKey);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(fileKey)
                .build();

        try (ResponseInputStream<GetObjectResponse> s3is = s3Client.getObject(getObjectRequest);
             InputStreamReader streamReader = new InputStreamReader(s3is, StandardCharsets.UTF_8);
             BufferedReader bufferedReader = new BufferedReader(streamReader)) {

            String content = bufferedReader.lines().collect(Collectors.joining(System.lineSeparator()));
            logger.info("Successfully downloaded and read file s3://{}/{}", bucketName, fileKey);
            return content;

        } catch (NoSuchKeyException e) {
            logger.warn("File not found in S3: s3://{}/{} (NoSuchKeyException)", bucketName, fileKey);
            return null;
        } catch (S3Exception e) {
            logger.error("S3Exception while downloading file s3://{}/{}: {}", bucketName, fileKey, e.getMessage(), e);
            throw new RuntimeException("S3 error while downloading file: " + e.getMessage(), e);
        } catch (IOException e) {
            logger.error("IOException while reading content from S3 file s3://{}/{}: {}", bucketName, fileKey, e.getMessage(), e);
            throw new RuntimeException("IO error while reading S3 file content: " + e.getMessage(), e);
        } catch (Exception e) {
            logger.error("Unexpected error while downloading file s3://{}/{}: {}", bucketName, fileKey, e.getMessage(), e);
            throw new RuntimeException("Unexpected error during S3 file download: " + e.getMessage(), e);
        }
    }
}