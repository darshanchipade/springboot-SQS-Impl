package com.apple.springboot.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DTO for the extracted and cleansed content to be returned in the API response.
 *
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExtractedContentOutput {
    private String sourcePath;
    private String originalFieldName; // New field: "copy" or "_path"
    private String cleansedContent;   // Renamed from cleansedCopy
    private String model;
}