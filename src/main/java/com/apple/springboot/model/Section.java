
package com.apple.springboot.model;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Base DTO for sections. Using @JsonTypeInfo and @JsonSubTypes to handle
 * different types of sections based on "_model".
 */
//@JsonTypeInfo(
//        use = JsonTypeInfo.Id.NAME,
//        include = JsonTypeInfo.As.PROPERTY,
//        property = "_model"
//)
//@JsonSubTypes({
//        @JsonSubTypes.Type(value = RibbonSection.class, name = "ribbon-section"),
//        @JsonSubTypes.Type(value = ChapterNavSection.class, name = "chapter-nav-section"),
//        @JsonSubTypes.Type(value = FeatureVideoHeaderSection.class, name = "feature-video-header-section"),
//        @JsonSubTypes.Type(value = FeatureCardSection.class, name = "feature-card-section")
//
// @JsonSubTypes.Type(value = ChapterNavSection.class, name = "chapter-nav-section"),
// @JsonSubTypes.Type(value = VideoHeaderSection.class, name = "feature-video-header-section"),
// @JsonSubTypes.Type(value = FeatureCardSection.class, name = "feature-card-section")
//})
@Data
public class Section {
//    @JsonProperty("_path")
//    private String path;
//    @JsonProperty("_model")
//    private String model;
//    @JsonProperty("_modelname")
//    private String modelName;

    // This map will capture any properties not explicitly defined above.
    // Useful when you want to dynamically traverse without strict DTOs for every field.
//    private Map<String, Object> additionalProperties = new HashMap<>();
//
//    @JsonAnySetter
//    public void setAdditionalProperty(String name, Object value) {
//        this.additionalProperties.put(name, value);
  //  }
}
