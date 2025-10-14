package com.apple.springboot.config;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {

    @Value("${aws.bedrock.rateLimit:5.0}") // Default to 5 permits per second
    private double rateLimit;

    @Bean
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter bedrockRateLimiter() {
        // Allow a configurable number of permits per second
        return RateLimiter.create(rateLimit);
    }
}