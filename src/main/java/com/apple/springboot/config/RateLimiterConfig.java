package com.apple.springboot.config;

import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RateLimiterConfig {

    @Value("${aws.bedrock.rateLimit:5.0}") // Default to 5 permits per second
    private double rateLimit;
    @Value("${app.ratelimit.chatQps:0}")
    private double chatRateLimit;
    @Value("${app.ratelimit.embedQps:0}")
    private double embedRateLimit;

    @Bean
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter bedrockRateLimiter() {
        // Allow a configurable number of permits per second
        return RateLimiter.create(Math.max(0.1, rateLimit));
    }

    @Bean("chatRateLimiter")
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter chatRateLimiter() {
        return createOptionalLimiter(chatRateLimit);
    }

    @Bean("embedRateLimiter")
    @SuppressWarnings("UnstableApiUsage")
    public RateLimiter embedRateLimiter() {
        return createOptionalLimiter(embedRateLimit);
    }

    private RateLimiter createOptionalLimiter(double qps) {
        double effectiveQps = qps > 0 ? qps : Double.MAX_VALUE;
        return RateLimiter.create(effectiveQps);
    }
}