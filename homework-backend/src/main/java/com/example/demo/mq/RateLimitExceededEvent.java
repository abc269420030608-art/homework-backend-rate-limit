package com.example.demo.mq;

import java.time.Instant;

public record RateLimitExceededEvent(
        String eventType,
        String apiKey,
        long currentUsage,
        int limit,
        Instant occurredAt) {

    public static RateLimitExceededEvent from(String apiKey, long currentUsage, int limit) {
        return new RateLimitExceededEvent("RATE_LIMIT_EXCEEDED", apiKey, currentUsage, limit, Instant.now());
    }
}
