package com.example.demo.mq;

import com.example.demo.model.RateLimitRule;
import java.time.Instant;

/** Audit event for a successful rate-limit rule operation. */
public record RateLimitOperationEvent(
        String apiKey,
        String operation,
        Integer previousLimit,
        Long previousWindowSeconds,
        Integer newLimit,
        Long newWindowSeconds,
        Instant occurredAt) {

    public static RateLimitOperationEvent create(RateLimitRule rule) {
        return new RateLimitOperationEvent(
                rule.apiKey(), "CREATE", null, null, rule.limit(), rule.windowSeconds(), Instant.now());
    }

    public static RateLimitOperationEvent update(RateLimitRule previous, RateLimitRule current) {
        return new RateLimitOperationEvent(
                current.apiKey(), "UPDATE",
                previous.limit(), previous.windowSeconds(),
                current.limit(), current.windowSeconds(), Instant.now());
    }

    public static RateLimitOperationEvent delete(RateLimitRule rule) {
        return new RateLimitOperationEvent(
                rule.apiKey(), "DELETE", rule.limit(), rule.windowSeconds(), null, null, Instant.now());
    }
}
