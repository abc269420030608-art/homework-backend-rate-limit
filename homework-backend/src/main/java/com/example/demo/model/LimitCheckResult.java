package com.example.demo.model;

public record LimitCheckResult(
        String apiKey,
        boolean allowed,
        long currentUsage,
        long remaining,
        long ttlSeconds) {
}
