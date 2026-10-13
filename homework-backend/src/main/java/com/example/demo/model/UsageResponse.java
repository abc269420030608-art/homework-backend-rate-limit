package com.example.demo.model;

public record UsageResponse(
        String apiKey,
        long currentUsage,
        long remaining,
        long ttlSeconds) {
}
