package com.example.demo.model;

public record RateLimitRule(String apiKey, int limit, long windowSeconds, long version) {

    public RateLimitRule(String apiKey, int limit, long windowSeconds) {
        this(apiKey, limit, windowSeconds, 0);
    }
}
