package com.example.demo.model;

import java.util.UUID;

/** A durable request to bring one Redis counter in sync with a stored rule. */
public record RateLimitOutboxEvent(UUID id, String apiKey, long ruleVersion, long windowSeconds) {
}
