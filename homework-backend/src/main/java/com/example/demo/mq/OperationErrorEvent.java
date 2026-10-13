package com.example.demo.mq;

import java.time.Instant;

/** An operation failure that will be persisted asynchronously by a consumer. */
public record OperationErrorEvent(
        String operation,
        String apiKey,
        String errorMessage,
        Instant occurredAt) {

    public static OperationErrorEvent from(String operation, String apiKey, RuntimeException exception) {
        String errorMessage = exception.getMessage() == null
                ? exception.getClass().getSimpleName()
                : exception.getMessage();
        return new OperationErrorEvent(operation, apiKey, errorMessage, Instant.now());
    }
}
