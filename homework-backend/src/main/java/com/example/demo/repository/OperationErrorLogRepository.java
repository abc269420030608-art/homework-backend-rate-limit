package com.example.demo.repository;

import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Stores failures from rate-limit rule mutations for later diagnosis. */
@Repository
public class OperationErrorLogRepository {

    private final JdbcTemplate jdbcTemplate;

    public OperationErrorLogRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(String operation, String apiKey, String errorMessage, Instant occurredAt) {
        jdbcTemplate.update("""
                INSERT INTO operation_error_logs (operation, api_key, error_message, occurred_at)
                VALUES (?, ?, ?, ?)
                """, operation, apiKey, errorMessage, Timestamp.from(occurredAt));
    }
}
