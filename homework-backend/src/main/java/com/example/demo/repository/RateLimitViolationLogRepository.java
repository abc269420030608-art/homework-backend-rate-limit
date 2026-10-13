package com.example.demo.repository;

import java.sql.Timestamp;
import java.time.Instant;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Persists the first blocked request in each rate-limit window. */
@Repository
public class RateLimitViolationLogRepository {

    private final JdbcTemplate jdbcTemplate;

    public RateLimitViolationLogRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(String apiKey, long usageCount, Instant occurredAt) {
        jdbcTemplate.update("""
                INSERT INTO rate_limit_violation_logs (api_key, usage_count, occurred_at)
                VALUES (?, ?, ?)
                """, apiKey, usageCount, Timestamp.from(occurredAt));
    }
}
