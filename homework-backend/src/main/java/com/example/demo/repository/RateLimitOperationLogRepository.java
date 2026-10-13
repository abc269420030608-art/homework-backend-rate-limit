package com.example.demo.repository;

import com.example.demo.mq.RateLimitOperationEvent;
import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

/** Persists an audit trail of successful rate-limit rule mutations. */
@Repository
public class RateLimitOperationLogRepository {

    private final JdbcTemplate jdbcTemplate;

    public RateLimitOperationLogRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(RateLimitOperationEvent event) {
        jdbcTemplate.update("""
                INSERT INTO rate_limit_operation_logs (
                    api_key, operation,
                    previous_request_limit, previous_window_seconds,
                    new_request_limit, new_window_seconds,
                    occurred_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """, event.apiKey(), event.operation(),
                event.previousLimit(), event.previousWindowSeconds(),
                event.newLimit(), event.newWindowSeconds(),
                Timestamp.from(event.occurredAt()));
    }
}
