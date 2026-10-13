package com.example.demo.repository;

import com.example.demo.model.RateLimitOutboxEvent;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RateLimitOutboxRepository {

    private final JdbcTemplate jdbcTemplate;

    public RateLimitOutboxRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void save(RateLimitOutboxEvent event) {
        jdbcTemplate.update("""
                INSERT INTO rate_limit_outbox (id, api_key, rule_version, window_seconds)
                VALUES (?, ?, ?, ?)
                """, event.id().toString(), event.apiKey(), event.ruleVersion(), event.windowSeconds());
    }

    public List<RateLimitOutboxEvent> findPending(int limit) {
        return jdbcTemplate.query("""
                SELECT id, api_key, rule_version, window_seconds
                FROM rate_limit_outbox
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT ?
                """, this::mapRow, limit);
    }

    public void markCompleted(UUID id) {
        jdbcTemplate.update("""
                UPDATE rate_limit_outbox
                SET status = 'COMPLETED', processed_at = CURRENT_TIMESTAMP, last_error = NULL
                WHERE id = ?
                """, id.toString());
    }

    public void recordFailure(UUID id, RuntimeException exception) {
        jdbcTemplate.update("""
                UPDATE rate_limit_outbox
                SET retry_count = retry_count + 1, last_error = ?
                WHERE id = ?
                """, exception.getMessage(), id.toString());
    }

    private RateLimitOutboxEvent mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RateLimitOutboxEvent(
                UUID.fromString(resultSet.getString("id")),
                resultSet.getString("api_key"),
                resultSet.getLong("rule_version"),
                resultSet.getLong("window_seconds"));
    }
}
