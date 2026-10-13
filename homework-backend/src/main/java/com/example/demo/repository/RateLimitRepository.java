package com.example.demo.repository;

import com.example.demo.model.RateLimitRule;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class RateLimitRepository {

    private final JdbcTemplate jdbcTemplate;

    public RateLimitRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public RateLimitRule save(RateLimitRule rule) {
        jdbcTemplate.update("""
                INSERT INTO rate_limits (api_key, request_limit, window_seconds, rule_version)
                VALUES (?, ?, ?, 1)
                ON DUPLICATE KEY UPDATE
                    request_limit = VALUES(request_limit),
                    window_seconds = VALUES(window_seconds),
                    rule_version = rule_version + 1
                """, rule.apiKey(), rule.limit(), rule.windowSeconds());
        return findByApiKey(rule.apiKey())
                .orElseThrow(() -> new IllegalStateException("Saved rule could not be read"));
    }

    public Optional<RateLimitRule> findByApiKey(String apiKey) {
        List<RateLimitRule> rules = jdbcTemplate.query("""
                SELECT api_key, request_limit, window_seconds, rule_version
                FROM rate_limits WHERE api_key = ?
                """, this::mapRow, apiKey);
        return rules.stream().findFirst();
    }

    public List<RateLimitRule> findPage(int size, int offset) {
        return jdbcTemplate.query("""
                SELECT api_key, request_limit, window_seconds, rule_version
                FROM rate_limits ORDER BY api_key LIMIT ? OFFSET ?
                """, this::mapRow, size, offset);
    }

    public long count() {
        Long total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM rate_limits", Long.class);
        return total == null ? 0 : total;
    }

    public boolean deleteByApiKey(String apiKey) {
        return jdbcTemplate.update("DELETE FROM rate_limits WHERE api_key = ?", apiKey) > 0;
    }

    private RateLimitRule mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
        return new RateLimitRule(
                resultSet.getString("api_key"),
                resultSet.getInt("request_limit"),
                resultSet.getLong("window_seconds"),
                resultSet.getLong("rule_version"));
    }
}
