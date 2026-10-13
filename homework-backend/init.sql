
CREATE TABLE IF NOT EXISTS rate_limits (
    api_key VARCHAR(128) NOT NULL PRIMARY KEY,
    request_limit INT NOT NULL,
    window_seconds BIGINT NOT NULL,
    rule_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT chk_request_limit_positive CHECK (request_limit > 0),
    CONSTRAINT chk_window_seconds_positive CHECK (window_seconds > 0)
);

CREATE TABLE IF NOT EXISTS rate_limit_outbox (
    id CHAR(36) NOT NULL PRIMARY KEY,
    api_key VARCHAR(128) NOT NULL,
    rule_version BIGINT NOT NULL,
    window_seconds BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    retry_count INT NOT NULL DEFAULT 0,
    last_error TEXT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    INDEX idx_rate_limit_outbox_status_created_at (status, created_at)
);

CREATE TABLE IF NOT EXISTS operation_error_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    operation VARCHAR(32) NOT NULL,
    api_key VARCHAR(128),
    error_message TEXT NOT NULL,
    occurred_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS rate_limit_violation_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    api_key VARCHAR(128) NOT NULL,
    usage_count BIGINT NOT NULL,
    occurred_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_limit_operation_logs (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    api_key VARCHAR(128) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    previous_request_limit INT,
    previous_window_seconds BIGINT,
    new_request_limit INT,
    new_window_seconds BIGINT,
    occurred_at TIMESTAMP NOT NULL
);
