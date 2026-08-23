-- Append-only delivery-attempt evidence for portfolio risk notifications.
-- Apply after sql/portfolio_risk_notification_outbox_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.portfolio_risk_notification_delivery_attempts (
    attempt_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    event_id TEXT NOT NULL REFERENCES
        risk_platform.portfolio_risk_notification_outbox (event_id),
    channel TEXT NOT NULL CHECK (channel = 'webhook'),
    attempt_number INTEGER NOT NULL CHECK (
        attempt_number >= 1 AND attempt_number <= 10
    ),
    idempotency_key TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed')),
    http_status INTEGER CHECK (http_status BETWEEN 100 AND 599),
    error_code TEXT,
    endpoint_host TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, channel, attempt_number),
    CHECK (attempt_id <> ''),
    CHECK (model_version <> ''),
    CHECK (event_id <> ''),
    CHECK (idempotency_key = event_id),
    CHECK (endpoint_host <> ''),
    CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (
        (outcome = 'succeeded'
            AND http_status BETWEEN 200 AND 299
            AND error_code IS NULL)
        OR
        (outcome = 'failed'
            AND error_code IS NOT NULL)
    ),
    CHECK (
        model_version <> 'portfolio-risk-webhook-delivery-v1'
        OR channel = 'webhook'
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS
idx_portfolio_notification_delivery_one_success
    ON risk_platform.portfolio_risk_notification_delivery_attempts (
        event_id,
        channel
    )
    WHERE outcome = 'succeeded';

CREATE INDEX IF NOT EXISTS idx_portfolio_notification_delivery_history
    ON risk_platform.portfolio_risk_notification_delivery_attempts (
        event_id,
        channel,
        attempt_number DESC,
        attempted_at DESC
    );

CREATE OR REPLACE VIEW
risk_platform.portfolio_risk_notification_delivery_status AS
SELECT
    pending.event_id,
    pending.event_type,
    pending.transition_type,
    pending.policy_id,
    pending.policy_fingerprint,
    pending.portfolio_id,
    pending.definition_fingerprint,
    pending.metric_name,
    pending.subject_key,
    pending.current_status,
    pending.ts_event,
    COUNT(attempt.attempt_id) AS attempt_count,
    COUNT(attempt.attempt_id) FILTER (
        WHERE attempt.outcome = 'failed'
    ) AS failed_attempt_count,
    COALESCE(
        BOOL_OR(attempt.outcome = 'succeeded'),
        FALSE
    ) AS delivered,
    MAX(attempt.attempted_at) AS last_attempted_at,
    MAX(attempt.http_status) FILTER (
        WHERE attempt.attempt_number = (
            SELECT MAX(latest.attempt_number)
            FROM risk_platform.portfolio_risk_notification_delivery_attempts latest
            WHERE latest.event_id = pending.event_id
              AND latest.channel = 'webhook'
        )
    ) AS last_http_status,
    MAX(attempt.error_code) FILTER (
        WHERE attempt.attempt_number = (
            SELECT MAX(latest.attempt_number)
            FROM risk_platform.portfolio_risk_notification_delivery_attempts latest
            WHERE latest.event_id = pending.event_id
              AND latest.channel = 'webhook'
        )
    ) AS last_error_code
FROM risk_platform.portfolio_risk_notification_pending pending
LEFT JOIN risk_platform.portfolio_risk_notification_delivery_attempts attempt
  ON attempt.event_id = pending.event_id
 AND attempt.channel = 'webhook'
GROUP BY
    pending.event_id,
    pending.event_type,
    pending.transition_type,
    pending.policy_id,
    pending.policy_fingerprint,
    pending.portfolio_id,
    pending.definition_fingerprint,
    pending.metric_name,
    pending.subject_key,
    pending.current_status,
    pending.ts_event;

CREATE OR REPLACE VIEW
risk_platform.portfolio_risk_notification_delivery_pending AS
SELECT *
FROM risk_platform.portfolio_risk_notification_delivery_status
WHERE NOT delivered;

CREATE OR REPLACE VIEW
risk_platform.portfolio_risk_notification_delivery_succeeded AS
SELECT *
FROM risk_platform.portfolio_risk_notification_delivery_status
WHERE delivered;
