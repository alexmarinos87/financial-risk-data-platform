-- Append-only terminal evidence for governed portfolio-risk notification retries.
-- Apply after sql/portfolio_risk_notification_delivery_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.portfolio_risk_notification_retry_executions (
    record_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-notification-retry-execution-record-v1'
    ),
    request_id TEXT NOT NULL UNIQUE,
    plan_id TEXT NOT NULL,
    execution_id TEXT UNIQUE,
    terminal_status TEXT NOT NULL CHECK (
        terminal_status IN (
            'completed',
            'failed_before_request',
            'failed_after_request',
            'persistence_uncertain'
        )
    ),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    failure_stage TEXT,
    failure_code TEXT,
    delivery_fingerprint TEXT NOT NULL,
    retry_policy_fingerprint TEXT NOT NULL,
    retry_execution_policy_fingerprint TEXT NOT NULL,
    delivery_lock_model_version TEXT NOT NULL,
    delivery_lock_key_fingerprint TEXT NOT NULL,
    requested_event_count INTEGER NOT NULL CHECK (
        requested_event_count >= 0 AND requested_event_count <= 100
    ),
    persisted_event_count INTEGER NOT NULL CHECK (
        persisted_event_count >= 0 AND persisted_event_count <= 100
    ),
    requested_event_ids JSONB NOT NULL,
    persisted_event_ids JSONB NOT NULL,
    persisted_attempt_ids JSONB NOT NULL,
    execution_json JSONB,
    record_json JSONB NOT NULL,
    document_sha256 TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (record_id <> ''),
    CHECK (request_id <> ''),
    CHECK (plan_id <> ''),
    CHECK (finished_at >= started_at),
    CHECK (delivery_fingerprint <> ''),
    CHECK (retry_policy_fingerprint <> ''),
    CHECK (retry_execution_policy_fingerprint <> ''),
    CHECK (delivery_lock_model_version <> ''),
    CHECK (delivery_lock_key_fingerprint ~ '^[0-9a-f]{24}$'),
    CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(requested_event_ids) = 'array'),
    CHECK (jsonb_typeof(persisted_event_ids) = 'array'),
    CHECK (jsonb_typeof(persisted_attempt_ids) = 'array'),
    CHECK (jsonb_typeof(record_json) = 'object'),
    CHECK (
        execution_json IS NULL OR jsonb_typeof(execution_json) = 'object'
    ),
    CHECK (requested_event_count = jsonb_array_length(requested_event_ids)),
    CHECK (persisted_event_count = jsonb_array_length(persisted_event_ids)),
    CHECK (
        persisted_event_count = jsonb_array_length(persisted_attempt_ids)
    ),
    CHECK (persisted_event_count <= requested_event_count),
    CHECK (
        (terminal_status = 'completed'
            AND execution_id IS NOT NULL
            AND failure_stage IS NULL
            AND failure_code IS NULL
            AND execution_json IS NOT NULL
            AND requested_event_count > 0
            AND requested_event_count = persisted_event_count)
        OR
        (terminal_status = 'failed_before_request'
            AND execution_id IS NULL
            AND failure_stage IS NOT NULL
            AND failure_code IS NOT NULL
            AND execution_json IS NULL
            AND requested_event_count = 0
            AND persisted_event_count = 0)
        OR
        (terminal_status = 'failed_after_request'
            AND execution_id IS NULL
            AND failure_stage IS NOT NULL
            AND failure_code IS NOT NULL
            AND execution_json IS NULL
            AND requested_event_count > 0
            AND requested_event_count = persisted_event_count)
        OR
        (terminal_status = 'persistence_uncertain'
            AND execution_id IS NULL
            AND failure_stage IS NOT NULL
            AND failure_code IS NOT NULL
            AND execution_json IS NULL
            AND requested_event_count > persisted_event_count)
    )
);

CREATE INDEX IF NOT EXISTS
idx_notification_retry_execution_recent
    ON risk_platform.portfolio_risk_notification_retry_executions (
        finished_at DESC,
        record_id DESC
    );

CREATE INDEX IF NOT EXISTS
idx_notification_retry_execution_plan
    ON risk_platform.portfolio_risk_notification_retry_executions (
        plan_id,
        finished_at DESC,
        record_id DESC
    );

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_retry_execution_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'portfolio risk notification retry execution history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS
reject_notification_retry_execution_update
ON risk_platform.portfolio_risk_notification_retry_executions;

CREATE TRIGGER reject_notification_retry_execution_update
BEFORE UPDATE ON risk_platform.portfolio_risk_notification_retry_executions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_retry_execution_mutation();

DROP TRIGGER IF EXISTS
reject_notification_retry_execution_delete
ON risk_platform.portfolio_risk_notification_retry_executions;

CREATE TRIGGER reject_notification_retry_execution_delete
BEFORE DELETE ON risk_platform.portfolio_risk_notification_retry_executions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_retry_execution_mutation();

CREATE OR REPLACE VIEW
risk_platform.recent_notification_retry_executions AS
SELECT
    record_id,
    request_id,
    plan_id,
    execution_id,
    terminal_status,
    started_at,
    finished_at,
    failure_stage,
    failure_code,
    requested_event_count,
    persisted_event_count,
    delivery_lock_model_version,
    delivery_lock_key_fingerprint,
    document_sha256,
    loaded_at
FROM risk_platform.portfolio_risk_notification_retry_executions
ORDER BY finished_at DESC, record_id DESC;
