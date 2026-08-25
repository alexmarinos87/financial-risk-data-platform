-- Append-only terminal history for governed portfolio-risk notification retries.
-- Apply after sql/portfolio_risk_notification_delivery_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.portfolio_risk_notification_retry_executions (
    record_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-notification-retry-execution-record-v1'
    ),
    request_id TEXT NOT NULL UNIQUE,
    execution_id TEXT,
    plan_id TEXT NOT NULL,
    terminal_status TEXT NOT NULL CHECK (
        terminal_status IN (
            'completed',
            'failed_before_request',
            'failed_after_request',
            'persistence_uncertain'
        )
    ),
    failure_code TEXT CHECK (
        failure_code IS NULL
        OR failure_code IN (
            'overlap_error',
            'storage_error',
            'unexpected_error',
            'validation_error'
        )
    ),
    channel TEXT NOT NULL CHECK (channel = 'webhook'),
    endpoint_host TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    request_count INTEGER NOT NULL CHECK (
        request_count >= 0 AND request_count <= 100
    ),
    attempts_persisted INTEGER NOT NULL CHECK (
        attempts_persisted >= 0 AND attempts_persisted <= request_count
    ),
    succeeded_count INTEGER NOT NULL CHECK (
        succeeded_count >= 0 AND succeeded_count <= 100
    ),
    failed_count INTEGER NOT NULL CHECK (
        failed_count >= 0 AND failed_count <= 100
    ),
    attempt_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(attempt_ids_json) = 'array'
    ),
    requested_event_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(requested_event_ids_json) = 'array'
    ),
    persisted_event_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(persisted_event_ids_json) = 'array'
    ),
    delivery_fingerprint TEXT,
    retry_policy_fingerprint TEXT,
    retry_execution_policy_fingerprint TEXT,
    lock_model_version TEXT,
    lock_key_fingerprint TEXT,
    lock_acquired BOOLEAN,
    lock_released BOOLEAN,
    execution_summary_json JSONB,
    record_json JSONB NOT NULL CHECK (jsonb_typeof(record_json) = 'object'),
    document_sha256 TEXT NOT NULL CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (record_id <> ''),
    CHECK (request_id <> ''),
    CHECK (plan_id <> ''),
    CHECK (execution_id IS NULL OR execution_id <> ''),
    CHECK (endpoint_host IS NULL OR endpoint_host <> ''),
    CHECK (delivery_fingerprint IS NULL OR delivery_fingerprint <> ''),
    CHECK (retry_policy_fingerprint IS NULL OR retry_policy_fingerprint <> ''),
    CHECK (
        retry_execution_policy_fingerprint IS NULL
        OR retry_execution_policy_fingerprint <> ''
    ),
    CHECK (lock_model_version IS NULL OR lock_model_version <> ''),
    CHECK (lock_key_fingerprint IS NULL OR lock_key_fingerprint <> ''),
    CHECK (
        (lock_model_version IS NULL AND lock_key_fingerprint IS NULL)
        OR (lock_model_version IS NOT NULL AND lock_key_fingerprint IS NOT NULL)
    ),
    CHECK (lock_released IS NOT TRUE OR lock_acquired IS TRUE),
    CHECK (finished_at >= started_at),
    CHECK (recorded_at >= finished_at),
    CHECK (jsonb_array_length(attempt_ids_json) = attempts_persisted),
    CHECK (jsonb_array_length(requested_event_ids_json) = request_count),
    CHECK (jsonb_array_length(persisted_event_ids_json) = attempts_persisted),
    CHECK (succeeded_count + failed_count = attempts_persisted),
    CHECK (
        (terminal_status = 'completed' AND failure_code IS NULL)
        OR (terminal_status <> 'completed' AND failure_code IS NOT NULL)
    ),
    CHECK (
        terminal_status <> 'completed'
        OR (
            execution_id IS NOT NULL
            AND endpoint_host IS NOT NULL
            AND delivery_fingerprint IS NOT NULL
            AND retry_policy_fingerprint IS NOT NULL
            AND retry_execution_policy_fingerprint IS NOT NULL
            AND lock_model_version IS NOT NULL
            AND lock_key_fingerprint IS NOT NULL
            AND lock_acquired
            AND lock_released
            AND execution_summary_json IS NOT NULL
            AND request_count > 0
            AND request_count = attempts_persisted
            AND requested_event_ids_json = persisted_event_ids_json
        )
    ),
    CHECK (
        terminal_status <> 'failed_before_request'
        OR (
            execution_id IS NULL
            AND request_count = 0
            AND attempts_persisted = 0
            AND execution_summary_json IS NULL
        )
    ),
    CHECK (
        terminal_status <> 'failed_after_request'
        OR (
            execution_id IS NULL
            AND request_count > 0
            AND request_count = attempts_persisted
            AND requested_event_ids_json = persisted_event_ids_json
            AND execution_summary_json IS NULL
        )
    ),
    CHECK (
        terminal_status <> 'persistence_uncertain'
        OR (
            execution_id IS NULL
            AND request_count > attempts_persisted
            AND execution_summary_json IS NULL
        )
    ),
    CHECK (record_json ->> 'record_id' = record_id),
    CHECK (record_json ->> 'model_version' = model_version),
    CHECK (record_json ->> 'request_id' = request_id),
    CHECK (record_json ->> 'plan_id' = plan_id),
    CHECK (record_json ->> 'terminal_status' = terminal_status),
    CHECK (record_json -> 'attempt_ids' = attempt_ids_json),
    CHECK (record_json -> 'requested_event_ids' = requested_event_ids_json),
    CHECK (record_json -> 'persisted_event_ids' = persisted_event_ids_json),
    CHECK (record_json -> 'execution_summary' = execution_summary_json)
);

CREATE UNIQUE INDEX IF NOT EXISTS
idx_notification_retry_execution_id_unique
    ON risk_platform.portfolio_risk_notification_retry_executions (execution_id)
    WHERE execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_notification_retry_execution_history
    ON risk_platform.portfolio_risk_notification_retry_executions (
        recorded_at DESC,
        terminal_status,
        request_id
    );

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_retry_execution_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'portfolio_risk_notification_retry_executions is append-only';
END;
$$;

DROP TRIGGER IF EXISTS
trg_notification_retry_execution_append_only
ON risk_platform.portfolio_risk_notification_retry_executions;

CREATE TRIGGER trg_notification_retry_execution_append_only
BEFORE UPDATE OR DELETE
ON risk_platform.portfolio_risk_notification_retry_executions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_retry_execution_mutation();
