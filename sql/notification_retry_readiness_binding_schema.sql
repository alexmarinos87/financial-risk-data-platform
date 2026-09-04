-- Append-only readiness bindings for terminal notification retry history.
-- Apply after notification_execution_readiness_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.notification_retry_readiness_bindings (
    binding_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-notification-retry-readiness-binding-v1'
    ),
    terminal_record_id TEXT NOT NULL UNIQUE REFERENCES
        risk_platform.portfolio_risk_notification_retry_executions (record_id),
    terminal_request_id TEXT NOT NULL,
    terminal_plan_id TEXT NOT NULL,
    terminal_execution_id TEXT,
    terminal_status TEXT NOT NULL CHECK (
        terminal_status IN (
            'completed',
            'failed_before_request',
            'failed_after_request',
            'persistence_uncertain'
        )
    ),
    terminal_started_at TIMESTAMPTZ NOT NULL,
    terminal_finished_at TIMESTAMPTZ NOT NULL,
    terminal_recorded_at TIMESTAMPTZ NOT NULL,
    terminal_request_count INTEGER NOT NULL CHECK (
        terminal_request_count BETWEEN 0 AND 100
    ),
    terminal_attempts_persisted INTEGER NOT NULL CHECK (
        terminal_attempts_persisted BETWEEN 0 AND terminal_request_count
    ),
    terminal_document_sha256 CHAR(64) NOT NULL CHECK (
        terminal_document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    readiness_record_id TEXT NOT NULL REFERENCES
        risk_platform.notification_execution_readiness_decisions (record_id),
    readiness_request_id TEXT NOT NULL,
    retained_decision_id TEXT NOT NULL REFERENCES
        risk_platform.notification_execution_readiness_decisions (decision_id),
    refreshed_decision_id TEXT NOT NULL,
    enforcement_id TEXT NOT NULL UNIQUE,
    destination_id TEXT NOT NULL,
    execution_kind TEXT NOT NULL CHECK (execution_kind = 'retry'),
    enforced_at TIMESTAMPTZ NOT NULL,
    retained_decision_evaluated_at TIMESTAMPTZ NOT NULL,
    refreshed_decision_evaluated_at TIMESTAMPTZ NOT NULL,
    lock_model_version TEXT NOT NULL,
    lock_scope TEXT NOT NULL,
    lock_key_fingerprint TEXT NOT NULL,
    readiness_enforcement_sha256 CHAR(64) NOT NULL CHECK (
        readiness_enforcement_sha256 ~ '^[0-9a-f]{64}$'
    ),
    binding_recorded_at TIMESTAMPTZ NOT NULL,
    readiness_enforcement_json JSONB NOT NULL CHECK (
        jsonb_typeof(readiness_enforcement_json) = 'object'
    ),
    binding_json JSONB NOT NULL CHECK (
        jsonb_typeof(binding_json) = 'object'
    ),
    document_sha256 CHAR(64) NOT NULL CHECK (
        document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (binding_id <> ''),
    CHECK (terminal_record_id <> ''),
    CHECK (terminal_request_id <> ''),
    CHECK (terminal_plan_id <> ''),
    CHECK (terminal_execution_id IS NULL OR terminal_execution_id <> ''),
    CHECK (readiness_record_id <> ''),
    CHECK (readiness_request_id <> ''),
    CHECK (retained_decision_id <> ''),
    CHECK (refreshed_decision_id <> ''),
    CHECK (enforcement_id <> ''),
    CHECK (destination_id <> ''),
    CHECK (lock_model_version <> ''),
    CHECK (lock_scope <> ''),
    CHECK (lock_key_fingerprint <> ''),
    CHECK (
        terminal_started_at <= enforced_at
        AND enforced_at <= terminal_finished_at
    ),
    CHECK (terminal_started_at <= terminal_finished_at),
    CHECK (terminal_finished_at <= terminal_recorded_at),
    CHECK (terminal_recorded_at <= binding_recorded_at),
    CHECK (refreshed_decision_evaluated_at = enforced_at),
    CHECK (retained_decision_evaluated_at <= enforced_at),
    CHECK (binding_json ->> 'binding_id' = binding_id),
    CHECK (binding_json ->> 'model_version' = model_version),
    CHECK (
        binding_json -> 'terminal_execution' ->> 'record_id'
            = terminal_record_id
    ),
    CHECK (
        binding_json -> 'terminal_execution' ->> 'request_id'
            = terminal_request_id
    ),
    CHECK (
        binding_json -> 'terminal_execution' ->> 'plan_id'
            = terminal_plan_id
    ),
    CHECK (
        binding_json -> 'terminal_execution' ->> 'terminal_status'
            = terminal_status
    ),
    CHECK (
        binding_json -> 'terminal_execution' ->> 'document_sha256'
            = terminal_document_sha256
    ),
    CHECK (
        binding_json ->> 'readiness_enforcement_sha256'
            = readiness_enforcement_sha256
    ),
    CHECK (
        binding_json -> 'readiness_enforcement'
            = readiness_enforcement_json
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'readiness_record_id'
            = readiness_record_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'readiness_request_id'
            = readiness_request_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'retained_decision_id'
            = retained_decision_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'refreshed_decision_id'
            = refreshed_decision_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'enforcement_id'
            = enforcement_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'destination_id'
            = destination_id
    ),
    CHECK (
        binding_json -> 'readiness_enforcement' ->> 'execution_kind'
            = execution_kind
    ),
    CHECK (
        (binding_json ->> 'recorded_at')::TIMESTAMPTZ
            = binding_recorded_at
    ),
    CHECK (
        (readiness_enforcement_json ->> 'execution_ready')::BOOLEAN
    ),
    CHECK (
        readiness_enforcement_json ->> 'readiness_review_status' = 'allowed'
    ),
    CHECK (
        (readiness_enforcement_json ->> 'substantive_evidence_match')::BOOLEAN
    )
);

CREATE INDEX IF NOT EXISTS idx_notification_retry_readiness_destination
ON risk_platform.notification_retry_readiness_bindings (
    destination_id,
    enforced_at DESC,
    terminal_record_id
);

CREATE INDEX IF NOT EXISTS idx_notification_retry_readiness_decision
ON risk_platform.notification_retry_readiness_bindings (
    readiness_record_id,
    retained_decision_id
);

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_retry_readiness_binding_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'notification retry readiness bindings are append-only';
END;
$$;

DROP TRIGGER IF EXISTS notification_retry_readiness_binding_reject_update
ON risk_platform.notification_retry_readiness_bindings;
CREATE TRIGGER notification_retry_readiness_binding_reject_update
BEFORE UPDATE ON risk_platform.notification_retry_readiness_bindings
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.reject_notification_retry_readiness_binding_mutation();

DROP TRIGGER IF EXISTS notification_retry_readiness_binding_reject_delete
ON risk_platform.notification_retry_readiness_bindings;
CREATE TRIGGER notification_retry_readiness_binding_reject_delete
BEFORE DELETE ON risk_platform.notification_retry_readiness_bindings
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.reject_notification_retry_readiness_binding_mutation();

CREATE OR REPLACE VIEW
risk_platform.notification_retry_readiness_binding_status AS
SELECT
    terminal.record_id AS terminal_record_id,
    terminal.request_id AS terminal_request_id,
    terminal.plan_id AS terminal_plan_id,
    terminal.execution_id AS terminal_execution_id,
    terminal.terminal_status,
    terminal.started_at AS terminal_started_at,
    terminal.finished_at AS terminal_finished_at,
    terminal.recorded_at AS terminal_recorded_at,
    terminal.request_count AS terminal_request_count,
    terminal.attempts_persisted AS terminal_attempts_persisted,
    terminal.document_sha256 AS terminal_document_sha256,
    binding.binding_id,
    binding.readiness_record_id,
    binding.readiness_request_id,
    binding.retained_decision_id,
    binding.refreshed_decision_id,
    binding.enforcement_id,
    binding.destination_id,
    binding.enforced_at,
    binding.binding_recorded_at,
    binding.document_sha256 AS binding_document_sha256,
    CASE
        WHEN binding.binding_id IS NULL THEN 'binding_missing'
        ELSE 'bound'
    END AS readiness_binding_status,
    binding.binding_id IS NULL AS readiness_binding_review_required
FROM risk_platform.portfolio_risk_notification_retry_executions terminal
LEFT JOIN risk_platform.notification_retry_readiness_bindings binding
  ON binding.terminal_record_id = terminal.record_id;

CREATE OR REPLACE VIEW
risk_platform.notification_retry_readiness_bound AS
SELECT *
FROM risk_platform.notification_retry_readiness_binding_status
WHERE readiness_binding_status = 'bound';

CREATE OR REPLACE VIEW
risk_platform.notification_retry_readiness_binding_missing AS
SELECT *
FROM risk_platform.notification_retry_readiness_binding_status
WHERE readiness_binding_status = 'binding_missing';
