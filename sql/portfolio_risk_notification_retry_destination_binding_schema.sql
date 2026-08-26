-- Append-only destination authority bindings for terminal notification retry history.
-- Apply after sql/portfolio_risk_notification_retry_execution_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.portfolio_risk_notification_retry_destination_bindings (
    binding_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-notification-retry-destination-binding-v1'
    ),
    record_id TEXT NOT NULL UNIQUE REFERENCES
        risk_platform.portfolio_risk_notification_retry_executions (record_id),
    request_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    execution_id TEXT,
    authority_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    destination_fingerprint TEXT NOT NULL,
    endpoint_environment_variable TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    evaluated_event_types_json JSONB NOT NULL CHECK (
        jsonb_typeof(evaluated_event_types_json) = 'array'
    ),
    authority_json JSONB NOT NULL CHECK (
        jsonb_typeof(authority_json) = 'object'
    ),
    recorded_at TIMESTAMPTZ NOT NULL,
    binding_json JSONB NOT NULL CHECK (
        jsonb_typeof(binding_json) = 'object'
    ),
    document_sha256 TEXT NOT NULL CHECK (
        document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (binding_id <> ''),
    CHECK (record_id <> ''),
    CHECK (request_id <> ''),
    CHECK (plan_id <> ''),
    CHECK (execution_id IS NULL OR execution_id <> ''),
    CHECK (authority_id <> ''),
    CHECK (destination_id <> ''),
    CHECK (destination_fingerprint <> ''),
    CHECK (endpoint_environment_variable ~ '^[A-Z][A-Z0-9_]{2,127}$'),
    CHECK (recorded_at >= evaluated_at),
    CHECK (binding_json ->> 'binding_id' = binding_id),
    CHECK (binding_json ->> 'model_version' = model_version),
    CHECK (binding_json ->> 'record_id' = record_id),
    CHECK (binding_json ->> 'request_id' = request_id),
    CHECK (binding_json ->> 'plan_id' = plan_id),
    CHECK (binding_json -> 'destination_authority' = authority_json),
    CHECK (authority_json ->> 'authority_id' = authority_id),
    CHECK (authority_json ->> 'destination_id' = destination_id),
    CHECK (
        authority_json ->> 'destination_fingerprint' = destination_fingerprint
    ),
    CHECK (
        authority_json ->> 'endpoint_environment_variable'
        = endpoint_environment_variable
    ),
    CHECK (
        authority_json -> 'evaluated_event_types'
        = evaluated_event_types_json
    ),
    CHECK ((authority_json ->> 'active')::BOOLEAN),
    CHECK (authority_json ->> 'channel' = 'webhook')
);

CREATE INDEX IF NOT EXISTS idx_notification_retry_destination_history
    ON risk_platform.portfolio_risk_notification_retry_destination_bindings (
        destination_id,
        evaluated_at DESC,
        record_id
    );

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_retry_destination_binding_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'portfolio_risk_notification_retry_destination_bindings is append-only';
END;
$$;

DROP TRIGGER IF EXISTS
trg_notification_retry_destination_binding_append_only
ON risk_platform.portfolio_risk_notification_retry_destination_bindings;

CREATE TRIGGER trg_notification_retry_destination_binding_append_only
BEFORE UPDATE OR DELETE
ON risk_platform.portfolio_risk_notification_retry_destination_bindings
FOR EACH ROW
EXECUTE FUNCTION
    risk_platform.reject_notification_retry_destination_binding_mutation();
