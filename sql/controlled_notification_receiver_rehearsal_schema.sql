-- Append-only notification activation checklists and controlled receiver rehearsals.
-- Apply after notification retry destination follow-up serving views.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.notification_activation_checklists (
    checklist_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version = 'portfolio-risk-notification-activation-checklist-v1'
    ),
    destination_id TEXT NOT NULL,
    destination_fingerprint TEXT NOT NULL,
    authority_id TEXT NOT NULL,
    reviewed_at TIMESTAMPTZ NOT NULL,
    review_expires_at TIMESTAMPTZ NOT NULL,
    reviewed_by_json JSONB NOT NULL CHECK (
        jsonb_typeof(reviewed_by_json) = 'array'
    ),
    controls_json JSONB NOT NULL CHECK (
        jsonb_typeof(controls_json) = 'object'
    ),
    activation_ready BOOLEAN NOT NULL,
    checklist_json JSONB NOT NULL CHECK (
        jsonb_typeof(checklist_json) = 'object'
    ),
    document_sha256 TEXT NOT NULL CHECK (
        document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (checklist_id <> ''),
    CHECK (destination_id <> ''),
    CHECK (destination_fingerprint <> ''),
    CHECK (authority_id <> ''),
    CHECK (review_expires_at > reviewed_at),
    CHECK (jsonb_array_length(reviewed_by_json) BETWEEN 1 AND 16),
    CHECK (checklist_json ->> 'checklist_id' = checklist_id),
    CHECK (checklist_json ->> 'model_version' = model_version),
    CHECK (checklist_json ->> 'destination_id' = destination_id),
    CHECK (
        checklist_json ->> 'destination_fingerprint'
        = destination_fingerprint
    ),
    CHECK (checklist_json ->> 'authority_id' = authority_id),
    CHECK (checklist_json -> 'reviewed_by' = reviewed_by_json),
    CHECK (checklist_json -> 'controls' = controls_json),
    CHECK (
        (checklist_json ->> 'activation_ready')::BOOLEAN = activation_ready
    ),
    CHECK (NOT (checklist_json ->> 'credential_recorded')::BOOLEAN),
    CHECK (NOT (checklist_json ->> 'endpoint_value_recorded')::BOOLEAN),
    CHECK (NOT (checklist_json ->> 'external_request_performed')::BOOLEAN),
    CHECK (NOT (checklist_json ->> 'infrastructure_deployed')::BOOLEAN)
);

CREATE INDEX IF NOT EXISTS idx_notification_activation_checklist_history
    ON risk_platform.notification_activation_checklists (
        destination_id,
        reviewed_at DESC,
        checklist_id DESC
    );

CREATE TABLE IF NOT EXISTS
risk_platform.controlled_notification_receiver_rehearsals (
    record_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version
            = 'portfolio-risk-controlled-receiver-rehearsal-record-v1'
    ),
    request_id TEXT NOT NULL UNIQUE,
    terminal_status TEXT NOT NULL CHECK (
        terminal_status IN (
            'completed',
            'rejected_before_request',
            'failed_during_rehearsal'
        )
    ),
    failure_code TEXT CHECK (
        failure_code IS NULL
        OR failure_code IN (
            'storage_error',
            'unexpected_error',
            'validation_error'
        )
    ),
    checklist_id TEXT NOT NULL REFERENCES
        risk_platform.notification_activation_checklists (checklist_id),
    destination_id TEXT NOT NULL,
    destination_fingerprint TEXT NOT NULL,
    authority_id TEXT NOT NULL,
    receiver_model_version TEXT NOT NULL CHECK (
        receiver_model_version
            = 'portfolio-risk-controlled-notification-receiver-v1'
    ),
    rehearsal_id TEXT,
    allowed_hosts_json JSONB NOT NULL CHECK (
        jsonb_typeof(allowed_hosts_json) = 'array'
    ),
    allowed_event_types_json JSONB NOT NULL CHECK (
        jsonb_typeof(allowed_event_types_json) = 'array'
    ),
    response_status INTEGER NOT NULL CHECK (
        response_status BETWEEN 200 AND 299
    ),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    attempted_request_count INTEGER NOT NULL CHECK (
        attempted_request_count BETWEEN 0 AND 100
    ),
    request_count INTEGER NOT NULL CHECK (
        request_count BETWEEN 0 AND attempted_request_count
    ),
    unique_idempotency_keys INTEGER NOT NULL CHECK (
        unique_idempotency_keys BETWEEN 0 AND request_count
    ),
    same_content_duplicate_count INTEGER NOT NULL CHECK (
        same_content_duplicate_count BETWEEN 0 AND request_count
    ),
    receipts_json JSONB NOT NULL CHECK (
        jsonb_typeof(receipts_json) = 'array'
    ),
    receiver_summary_json JSONB,
    record_json JSONB NOT NULL CHECK (
        jsonb_typeof(record_json) = 'object'
    ),
    document_sha256 TEXT NOT NULL CHECK (
        document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (record_id <> ''),
    CHECK (request_id <> ''),
    CHECK (destination_id <> ''),
    CHECK (destination_fingerprint <> ''),
    CHECK (authority_id <> ''),
    CHECK (finished_at >= started_at),
    CHECK (recorded_at >= finished_at),
    CHECK (jsonb_array_length(allowed_hosts_json) BETWEEN 1 AND 16),
    CHECK (jsonb_array_length(allowed_event_types_json) BETWEEN 1 AND 64),
    CHECK (jsonb_array_length(receipts_json) = request_count),
    CHECK (unique_idempotency_keys + same_content_duplicate_count = request_count),
    CHECK (
        (terminal_status = 'completed' AND failure_code IS NULL)
        OR (terminal_status <> 'completed' AND failure_code IS NOT NULL)
    ),
    CHECK (
        terminal_status <> 'completed'
        OR (
            rehearsal_id IS NOT NULL
            AND receiver_summary_json IS NOT NULL
            AND request_count > 0
            AND attempted_request_count = request_count
        )
    ),
    CHECK (
        terminal_status <> 'rejected_before_request'
        OR (
            rehearsal_id IS NULL
            AND receiver_summary_json IS NULL
            AND attempted_request_count = 0
            AND request_count = 0
        )
    ),
    CHECK (
        terminal_status <> 'failed_during_rehearsal'
        OR (
            rehearsal_id IS NOT NULL
            AND receiver_summary_json IS NOT NULL
            AND attempted_request_count > request_count
        )
    ),
    CHECK (record_json ->> 'record_id' = record_id),
    CHECK (record_json ->> 'model_version' = model_version),
    CHECK (record_json ->> 'request_id' = request_id),
    CHECK (record_json ->> 'terminal_status' = terminal_status),
    CHECK (record_json -> 'activation_checklist' ->> 'checklist_id' = checklist_id),
    CHECK (
        record_json -> 'activation_checklist' ->> 'destination_id'
        = destination_id
    ),
    CHECK (
        record_json -> 'activation_checklist' ->> 'destination_fingerprint'
        = destination_fingerprint
    ),
    CHECK (
        record_json -> 'activation_checklist' ->> 'authority_id'
        = authority_id
    ),
    CHECK (record_json -> 'allowed_hosts' = allowed_hosts_json),
    CHECK (record_json -> 'allowed_event_types' = allowed_event_types_json),
    CHECK (record_json -> 'receipts' = receipts_json),
    CHECK (record_json -> 'receiver_summary' = receiver_summary_json),
    CHECK (NOT (record_json ->> 'credential_recorded')::BOOLEAN),
    CHECK (NOT (record_json ->> 'endpoint_value_recorded')::BOOLEAN),
    CHECK (NOT (record_json ->> 'payload_bodies_recorded')::BOOLEAN),
    CHECK (NOT (record_json ->> 'response_bodies_recorded')::BOOLEAN),
    CHECK (NOT (record_json ->> 'external_request_performed')::BOOLEAN),
    CHECK (NOT (record_json ->> 'socket_opened')::BOOLEAN),
    CHECK (NOT (record_json ->> 'dns_lookup_performed')::BOOLEAN),
    CHECK (NOT (record_json ->> 'delivery_attempt_written')::BOOLEAN),
    CHECK (NOT (record_json ->> 'outbox_mutated')::BOOLEAN),
    CHECK (NOT (record_json ->> 'acknowledgement_mutated')::BOOLEAN),
    CHECK (NOT (record_json ->> 'infrastructure_deployed')::BOOLEAN)
);

CREATE UNIQUE INDEX IF NOT EXISTS
idx_controlled_notification_receiver_rehearsal_id_unique
    ON risk_platform.controlled_notification_receiver_rehearsals (rehearsal_id)
    WHERE rehearsal_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_controlled_notification_receiver_history
    ON risk_platform.controlled_notification_receiver_rehearsals (
        destination_id,
        recorded_at DESC,
        record_id DESC
    );

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_activation_checklist_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'notification_activation_checklists is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_notification_activation_checklist_append_only
ON risk_platform.notification_activation_checklists;

CREATE TRIGGER trg_notification_activation_checklist_append_only
BEFORE UPDATE OR DELETE
ON risk_platform.notification_activation_checklists
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_activation_checklist_mutation();

CREATE OR REPLACE FUNCTION
risk_platform.reject_controlled_receiver_rehearsal_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'controlled_notification_receiver_rehearsals is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_controlled_receiver_rehearsal_append_only
ON risk_platform.controlled_notification_receiver_rehearsals;

CREATE TRIGGER trg_controlled_receiver_rehearsal_append_only
BEFORE UPDATE OR DELETE
ON risk_platform.controlled_notification_receiver_rehearsals
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_controlled_receiver_rehearsal_mutation();
