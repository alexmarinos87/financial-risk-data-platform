-- Explicit installation only; no scheduler, migration runner or runtime grant.
-- Apply after the delivery-attempt and worker-authority schemas.
CREATE TABLE IF NOT EXISTS risk_platform.notification_worker_execution_contexts (
    context_id TEXT PRIMARY KEY,
    scope_id TEXT NOT NULL UNIQUE,
    request_id TEXT NOT NULL UNIQUE,
    authority_transition_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    max_events INTEGER NOT NULL CHECK (max_events BETWEEN 1 AND 100),
    context_json JSONB NOT NULL,
    canonical_context TEXT NOT NULL,
    context_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (context_id, worker_id, destination_id),
    FOREIGN KEY (authority_transition_id, worker_id, destination_id) REFERENCES
        risk_platform.notification_worker_authority_history (transition_id, worker_id, destination_id),
    CHECK (jsonb_typeof(context_json) = 'object'),
    CHECK (canonical_context::JSONB = context_json),
    CHECK (octet_length(canonical_context) BETWEEN 1 AND 16384),
    CHECK (encode(sha256(convert_to(canonical_context, 'UTF8')), 'hex') = context_sha256),
    CHECK ((context_json ->> 'model_version') IS NOT DISTINCT FROM 'portfolio-risk-worker-execution-context-v1'),
    CHECK ((context_json ->> 'context_id') IS NOT DISTINCT FROM context_id),
    CHECK ((context_json ->> 'scope_id') IS NOT DISTINCT FROM scope_id),
    CHECK ((context_json ->> 'request_id') IS NOT DISTINCT FROM request_id),
    CHECK ((context_json ->> 'authority_transition_id') IS NOT DISTINCT FROM authority_transition_id),
    CHECK ((context_json ->> 'worker_id') IS NOT DISTINCT FROM worker_id),
    CHECK ((context_json ->> 'destination_id') IS NOT DISTINCT FROM destination_id),
    CHECK ((context_json ->> 'max_events')::INTEGER IS NOT DISTINCT FROM max_events),
    CHECK ((context_json -> 'current_authority_verified') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((context_json -> 'readiness_verified') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((context_json -> 'slot_claimed') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((context_json -> 'runtime_permission_granted') IS NOT DISTINCT FROM 'false'::JSONB)
);

CREATE TABLE IF NOT EXISTS risk_platform.notification_worker_attempt_attributions (
    attribution_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL UNIQUE REFERENCES risk_platform.portfolio_risk_notification_delivery_attempts (attempt_id),
    context_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    record_json JSONB NOT NULL,
    canonical_record TEXT NOT NULL,
    record_sha256 TEXT NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (context_id, event_id),
    FOREIGN KEY (context_id, worker_id, destination_id) REFERENCES
        risk_platform.notification_worker_execution_contexts (context_id, worker_id, destination_id),
    CHECK (jsonb_typeof(record_json) = 'object'),
    CHECK (canonical_record::JSONB = record_json),
    CHECK (octet_length(canonical_record) BETWEEN 1 AND 1048576),
    CHECK (encode(sha256(convert_to(canonical_record, 'UTF8')), 'hex') = record_sha256),
    CHECK ((record_json ->> 'model_version') IS NOT DISTINCT FROM 'portfolio-risk-worker-attempt-attribution-v1'),
    CHECK ((record_json ->> 'attribution_id') IS NOT DISTINCT FROM attribution_id),
    CHECK ((record_json -> 'attempt' ->> 'attempt_id') IS NOT DISTINCT FROM attempt_id),
    CHECK ((record_json -> 'attempt' ->> 'event_id') IS NOT DISTINCT FROM event_id),
    CHECK ((record_json -> 'context' ->> 'context_id') IS NOT DISTINCT FROM context_id),
    CHECK ((record_json -> 'context' ->> 'worker_id') IS NOT DISTINCT FROM worker_id),
    CHECK ((record_json -> 'context' ->> 'destination_id') IS NOT DISTINCT FROM destination_id),
    CHECK ((record_json -> 'receipt' ->> 'context_id') IS NOT DISTINCT FROM context_id),
    CHECK ((record_json -> 'receipt' ->> 'attempt_id') IS NOT DISTINCT FROM attempt_id),
    CHECK ((record_json -> 'receipt' ->> 'event_id') IS NOT DISTINCT FROM event_id),
    CHECK ((record_json -> 'failure_history_complete') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((record_json -> 'runtime_permission_granted') IS NOT DISTINCT FROM 'false'::JSONB)
);

CREATE OR REPLACE FUNCTION risk_platform.guard_worker_attempt_attribution_insert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    selected risk_platform.notification_worker_execution_contexts%ROWTYPE;
    authority risk_platform.notification_worker_authority_history%ROWTYPE;
    source risk_platform.portfolio_risk_notification_delivery_attempts%ROWTYPE;
    existing_count BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended('notification-worker-authority:' || NEW.worker_id, 0));
    SELECT * INTO selected FROM risk_platform.notification_worker_execution_contexts
    WHERE context_id = NEW.context_id;
    IF NOT FOUND OR selected.context_json IS DISTINCT FROM NEW.record_json -> 'context' THEN
        RAISE EXCEPTION 'attribution retained context differs' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO authority FROM risk_platform.notification_worker_authority_history
    WHERE transition_id = selected.authority_transition_id;
    IF NOT FOUND OR authority.document_json IS DISTINCT FROM NEW.record_json -> 'authority'
        OR authority.document_sha256 IS DISTINCT FROM selected.context_json ->> 'authority_sha256' THEN
        RAISE EXCEPTION 'attribution retained authority differs' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO source FROM risk_platform.portfolio_risk_notification_delivery_attempts
    WHERE attempt_id = NEW.attempt_id FOR SHARE;
    IF NOT FOUND OR (to_jsonb(source) - 'loaded_at' - 'attempted_at') IS DISTINCT FROM
            ((NEW.record_json -> 'attempt') - 'attempted_at')
        OR source.attempted_at IS DISTINCT FROM (NEW.record_json -> 'attempt' ->> 'attempted_at')::TIMESTAMPTZ
        OR source.attempted_at > clock_timestamp() THEN
        RAISE EXCEPTION 'attribution retained source differs' USING ERRCODE = '23514';
    END IF;
    SELECT COUNT(*) INTO existing_count FROM risk_platform.notification_worker_attempt_attributions
    WHERE context_id = NEW.context_id;
    IF existing_count >= selected.max_events THEN
        RAISE EXCEPTION 'attribution context event limit reached' USING ERRCODE = '23514';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS worker_attempt_attribution_insert_guard
ON risk_platform.notification_worker_attempt_attributions;
CREATE TRIGGER worker_attempt_attribution_insert_guard
BEFORE INSERT ON risk_platform.notification_worker_attempt_attributions
FOR EACH ROW EXECUTE FUNCTION risk_platform.guard_worker_attempt_attribution_insert();

CREATE OR REPLACE FUNCTION risk_platform.reject_worker_attribution_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'worker attribution is append-only' USING ERRCODE = '23514';
END;
$$;

DROP TRIGGER IF EXISTS worker_context_reject_mutation ON risk_platform.notification_worker_execution_contexts;
CREATE TRIGGER worker_context_reject_mutation BEFORE UPDATE OR DELETE
ON risk_platform.notification_worker_execution_contexts
FOR EACH ROW EXECUTE FUNCTION risk_platform.reject_worker_attribution_mutation();
DROP TRIGGER IF EXISTS worker_context_reject_truncate ON risk_platform.notification_worker_execution_contexts;
CREATE TRIGGER worker_context_reject_truncate BEFORE TRUNCATE
ON risk_platform.notification_worker_execution_contexts
FOR EACH STATEMENT EXECUTE FUNCTION risk_platform.reject_worker_attribution_mutation();

DROP TRIGGER IF EXISTS worker_attribution_reject_mutation ON risk_platform.notification_worker_attempt_attributions;
CREATE TRIGGER worker_attribution_reject_mutation BEFORE UPDATE OR DELETE
ON risk_platform.notification_worker_attempt_attributions
FOR EACH ROW EXECUTE FUNCTION risk_platform.reject_worker_attribution_mutation();
DROP TRIGGER IF EXISTS worker_attribution_reject_truncate ON risk_platform.notification_worker_attempt_attributions;
CREATE TRIGGER worker_attribution_reject_truncate BEFORE TRUNCATE
ON risk_platform.notification_worker_attempt_attributions
FOR EACH STATEMENT EXECUTE FUNCTION risk_platform.reject_worker_attribution_mutation();
