-- Append-only worker authority evidence. No scheduler state is modified.
CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.notification_worker_authority_history (
    transition_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    worker_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    previous_transition_id TEXT UNIQUE,
    authority_sequence BIGINT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('activate', 'suspend', 'resume', 'disable')),
    from_state TEXT NOT NULL CHECK (
        from_state IN ('inactive', 'active', 'suspended', 'expired', 'disabled')
    ),
    to_state TEXT NOT NULL CHECK (to_state IN ('active', 'suspended', 'disabled')),
    requested_at TIMESTAMPTZ NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    document_json JSONB NOT NULL,
    canonical_document TEXT NOT NULL,
    document_sha256 TEXT NOT NULL CHECK (document_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_id, authority_sequence),
    UNIQUE (transition_id, worker_id, destination_id),
    FOREIGN KEY (previous_transition_id, worker_id, destination_id) REFERENCES
        risk_platform.notification_worker_authority_history
        (transition_id, worker_id, destination_id),
    CHECK (authority_sequence > 0),
    CHECK (requested_at <= effective_at AND effective_at <= recorded_at),
    CHECK ((to_state = 'active' AND expires_at IS NOT NULL AND effective_at < expires_at)
        OR (to_state <> 'active' AND expires_at IS NULL)),
    CHECK (jsonb_typeof(document_json) = 'object'),
    CHECK (canonical_document::JSONB = document_json),
    CHECK (octet_length(canonical_document) BETWEEN 1 AND 1048576),
    CHECK (encode(sha256(convert_to(canonical_document, 'UTF8')), 'hex') = document_sha256),
    CHECK ((document_json ->> 'model_version') IS NOT DISTINCT FROM
        'portfolio-risk-notification-worker-authority-transition-v1'),
    CHECK ((document_json ->> 'transition_id') IS NOT DISTINCT FROM transition_id),
    CHECK ((document_json ->> 'request_id') IS NOT DISTINCT FROM request_id),
    CHECK ((document_json -> 'plan' -> 'worker' ->> 'worker_id') IS NOT DISTINCT FROM worker_id),
    CHECK ((document_json -> 'plan' -> 'destination' ->> 'destination_id') IS NOT DISTINCT FROM destination_id),
    CHECK ((document_json -> 'plan' ->> 'plan_id') IS NOT DISTINCT FROM plan_id),
    CHECK ((document_json ->> 'previous_transition_id') IS NOT DISTINCT FROM previous_transition_id),
    CHECK ((document_json ->> 'action') IS NOT DISTINCT FROM action),
    CHECK ((document_json ->> 'from_state') IS NOT DISTINCT FROM from_state),
    CHECK ((document_json ->> 'to_state') IS NOT DISTINCT FROM to_state),
    CHECK ((document_json ->> 'requested_at')::TIMESTAMPTZ IS NOT DISTINCT FROM requested_at),
    CHECK ((document_json ->> 'effective_at')::TIMESTAMPTZ IS NOT DISTINCT FROM effective_at),
    CHECK ((document_json ->> 'expires_at')::TIMESTAMPTZ IS NOT DISTINCT FROM expires_at),
    CHECK ((document_json -> 'scheduler_mutated') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((document_json -> 'external_request_performed') IS NOT DISTINCT FROM 'false'::JSONB)
);

CREATE UNIQUE INDEX IF NOT EXISTS notification_worker_authority_one_root
ON risk_platform.notification_worker_authority_history (worker_id)
WHERE previous_transition_id IS NULL;

CREATE OR REPLACE FUNCTION risk_platform.guard_notification_worker_authority_insert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    prior risk_platform.notification_worker_authority_history%ROWTYPE;
    expected_state TEXT;
BEGIN
    -- Same key as the application recorder; unique constraints also reject forks.
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'notification-worker-authority:' || NEW.worker_id, 0
    ));
    SELECT * INTO prior
    FROM risk_platform.notification_worker_authority_history
    WHERE worker_id = NEW.worker_id
    ORDER BY authority_sequence DESC LIMIT 1;

    IF NOT FOUND THEN
        IF NEW.previous_transition_id IS NOT NULL OR NEW.action <> 'activate'
            OR NEW.from_state <> 'inactive' THEN
            RAISE EXCEPTION 'worker authority requires initial activation' USING ERRCODE = '23514';
        END IF;
        NEW.authority_sequence := 1;
    ELSE
        IF NEW.previous_transition_id IS DISTINCT FROM prior.transition_id
            OR NEW.destination_id <> prior.destination_id
            OR NEW.effective_at <= prior.effective_at
            OR NEW.requested_at < prior.effective_at THEN
            RAISE EXCEPTION 'worker authority head or chronology conflict' USING ERRCODE = '23514';
        END IF;
        expected_state := CASE
            WHEN prior.to_state = 'active' AND NEW.effective_at >= prior.expires_at
                THEN 'expired' ELSE prior.to_state END;
        IF NEW.from_state <> expected_state THEN
            RAISE EXCEPTION 'worker authority source state conflict' USING ERRCODE = '23514';
        END IF;
        IF NEW.action IN ('suspend', 'disable') AND
            NEW.document_json -> 'plan' IS DISTINCT FROM prior.document_json -> 'plan' THEN
            RAISE EXCEPTION 'worker stop plan differs from retained authority' USING ERRCODE = '23514';
        END IF;
        IF NEW.action = 'resume' AND NEW.effective_at < prior.effective_at
            + ((prior.document_json -> 'plan' -> 'suspension' ->> 'cooldown_seconds')::INTEGER
                * INTERVAL '1 second') THEN
            RAISE EXCEPTION 'worker authority suspension cooldown remains active' USING ERRCODE = '23514';
        END IF;
        NEW.authority_sequence := prior.authority_sequence + 1;
    END IF;
    IF NOT (
        (NEW.action = 'activate' AND NEW.from_state IN ('inactive', 'disabled', 'expired') AND NEW.to_state = 'active')
        OR (NEW.action = 'resume' AND NEW.from_state = 'suspended' AND NEW.to_state = 'active')
        OR (NEW.action = 'suspend' AND NEW.from_state = 'active' AND NEW.to_state = 'suspended')
        OR (NEW.action = 'disable' AND NEW.from_state IN ('active', 'suspended', 'expired') AND NEW.to_state = 'disabled')
    ) THEN
        RAISE EXCEPTION 'worker authority action is invalid' USING ERRCODE = '23514';
    END IF;
    -- Server time, not caller-supplied metadata. Future heads cannot hide current grants.
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS notification_worker_authority_insert_guard
ON risk_platform.notification_worker_authority_history;
CREATE TRIGGER notification_worker_authority_insert_guard
BEFORE INSERT ON risk_platform.notification_worker_authority_history
FOR EACH ROW EXECUTE FUNCTION risk_platform.guard_notification_worker_authority_insert();

CREATE OR REPLACE FUNCTION risk_platform.reject_notification_worker_authority_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'notification worker authority history is append-only';
END;
$$;
DROP TRIGGER IF EXISTS notification_worker_authority_reject_mutation
ON risk_platform.notification_worker_authority_history;
CREATE TRIGGER notification_worker_authority_reject_mutation
BEFORE UPDATE OR DELETE ON risk_platform.notification_worker_authority_history
FOR EACH ROW EXECUTE FUNCTION risk_platform.reject_notification_worker_authority_mutation();
DROP TRIGGER IF EXISTS notification_worker_authority_reject_truncate
ON risk_platform.notification_worker_authority_history;
CREATE TRIGGER notification_worker_authority_reject_truncate
BEFORE TRUNCATE ON risk_platform.notification_worker_authority_history
FOR EACH STATEMENT EXECUTE FUNCTION risk_platform.reject_notification_worker_authority_mutation();

CREATE OR REPLACE VIEW risk_platform.current_notification_worker_authority AS
SELECT latest.*,
    CASE WHEN to_state = 'active' AND statement_timestamp() >= expires_at
        THEN 'expired' ELSE to_state END AS authority_state,
    FALSE AS runtime_permission_granted
FROM (
    SELECT DISTINCT ON (worker_id)
        worker_id, destination_id, transition_id, request_id, authority_sequence,
        plan_id, action, to_state, effective_at, expires_at, recorded_at, document_sha256
    FROM risk_platform.notification_worker_authority_history
    ORDER BY worker_id, authority_sequence DESC
) latest;
