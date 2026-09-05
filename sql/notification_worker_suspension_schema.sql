-- Atomic evidence companion to the existing worker authority history.
CREATE TABLE IF NOT EXISTS risk_platform.notification_worker_suspension_evidence (
    decision_id TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL UNIQUE,
    previous_transition_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    destination_id TEXT NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    bundle_json JSONB NOT NULL,
    canonical_bundle TEXT NOT NULL,
    bundle_sha256 TEXT NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (transition_id, worker_id, destination_id) REFERENCES
        risk_platform.notification_worker_authority_history (transition_id, worker_id, destination_id),
    FOREIGN KEY (previous_transition_id, worker_id, destination_id) REFERENCES
        risk_platform.notification_worker_authority_history (transition_id, worker_id, destination_id),
    CHECK (evaluated_at <= recorded_at),
    CHECK (jsonb_typeof(bundle_json) = 'object'),
    CHECK (canonical_bundle::JSONB = bundle_json),
    CHECK (octet_length(canonical_bundle) BETWEEN 1 AND 1048576),
    CHECK (encode(sha256(convert_to(canonical_bundle, 'UTF8')), 'hex') = bundle_sha256),
    CHECK ((bundle_json -> 'decision' ->> 'model_version') IS NOT DISTINCT FROM
        'portfolio-risk-notification-worker-suspension-decision-v1'),
    CHECK ((bundle_json -> 'decision' ->> 'decision_id') IS NOT DISTINCT FROM decision_id),
    CHECK ((bundle_json -> 'decision' ->> 'authority_transition_id') IS NOT DISTINCT FROM previous_transition_id),
    CHECK ((bundle_json -> 'decision' ->> 'worker_id') IS NOT DISTINCT FROM worker_id),
    CHECK ((bundle_json -> 'decision' ->> 'destination_id') IS NOT DISTINCT FROM destination_id),
    CHECK ((bundle_json -> 'decision' ->> 'evaluated_at')::TIMESTAMPTZ IS NOT DISTINCT FROM evaluated_at),
    CHECK ((bundle_json -> 'decision' ->> 'outcome') IS NOT DISTINCT FROM 'suspend'),
    CHECK ((bundle_json -> 'decision' -> 'runtime_permission_granted') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((bundle_json -> 'decision' -> 'scheduler_mutated') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((bundle_json -> 'decision' -> 'external_request_performed') IS NOT DISTINCT FROM 'false'::JSONB),
    CHECK ((bundle_json -> 'transition' ->> 'transition_id') IS NOT DISTINCT FROM transition_id),
    CHECK ((bundle_json -> 'transition' ->> 'previous_transition_id') IS NOT DISTINCT FROM previous_transition_id),
    CHECK ((bundle_json -> 'transition' ->> 'action') IS NOT DISTINCT FROM 'suspend'),
    CHECK ((bundle_json -> 'transition' -> 'reason_codes') IS NOT DISTINCT FROM
        (bundle_json -> 'decision' -> 'reason_codes')),
    CHECK ((bundle_json -> 'transition' ->> 'effective_at')::TIMESTAMPTZ IS NOT DISTINCT FROM evaluated_at),
    CHECK ((bundle_json -> 'authority' ->> 'transition_id') IS NOT DISTINCT FROM previous_transition_id)
);

CREATE OR REPLACE FUNCTION risk_platform.guard_notification_worker_suspension_insert()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    prior risk_platform.notification_worker_authority_history%ROWTYPE;
    stopped risk_platform.notification_worker_authority_history%ROWTYPE;
    current_id TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(
        'notification-worker-authority:' || NEW.worker_id, 0
    ));
    SELECT * INTO prior FROM risk_platform.notification_worker_authority_history
    WHERE transition_id = NEW.previous_transition_id;
    IF NOT FOUND OR prior.document_json IS DISTINCT FROM NEW.bundle_json -> 'authority'
        OR prior.document_sha256 IS DISTINCT FROM NEW.bundle_json -> 'decision' ->> 'authority_sha256' THEN
        RAISE EXCEPTION 'suspension predecessor evidence differs' USING ERRCODE = '23514';
    END IF;
    SELECT * INTO stopped FROM risk_platform.notification_worker_authority_history
    WHERE transition_id = NEW.transition_id;
    IF NOT FOUND OR stopped.document_json IS DISTINCT FROM NEW.bundle_json -> 'transition'
        OR stopped.action <> 'suspend' OR stopped.previous_transition_id <> prior.transition_id THEN
        RAISE EXCEPTION 'suspension stop evidence differs' USING ERRCODE = '23514';
    END IF;
    SELECT transition_id INTO current_id FROM risk_platform.notification_worker_authority_history
    WHERE worker_id = NEW.worker_id ORDER BY authority_sequence DESC LIMIT 1;
    IF current_id IS DISTINCT FROM NEW.transition_id THEN
        RAISE EXCEPTION 'suspension stop is not current authority' USING ERRCODE = '23514';
    END IF;
    NEW.recorded_at := clock_timestamp();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS notification_worker_suspension_insert_guard
ON risk_platform.notification_worker_suspension_evidence;
CREATE TRIGGER notification_worker_suspension_insert_guard
BEFORE INSERT ON risk_platform.notification_worker_suspension_evidence
FOR EACH ROW EXECUTE FUNCTION risk_platform.guard_notification_worker_suspension_insert();

DROP TRIGGER IF EXISTS notification_worker_suspension_reject_mutation
ON risk_platform.notification_worker_suspension_evidence;
CREATE TRIGGER notification_worker_suspension_reject_mutation
BEFORE UPDATE OR DELETE ON risk_platform.notification_worker_suspension_evidence
FOR EACH ROW EXECUTE FUNCTION risk_platform.reject_notification_worker_authority_mutation();

DROP TRIGGER IF EXISTS notification_worker_suspension_reject_truncate
ON risk_platform.notification_worker_suspension_evidence;
CREATE TRIGGER notification_worker_suspension_reject_truncate
BEFORE TRUNCATE ON risk_platform.notification_worker_suspension_evidence
FOR EACH STATEMENT EXECUTE FUNCTION risk_platform.reject_notification_worker_authority_mutation();

CREATE OR REPLACE VIEW risk_platform.current_notification_worker_suspension_review AS
SELECT head.worker_id, head.destination_id, head.transition_id,
    head.authority_sequence, head.authority_state,
    CASE WHEN head.authority_state <> 'suspended' THEN 'not_applicable'
        WHEN evidence.decision_id IS NULL THEN 'suspension_evidence_missing'
        ELSE 'bound' END AS suspension_evidence_status,
    evidence.decision_id, evidence.evaluated_at, evidence.bundle_sha256,
    FALSE AS runtime_permission_granted
FROM risk_platform.current_notification_worker_authority head
LEFT JOIN risk_platform.notification_worker_suspension_evidence evidence
    ON evidence.transition_id = head.transition_id;
