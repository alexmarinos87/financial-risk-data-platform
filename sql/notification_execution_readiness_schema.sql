-- Append-only notification execution readiness decisions and current review views.
-- Apply after notification_destination_transition_rehearsal_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.notification_execution_readiness_decisions (
    record_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version =
            'portfolio-risk-notification-execution-readiness-record-v1'
    ),
    request_id TEXT NOT NULL UNIQUE,
    decision_id TEXT NOT NULL UNIQUE,
    destination_id TEXT NOT NULL,
    execution_kind TEXT NOT NULL CHECK (
        execution_kind IN ('initial', 'retry')
    ),
    evaluated_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('allow', 'block')),
    blocking_reasons_json JSONB NOT NULL CHECK (
        jsonb_typeof(blocking_reasons_json) = 'array'
    ),
    delivery_fingerprint TEXT NOT NULL,
    retry_policy_fingerprint TEXT NOT NULL,
    retry_execution_policy_fingerprint TEXT NOT NULL,
    endpoint_environment_variable TEXT NOT NULL,
    destination_fingerprint TEXT NOT NULL,
    destination_activation_status TEXT NOT NULL CHECK (
        destination_activation_status IN (
            'active',
            'disabled',
            'not_yet_reviewed',
            'review_expired'
        )
    ),
    activation_authority_id TEXT,
    activation_checklist_id TEXT,
    activation_review_status TEXT,
    activation_ready BOOLEAN,
    transition_record_id TEXT,
    transition_rehearsal_id TEXT,
    transition_review_status TEXT,
    transition_ready BOOLEAN,
    ambiguity_count INTEGER NOT NULL CHECK (
        ambiguity_count BETWEEN 0 AND 500
    ),
    ambiguity_event_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(ambiguity_event_ids_json) = 'array'
    ),
    ambiguity_record_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(ambiguity_record_ids_json) = 'array'
    ),
    unbound_ambiguity_event_ids_json JSONB NOT NULL CHECK (
        jsonb_typeof(unbound_ambiguity_event_ids_json) = 'array'
    ),
    decision_json JSONB NOT NULL CHECK (
        jsonb_typeof(decision_json) = 'object'
    ),
    record_json JSONB NOT NULL CHECK (
        jsonb_typeof(record_json) = 'object'
    ),
    document_sha256 CHAR(64) NOT NULL CHECK (
        document_sha256 ~ '^[0-9a-f]{64}$'
    ),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (record_id <> ''),
    CHECK (request_id <> ''),
    CHECK (decision_id <> ''),
    CHECK (destination_id <> ''),
    CHECK (recorded_at >= evaluated_at),
    CHECK (
        (decision = 'allow' AND jsonb_array_length(blocking_reasons_json) = 0)
        OR (decision = 'block' AND jsonb_array_length(blocking_reasons_json) > 0)
    ),
    CHECK (
        jsonb_array_length(ambiguity_event_ids_json) = ambiguity_count
    ),
    CHECK (
        jsonb_array_length(ambiguity_record_ids_json) = ambiguity_count
    ),
    CHECK (
        jsonb_array_length(unbound_ambiguity_event_ids_json) <= ambiguity_count
    ),
    CHECK (
        (activation_authority_id IS NULL
            AND activation_checklist_id IS NULL
            AND activation_review_status IS NULL
            AND activation_ready IS NULL)
        OR
        (activation_authority_id IS NOT NULL
            AND activation_checklist_id IS NOT NULL
            AND activation_review_status IS NOT NULL
            AND activation_ready IS NOT NULL)
    ),
    CHECK (
        (transition_review_status IS NULL AND transition_ready IS NULL)
        OR
        (transition_review_status IS NOT NULL AND transition_ready IS NOT NULL)
    ),
    CHECK (decision_json ->> 'decision_id' = decision_id),
    CHECK (decision_json ->> 'execution_kind' = execution_kind),
    CHECK (decision_json ->> 'decision' = decision),
    CHECK (
        decision_json -> 'destination' ->> 'destination_id' = destination_id
    ),
    CHECK (
        decision_json -> 'destination' ->> 'fingerprint'
            = destination_fingerprint
    ),
    CHECK (
        decision_json -> 'configuration' ->> 'delivery_fingerprint'
            = delivery_fingerprint
    ),
    CHECK (
        decision_json -> 'configuration' ->>
            'retry_execution_policy_fingerprint'
            = retry_execution_policy_fingerprint
    ),
    CHECK (decision_json -> 'blocking_reasons' = blocking_reasons_json),
    CHECK (
        (decision_json -> 'ambiguity' ->> 'count')::INTEGER = ambiguity_count
    ),
    CHECK (
        decision_json -> 'ambiguity' -> 'event_ids'
            = ambiguity_event_ids_json
    ),
    CHECK (
        decision_json -> 'ambiguity' -> 'record_ids'
            = ambiguity_record_ids_json
    ),
    CHECK (
        decision_json -> 'ambiguity' -> 'unbound_event_ids'
            = unbound_ambiguity_event_ids_json
    ),
    CHECK (record_json ->> 'record_id' = record_id),
    CHECK (record_json ->> 'model_version' = model_version),
    CHECK (record_json ->> 'request_id' = request_id),
    CHECK (record_json -> 'decision' = decision_json),
    CHECK ((record_json ->> 'recorded_at')::TIMESTAMPTZ = recorded_at),
    CHECK ((decision_json ->> 'evaluated_at')::TIMESTAMPTZ = evaluated_at),
    CHECK ((decision_json ->> 'read_only')::BOOLEAN),
    CHECK (NOT (decision_json ->> 'external_request_performed')::BOOLEAN),
    CHECK (NOT (decision_json ->> 'delivery_attempt_written')::BOOLEAN),
    CHECK (NOT (decision_json ->> 'outbox_mutated')::BOOLEAN),
    CHECK (NOT (decision_json ->> 'acknowledgement_mutated')::BOOLEAN)
);

CREATE INDEX IF NOT EXISTS
idx_notification_execution_readiness_current
ON risk_platform.notification_execution_readiness_decisions (
    destination_id,
    execution_kind,
    evaluated_at DESC,
    record_id DESC
);

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_execution_readiness_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'notification execution readiness history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS notification_execution_readiness_reject_update
ON risk_platform.notification_execution_readiness_decisions;
CREATE TRIGGER notification_execution_readiness_reject_update
BEFORE UPDATE ON risk_platform.notification_execution_readiness_decisions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_execution_readiness_mutation();

DROP TRIGGER IF EXISTS notification_execution_readiness_reject_delete
ON risk_platform.notification_execution_readiness_decisions;
CREATE TRIGGER notification_execution_readiness_reject_delete
BEFORE DELETE ON risk_platform.notification_execution_readiness_decisions
FOR EACH ROW
EXECUTE FUNCTION risk_platform.reject_notification_execution_readiness_mutation();

CREATE OR REPLACE VIEW
risk_platform.latest_notification_execution_readiness_decisions AS
SELECT ranked.*
FROM (
    SELECT
        readiness.*,
        ROW_NUMBER() OVER (
            PARTITION BY readiness.destination_id, readiness.execution_kind
            ORDER BY readiness.evaluated_at DESC, readiness.record_id DESC
        ) AS current_version_rank
    FROM risk_platform.notification_execution_readiness_decisions readiness
) ranked
WHERE ranked.current_version_rank = 1;

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_review AS
WITH execution_kinds AS (
    SELECT execution_kind
    FROM (VALUES ('initial'::TEXT), ('retry'::TEXT)) values_(execution_kind)
),
evidence AS (
    SELECT
        transition.destination_id,
        execution.execution_kind,
        transition.current_destination_fingerprint,
        transition.current_authority_id,
        transition.current_checklist_id,
        transition.activation_review_status,
        transition.operational_activation_ready,
        transition.transition_record_id AS current_transition_record_id,
        transition.transition_rehearsal_id AS current_transition_rehearsal_id,
        transition.rollback_endpoint_environment_variable
            AS current_endpoint_environment_variable,
        transition.transition_review_status AS current_transition_review_status,
        transition.transition_ready AS current_transition_ready,
        readiness.record_id AS readiness_record_id,
        readiness.request_id AS readiness_request_id,
        readiness.decision_id,
        readiness.evaluated_at AS decision_evaluated_at,
        readiness.recorded_at AS decision_recorded_at,
        readiness.decision,
        readiness.blocking_reasons_json,
        readiness.delivery_fingerprint,
        readiness.retry_policy_fingerprint,
        readiness.retry_execution_policy_fingerprint,
        readiness.endpoint_environment_variable,
        readiness.destination_fingerprint,
        readiness.activation_authority_id,
        readiness.activation_checklist_id,
        readiness.activation_review_status AS decision_activation_review_status,
        readiness.activation_ready AS decision_activation_ready,
        readiness.transition_record_id AS decision_transition_record_id,
        readiness.transition_rehearsal_id AS decision_transition_rehearsal_id,
        readiness.transition_review_status AS decision_transition_review_status,
        readiness.transition_ready AS decision_transition_ready,
        readiness.ambiguity_count,
        readiness.ambiguity_event_ids_json,
        readiness.ambiguity_record_ids_json,
        readiness.unbound_ambiguity_event_ids_json,
        readiness.record_id IS NOT NULL
            AND readiness.destination_fingerprint
                = transition.current_destination_fingerprint
            AND readiness.activation_authority_id
                IS NOT DISTINCT FROM transition.current_authority_id
            AND readiness.activation_checklist_id
                IS NOT DISTINCT FROM transition.current_checklist_id
            AND readiness.activation_review_status
                IS NOT DISTINCT FROM transition.activation_review_status
            AND readiness.activation_ready
                IS NOT DISTINCT FROM transition.operational_activation_ready
            AND readiness.transition_record_id
                IS NOT DISTINCT FROM transition.transition_record_id
            AND readiness.transition_rehearsal_id
                IS NOT DISTINCT FROM transition.transition_rehearsal_id
            AND readiness.transition_review_status
                IS NOT DISTINCT FROM transition.transition_review_status
            AND readiness.transition_ready
                IS NOT DISTINCT FROM transition.transition_ready
            AND (
                transition.rollback_endpoint_environment_variable IS NULL
                OR readiness.endpoint_environment_variable
                    = transition.rollback_endpoint_environment_variable
            ) AS decision_matches_current_evidence
    FROM risk_platform.current_notification_destination_transition_review transition
    CROSS JOIN execution_kinds execution
    LEFT JOIN risk_platform.latest_notification_execution_readiness_decisions readiness
      ON readiness.destination_id = transition.destination_id
     AND readiness.execution_kind = execution.execution_kind
),
classified AS (
    SELECT
        evidence.*,
        CASE
            WHEN evidence.readiness_record_id IS NULL
                THEN 'decision_missing'
            WHEN NOT evidence.decision_matches_current_evidence
                THEN 'decision_superseded'
            WHEN evidence.decision_evaluated_at
                    < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                THEN 'decision_stale'
            WHEN evidence.decision = 'block'
                THEN 'blocked'
            ELSE 'allowed'
        END AS readiness_review_status
    FROM evidence
)
SELECT
    classified.*,
    classified.readiness_review_status <> 'allowed' AS readiness_review_required,
    classified.readiness_review_status = 'allowed' AS execution_ready,
    CASE classified.readiness_review_status
        WHEN 'decision_superseded' THEN 4
        WHEN 'decision_stale' THEN 3
        WHEN 'blocked' THEN 2
        WHEN 'decision_missing' THEN 1
        ELSE 0
    END AS readiness_review_priority
FROM classified;

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_allowed AS
SELECT *
FROM risk_platform.current_notification_execution_readiness_review
WHERE readiness_review_status = 'allowed';

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_blocked AS
SELECT *
FROM risk_platform.current_notification_execution_readiness_review
WHERE readiness_review_status = 'blocked';

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_stale AS
SELECT *
FROM risk_platform.current_notification_execution_readiness_review
WHERE readiness_review_status = 'decision_stale';

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_superseded AS
SELECT *
FROM risk_platform.current_notification_execution_readiness_review
WHERE readiness_review_status = 'decision_superseded';

CREATE OR REPLACE VIEW
risk_platform.current_notification_execution_readiness_missing AS
SELECT *
FROM risk_platform.current_notification_execution_readiness_review
WHERE readiness_review_status = 'decision_missing';
