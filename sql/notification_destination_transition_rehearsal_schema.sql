-- Append-only notification destination transition rehearsals and current review views.
-- Apply after controlled_notification_receiver_review_schema.sql.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS
risk_platform.notification_destination_transition_rehearsals (
    record_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL CHECK (
        model_version =
            'portfolio-risk-notification-destination-transition-record-v1'
    ),
    request_id TEXT NOT NULL UNIQUE,
    rehearsal_id TEXT NOT NULL UNIQUE,
    destination_id TEXT NOT NULL,
    rotate_plan_id TEXT NOT NULL,
    disable_plan_id TEXT NOT NULL,
    rollback_plan_id TEXT NOT NULL,
    baseline_authority_id TEXT NOT NULL,
    rotate_authority_id TEXT NOT NULL,
    rollback_authority_id TEXT NOT NULL,
    rotate_checklist_id TEXT NOT NULL,
    rollback_checklist_id TEXT NOT NULL,
    rotate_destination_fingerprint TEXT NOT NULL,
    disable_destination_fingerprint TEXT NOT NULL,
    rollback_destination_fingerprint TEXT NOT NULL,
    rotate_endpoint_environment_variable TEXT NOT NULL,
    disable_endpoint_environment_variable TEXT NOT NULL,
    rollback_endpoint_environment_variable TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    rotate_request_count INTEGER NOT NULL CHECK (
        rotate_request_count BETWEEN 1 AND 50
    ),
    rollback_request_count INTEGER NOT NULL CHECK (
        rollback_request_count BETWEEN 1 AND 50
    ),
    same_content_duplicate_count INTEGER NOT NULL CHECK (
        same_content_duplicate_count >= 0
    ),
    rehearsal_json JSONB NOT NULL CHECK (
        jsonb_typeof(rehearsal_json) = 'object'
    ),
    record_json JSONB NOT NULL CHECK (
        jsonb_typeof(record_json) = 'object'
    ),
    document_sha256 CHAR(64) NOT NULL,
    CHECK (started_at <= finished_at),
    CHECK (finished_at <= recorded_at),
    CHECK (
        rotate_request_count + rollback_request_count <= 100
    ),
    CHECK (
        same_content_duplicate_count
            <= rotate_request_count + rollback_request_count
    ),
    CHECK ((rehearsal_json ->> 'destination_id') = destination_id),
    CHECK ((rehearsal_json ->> 'rotate_plan_id') = rotate_plan_id),
    CHECK ((rehearsal_json ->> 'disable_plan_id') = disable_plan_id),
    CHECK ((rehearsal_json ->> 'rollback_plan_id') = rollback_plan_id),
    CHECK ((rehearsal_json ->> 'rehearsal_id') = rehearsal_id),
    CHECK (
        (rehearsal_json ->> 'external_request_performed')::BOOLEAN = FALSE
    ),
    CHECK ((rehearsal_json ->> 'socket_opened')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'dns_lookup_performed')::BOOLEAN = FALSE),
    CHECK (
        (rehearsal_json ->> 'delivery_attempt_written')::BOOLEAN = FALSE
    ),
    CHECK ((rehearsal_json ->> 'outbox_mutated')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'acknowledgement_mutated')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'endpoint_values_recorded')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'endpoint_paths_recorded')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'payload_bodies_recorded')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'response_bodies_recorded')::BOOLEAN = FALSE),
    CHECK ((rehearsal_json ->> 'infrastructure_deployed')::BOOLEAN = FALSE)
);

CREATE INDEX IF NOT EXISTS
idx_notification_destination_transition_rehearsals_current
ON risk_platform.notification_destination_transition_rehearsals (
    destination_id,
    finished_at DESC,
    record_id DESC
);

CREATE OR REPLACE FUNCTION
risk_platform.reject_notification_destination_transition_rehearsal_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION
        'notification destination transition rehearsal history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS
notification_destination_transition_rehearsals_reject_update
ON risk_platform.notification_destination_transition_rehearsals;
CREATE TRIGGER notification_destination_transition_rehearsals_reject_update
BEFORE UPDATE ON risk_platform.notification_destination_transition_rehearsals
FOR EACH ROW
EXECUTE FUNCTION
risk_platform.reject_notification_destination_transition_rehearsal_mutation();

DROP TRIGGER IF EXISTS
notification_destination_transition_rehearsals_reject_delete
ON risk_platform.notification_destination_transition_rehearsals;
CREATE TRIGGER notification_destination_transition_rehearsals_reject_delete
BEFORE DELETE ON risk_platform.notification_destination_transition_rehearsals
FOR EACH ROW
EXECUTE FUNCTION
risk_platform.reject_notification_destination_transition_rehearsal_mutation();

CREATE OR REPLACE VIEW
risk_platform.latest_notification_destination_transition_rehearsals AS
SELECT ranked.*
FROM (
    SELECT
        rehearsal.*,
        ROW_NUMBER() OVER (
            PARTITION BY rehearsal.destination_id
            ORDER BY rehearsal.finished_at DESC, rehearsal.record_id DESC
        ) AS current_version_rank
    FROM risk_platform.notification_destination_transition_rehearsals rehearsal
) ranked
WHERE ranked.current_version_rank = 1;

CREATE OR REPLACE VIEW
risk_platform.current_notification_destination_transition_review AS
WITH evidence AS (
    SELECT
        activation.destination_id,
        activation.destination_fingerprint AS current_destination_fingerprint,
        activation.authority_id AS current_authority_id,
        activation.checklist_id AS current_checklist_id,
        activation.review_status AS activation_review_status,
        activation.operational_activation_ready,
        transition.record_id AS transition_record_id,
        transition.request_id AS transition_request_id,
        transition.rehearsal_id AS transition_rehearsal_id,
        transition.rotate_plan_id,
        transition.disable_plan_id,
        transition.rollback_plan_id,
        transition.baseline_authority_id,
        transition.rotate_authority_id,
        transition.rollback_authority_id,
        transition.rotate_checklist_id,
        transition.rollback_checklist_id,
        transition.rotate_destination_fingerprint,
        transition.disable_destination_fingerprint,
        transition.rollback_destination_fingerprint,
        transition.rotate_endpoint_environment_variable,
        transition.disable_endpoint_environment_variable,
        transition.rollback_endpoint_environment_variable,
        transition.started_at AS transition_started_at,
        transition.finished_at AS transition_finished_at,
        transition.recorded_at AS transition_recorded_at,
        transition.rotate_request_count,
        transition.rollback_request_count,
        transition.same_content_duplicate_count,
        transition.record_id IS NOT NULL
            AND transition.rollback_checklist_id = activation.checklist_id
            AND transition.rollback_destination_fingerprint
                = activation.destination_fingerprint
            AND transition.rollback_authority_id = activation.authority_id
            AS transition_matches_current_activation
    FROM risk_platform.current_notification_activation_rehearsal_review activation
    LEFT JOIN
        risk_platform.latest_notification_destination_transition_rehearsals
        transition
      ON transition.destination_id = activation.destination_id
),
classified AS (
    SELECT
        evidence.*,
        CASE
            WHEN NOT evidence.operational_activation_ready
                THEN 'activation_not_ready'
            WHEN evidence.transition_record_id IS NULL
                THEN 'transition_rehearsal_missing'
            WHEN NOT evidence.transition_matches_current_activation
                THEN 'transition_rehearsal_superseded'
            ELSE 'ready'
        END AS transition_review_status
    FROM evidence
)
SELECT
    classified.*,
    classified.transition_review_status <> 'ready' AS transition_review_required,
    classified.transition_review_status = 'ready' AS transition_ready,
    CASE classified.transition_review_status
        WHEN 'activation_not_ready' THEN 3
        WHEN 'transition_rehearsal_superseded' THEN 2
        WHEN 'transition_rehearsal_missing' THEN 1
        ELSE 0
    END AS transition_review_priority
FROM classified;

CREATE OR REPLACE VIEW
risk_platform.current_notification_destination_transition_review_failures AS
SELECT *
FROM risk_platform.current_notification_destination_transition_review
WHERE transition_review_required;

CREATE OR REPLACE VIEW
risk_platform.current_notification_destination_transition_ready AS
SELECT *
FROM risk_platform.current_notification_destination_transition_review
WHERE transition_ready;
