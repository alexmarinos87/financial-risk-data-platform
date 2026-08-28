-- Current activation-checklist and controlled-receiver rehearsal review views.
-- Apply after controlled_notification_receiver_rehearsal_schema.sql.

CREATE OR REPLACE VIEW
risk_platform.latest_notification_activation_checklists AS
SELECT ranked.*
FROM (
    SELECT
        checklist.*,
        ROW_NUMBER() OVER (
            PARTITION BY checklist.destination_id
            ORDER BY checklist.reviewed_at DESC, checklist.checklist_id DESC
        ) AS current_version_rank
    FROM risk_platform.notification_activation_checklists checklist
) ranked
WHERE ranked.current_version_rank = 1;

CREATE OR REPLACE VIEW
risk_platform.latest_controlled_notification_receiver_rehearsals AS
SELECT ranked.*
FROM (
    SELECT
        rehearsal.*,
        ROW_NUMBER() OVER (
            PARTITION BY rehearsal.destination_id
            ORDER BY rehearsal.recorded_at DESC, rehearsal.record_id DESC
        ) AS current_version_rank
    FROM risk_platform.controlled_notification_receiver_rehearsals rehearsal
) ranked
WHERE ranked.current_version_rank = 1;

CREATE OR REPLACE VIEW
risk_platform.current_notification_activation_rehearsal_review AS
WITH evidence AS (
    SELECT
        checklist.destination_id,
        checklist.destination_fingerprint,
        checklist.authority_id,
        checklist.checklist_id,
        checklist.reviewed_at,
        checklist.review_expires_at,
        checklist.reviewed_by_json,
        checklist.controls_json,
        checklist.activation_ready AS checklist_activation_ready,
        COALESCE(incomplete.incomplete_controls_json, '[]'::jsonb)
            AS incomplete_controls_json,
        rehearsal.record_id AS rehearsal_record_id,
        rehearsal.request_id AS rehearsal_request_id,
        rehearsal.rehearsal_id,
        rehearsal.terminal_status AS rehearsal_terminal_status,
        rehearsal.failure_code AS rehearsal_failure_code,
        rehearsal.checklist_id AS rehearsal_checklist_id,
        rehearsal.destination_fingerprint AS rehearsal_destination_fingerprint,
        rehearsal.authority_id AS rehearsal_authority_id,
        rehearsal.receiver_model_version,
        rehearsal.response_status,
        rehearsal.started_at AS rehearsal_started_at,
        rehearsal.finished_at AS rehearsal_finished_at,
        rehearsal.recorded_at AS rehearsal_recorded_at,
        rehearsal.attempted_request_count,
        rehearsal.request_count,
        rehearsal.unique_idempotency_keys,
        rehearsal.same_content_duplicate_count,
        rehearsal.allowed_hosts_json,
        rehearsal.allowed_event_types_json,
        referenced.checklist_id IS NOT NULL
            AND referenced.destination_id = rehearsal.destination_id
            AND referenced.destination_fingerprint
                = rehearsal.destination_fingerprint
            AND referenced.authority_id = rehearsal.authority_id
            AS rehearsal_reference_consistent,
        rehearsal.record_id IS NOT NULL
            AND rehearsal.checklist_id = checklist.checklist_id
            AND rehearsal.destination_fingerprint
                = checklist.destination_fingerprint
            AND rehearsal.authority_id = checklist.authority_id
            AS rehearsal_matches_current_checklist
    FROM risk_platform.latest_notification_activation_checklists checklist
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(control.key ORDER BY control.key)
            AS incomplete_controls_json
        FROM jsonb_each(checklist.controls_json) AS control(key, value)
        WHERE control.value <> 'true'::jsonb
    ) incomplete ON TRUE
    LEFT JOIN risk_platform.latest_controlled_notification_receiver_rehearsals
        rehearsal
      ON rehearsal.destination_id = checklist.destination_id
    LEFT JOIN risk_platform.notification_activation_checklists referenced
      ON referenced.checklist_id = rehearsal.checklist_id
),
classified AS (
    SELECT
        evidence.*,
        CASE
            WHEN NOT evidence.checklist_activation_ready
                THEN 'checklist_incomplete'
            WHEN CURRENT_TIMESTAMP < evidence.reviewed_at
                THEN 'checklist_not_yet_active'
            WHEN CURRENT_TIMESTAMP >= evidence.review_expires_at
                THEN 'checklist_expired'
            WHEN evidence.rehearsal_record_id IS NULL
                THEN 'rehearsal_missing'
            WHEN NOT evidence.rehearsal_reference_consistent
                THEN 'rehearsal_evidence_conflict'
            WHEN NOT evidence.rehearsal_matches_current_checklist
                THEN 'rehearsal_superseded'
            WHEN evidence.rehearsal_terminal_status = 'rejected_before_request'
                THEN 'rehearsal_rejected'
            WHEN evidence.rehearsal_terminal_status = 'failed_during_rehearsal'
                THEN 'rehearsal_failed'
            WHEN evidence.rehearsal_terminal_status = 'completed'
                THEN 'ready'
            ELSE 'review_required'
        END AS review_status
    FROM evidence
)
SELECT
    classified.*,
    classified.review_status <> 'ready' AS review_required,
    classified.review_status = 'ready' AS operational_activation_ready,
    CASE classified.review_status
        WHEN 'rehearsal_evidence_conflict' THEN 9
        WHEN 'checklist_incomplete' THEN 8
        WHEN 'checklist_expired' THEN 7
        WHEN 'checklist_not_yet_active' THEN 6
        WHEN 'rehearsal_superseded' THEN 5
        WHEN 'rehearsal_failed' THEN 4
        WHEN 'rehearsal_rejected' THEN 3
        WHEN 'rehearsal_missing' THEN 2
        WHEN 'review_required' THEN 1
        ELSE 0
    END AS review_priority
FROM classified;

CREATE OR REPLACE VIEW
risk_platform.current_notification_activation_review_failures AS
SELECT *
FROM risk_platform.current_notification_activation_rehearsal_review
WHERE review_required;

CREATE OR REPLACE VIEW
risk_platform.current_notification_activation_ready AS
SELECT *
FROM risk_platform.current_notification_activation_rehearsal_review
WHERE operational_activation_ready;
