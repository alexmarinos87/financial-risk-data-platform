-- Reconciliation for append-only activation checklists and receiver rehearsals.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.notification_activation_checklists)
            AS checklist_rows,
        (SELECT COUNT(DISTINCT checklist_id)
         FROM risk_platform.notification_activation_checklists)
            AS distinct_checklist_ids,
        (SELECT COUNT(*)
         FROM risk_platform.controlled_notification_receiver_rehearsals)
            AS rehearsal_rows,
        (SELECT COUNT(DISTINCT request_id)
         FROM risk_platform.controlled_notification_receiver_rehearsals)
            AS distinct_request_ids
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals rehearsal
            LEFT JOIN risk_platform.notification_activation_checklists checklist
              ON checklist.checklist_id = rehearsal.checklist_id
            WHERE checklist.checklist_id IS NULL
               OR checklist.destination_id <> rehearsal.destination_id
               OR checklist.destination_fingerprint
                    <> rehearsal.destination_fingerprint
               OR checklist.authority_id <> rehearsal.authority_id
        ) AS missing_or_mismatched_checklists,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals
            WHERE jsonb_array_length(receipts_json) <> request_count
               OR unique_idempotency_keys
                    + same_content_duplicate_count <> request_count
        ) AS invalid_receipt_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals
            WHERE terminal_status = 'completed'
              AND (
                  failure_code IS NOT NULL
                  OR rehearsal_id IS NULL
                  OR receiver_summary_json IS NULL
                  OR request_count = 0
                  OR attempted_request_count <> request_count
              )
        ) AS invalid_completed_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals
            WHERE terminal_status = 'rejected_before_request'
              AND (
                  failure_code IS NULL
                  OR rehearsal_id IS NOT NULL
                  OR receiver_summary_json IS NOT NULL
                  OR attempted_request_count <> 0
                  OR request_count <> 0
              )
        ) AS invalid_rejected_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals
            WHERE terminal_status = 'failed_during_rehearsal'
              AND (
                  failure_code IS NULL
                  OR rehearsal_id IS NULL
                  OR receiver_summary_json IS NULL
                  OR attempted_request_count <= request_count
              )
        ) AS invalid_failed_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals
            WHERE (record_json ->> 'credential_recorded')::BOOLEAN
               OR (record_json ->> 'endpoint_value_recorded')::BOOLEAN
               OR (record_json ->> 'payload_bodies_recorded')::BOOLEAN
               OR (record_json ->> 'response_bodies_recorded')::BOOLEAN
               OR (record_json ->> 'external_request_performed')::BOOLEAN
               OR (record_json ->> 'socket_opened')::BOOLEAN
               OR (record_json ->> 'dns_lookup_performed')::BOOLEAN
               OR (record_json ->> 'delivery_attempt_written')::BOOLEAN
               OR (record_json ->> 'outbox_mutated')::BOOLEAN
               OR (record_json ->> 'acknowledgement_mutated')::BOOLEAN
               OR (record_json ->> 'infrastructure_deployed')::BOOLEAN
        ) AS unsafe_rehearsal_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_activation_checklists
            WHERE (checklist_json ->> 'credential_recorded')::BOOLEAN
               OR (checklist_json ->> 'endpoint_value_recorded')::BOOLEAN
               OR (checklist_json ->> 'external_request_performed')::BOOLEAN
               OR (checklist_json ->> 'infrastructure_deployed')::BOOLEAN
        ) AS unsafe_checklist_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.controlled_notification_receiver_rehearsals rehearsal
            JOIN risk_platform.notification_activation_checklists checklist
              ON checklist.checklist_id = rehearsal.checklist_id
            WHERE rehearsal.started_at < checklist.reviewed_at
               OR rehearsal.started_at >= checklist.review_expires_at
        ) AS rehearsals_outside_review_window
)
SELECT
    'notification_activation_checklist_ids_unique' AS check_name,
    checklist_rows::TEXT AS expected,
    distinct_checklist_ids::TEXT AS actual,
    CASE WHEN checklist_rows = distinct_checklist_ids THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'controlled_receiver_request_ids_unique',
    rehearsal_rows::TEXT,
    distinct_request_ids::TEXT,
    CASE WHEN rehearsal_rows = distinct_request_ids THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'controlled_receiver_checklist_references_match',
    '0',
    missing_or_mismatched_checklists::TEXT,
    CASE WHEN missing_or_mismatched_checklists = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_receipt_counts_match',
    '0',
    invalid_receipt_counts::TEXT,
    CASE WHEN invalid_receipt_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_completed_rows_valid',
    '0',
    invalid_completed_rows::TEXT,
    CASE WHEN invalid_completed_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_rejected_rows_valid',
    '0',
    invalid_rejected_rows::TEXT,
    CASE WHEN invalid_rejected_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_failed_rows_valid',
    '0',
    invalid_failed_rows::TEXT,
    CASE WHEN invalid_failed_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_rehearsal_side_effects_safe',
    '0',
    unsafe_rehearsal_rows::TEXT,
    CASE WHEN unsafe_rehearsal_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_activation_checklist_side_effects_safe',
    '0',
    unsafe_checklist_rows::TEXT,
    CASE WHEN unsafe_checklist_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'controlled_receiver_rehearsals_inside_review_window',
    '0',
    rehearsals_outside_review_window::TEXT,
    CASE WHEN rehearsals_outside_review_window = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
