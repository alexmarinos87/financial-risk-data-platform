-- Reconciliation for append-only notification execution readiness history.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.notification_execution_readiness_decisions)
            AS decision_rows,
        (SELECT COUNT(DISTINCT request_id)
         FROM risk_platform.notification_execution_readiness_decisions)
            AS distinct_request_ids,
        (SELECT COUNT(DISTINCT decision_id)
         FROM risk_platform.notification_execution_readiness_decisions)
            AS distinct_decision_ids,
        (SELECT COUNT(*)
         FROM (
             SELECT destination_id, execution_kind
             FROM risk_platform.notification_execution_readiness_decisions
             GROUP BY destination_id, execution_kind
         ) grains) AS expected_latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_execution_readiness_decisions)
            AS latest_rows,
        (SELECT COUNT(*) * 2
         FROM risk_platform.current_notification_destination_transition_review)
            AS expected_review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review)
            AS review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review
         WHERE readiness_review_status = 'allowed') AS expected_allowed_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_allowed)
            AS allowed_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review
         WHERE readiness_review_status = 'blocked') AS expected_blocked_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_blocked)
            AS blocked_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review
         WHERE readiness_review_status = 'decision_stale') AS expected_stale_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_stale)
            AS stale_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review
         WHERE readiness_review_status = 'decision_superseded')
            AS expected_superseded_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_superseded)
            AS superseded_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_review
         WHERE readiness_review_status = 'decision_missing') AS expected_missing_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_execution_readiness_missing)
            AS missing_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_execution_readiness_decisions
            WHERE decision_json ->> 'decision_id' <> decision_id
               OR decision_json ->> 'execution_kind' <> execution_kind
               OR decision_json ->> 'decision' <> decision
               OR decision_json -> 'blocking_reasons' <> blocking_reasons_json
               OR decision_json -> 'destination' ->> 'destination_id'
                    <> destination_id
               OR decision_json -> 'destination' ->> 'fingerprint'
                    <> destination_fingerprint
               OR record_json ->> 'record_id' <> record_id
               OR record_json ->> 'request_id' <> request_id
               OR record_json -> 'decision' <> decision_json
        ) AS invalid_document_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_execution_readiness_decisions
            WHERE recorded_at < evaluated_at
               OR jsonb_array_length(ambiguity_event_ids_json) <> ambiguity_count
               OR jsonb_array_length(ambiguity_record_ids_json) <> ambiguity_count
               OR jsonb_array_length(unbound_ambiguity_event_ids_json)
                    > ambiguity_count
        ) AS invalid_counts_or_timestamps,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_execution_readiness_decisions
            WHERE NOT (decision_json ->> 'read_only')::BOOLEAN
               OR (decision_json ->> 'external_request_performed')::BOOLEAN
               OR (decision_json ->> 'delivery_attempt_written')::BOOLEAN
               OR (decision_json ->> 'outbox_mutated')::BOOLEAN
               OR (decision_json ->> 'acknowledgement_mutated')::BOOLEAN
        ) AS unsafe_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id, execution_kind
                FROM risk_platform.latest_notification_execution_readiness_decisions
                GROUP BY destination_id, execution_kind
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_execution_readiness_decisions latest
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.notification_execution_readiness_decisions candidate
                WHERE candidate.destination_id = latest.destination_id
                  AND candidate.execution_kind = latest.execution_kind
                  AND (
                      candidate.evaluated_at > latest.evaluated_at
                      OR (
                          candidate.evaluated_at = latest.evaluated_at
                          AND candidate.record_id > latest.record_id
                      )
                  )
            )
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id, execution_kind
                FROM risk_platform.current_notification_execution_readiness_review
                GROUP BY destination_id, execution_kind
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_review_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_execution_readiness_review
            WHERE readiness_review_status <>
                CASE
                    WHEN readiness_record_id IS NULL THEN 'decision_missing'
                    WHEN NOT decision_matches_current_evidence
                        THEN 'decision_superseded'
                    WHEN decision_evaluated_at
                            < CURRENT_TIMESTAMP - INTERVAL '5 minutes'
                        THEN 'decision_stale'
                    WHEN decision = 'block' THEN 'blocked'
                    ELSE 'allowed'
                END
        ) AS invalid_review_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_execution_readiness_review
            WHERE readiness_review_required
                    <> (readiness_review_status <> 'allowed')
               OR execution_ready <> (readiness_review_status = 'allowed')
               OR readiness_review_priority <>
                    CASE readiness_review_status
                        WHEN 'decision_superseded' THEN 4
                        WHEN 'decision_stale' THEN 3
                        WHEN 'blocked' THEN 2
                        WHEN 'decision_missing' THEN 1
                        ELSE 0
                    END
        ) AS invalid_review_flags,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class relation ON relation.oid = trigger.tgrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'risk_platform'
              AND relation.relname = 'notification_execution_readiness_decisions'
              AND trigger.tgname IN (
                  'notification_execution_readiness_reject_update',
                  'notification_execution_readiness_reject_delete'
              )
              AND trigger.tgenabled <> 'D'
              AND NOT trigger.tgisinternal
        ) AS append_only_trigger_count
)
SELECT
    'notification_execution_readiness_request_ids_unique' AS check_name,
    decision_rows::TEXT AS expected,
    distinct_request_ids::TEXT AS actual,
    CASE WHEN decision_rows = distinct_request_ids THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_decision_ids_unique',
    decision_rows::TEXT,
    distinct_decision_ids::TEXT,
    CASE WHEN decision_rows = distinct_decision_ids THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_documents_reconcile',
    '0',
    invalid_document_rows::TEXT,
    CASE WHEN invalid_document_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_counts_and_timestamps_valid',
    '0',
    invalid_counts_or_timestamps::TEXT,
    CASE WHEN invalid_counts_or_timestamps = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_side_effects_safe',
    '0',
    unsafe_rows::TEXT,
    CASE WHEN unsafe_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_latest_rows_match_grains',
    expected_latest_rows::TEXT,
    latest_rows::TEXT,
    CASE WHEN expected_latest_rows = latest_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_latest_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_latest_selection_current',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_review_covers_execution_kinds',
    expected_review_rows::TEXT,
    review_rows::TEXT,
    CASE WHEN expected_review_rows = review_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_review_grain_unique',
    '0',
    duplicate_review_grains::TEXT,
    CASE WHEN duplicate_review_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_review_status_reconciles',
    '0',
    invalid_review_statuses::TEXT,
    CASE WHEN invalid_review_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_review_flags_reconcile',
    '0',
    invalid_review_flags::TEXT,
    CASE WHEN invalid_review_flags = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_execution_readiness_allowed_partition_matches',
    expected_allowed_rows::TEXT,
    allowed_rows::TEXT,
    CASE WHEN expected_allowed_rows = allowed_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_blocked_partition_matches',
    expected_blocked_rows::TEXT,
    blocked_rows::TEXT,
    CASE WHEN expected_blocked_rows = blocked_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_stale_partition_matches',
    expected_stale_rows::TEXT,
    stale_rows::TEXT,
    CASE WHEN expected_stale_rows = stale_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_superseded_partition_matches',
    expected_superseded_rows::TEXT,
    superseded_rows::TEXT,
    CASE WHEN expected_superseded_rows = superseded_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_missing_partition_matches',
    expected_missing_rows::TEXT,
    missing_rows::TEXT,
    CASE WHEN expected_missing_rows = missing_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_execution_readiness_append_only_triggers_enabled',
    '2',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 2 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
