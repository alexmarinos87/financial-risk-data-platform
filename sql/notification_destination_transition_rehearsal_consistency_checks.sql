-- Reconciliation for append-only destination transition rehearsal history.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.notification_destination_transition_rehearsals)
            AS rehearsal_rows,
        (SELECT COUNT(DISTINCT request_id)
         FROM risk_platform.notification_destination_transition_rehearsals)
            AS distinct_request_ids,
        (SELECT COUNT(DISTINCT rehearsal_id)
         FROM risk_platform.notification_destination_transition_rehearsals)
            AS distinct_rehearsal_ids,
        (SELECT COUNT(DISTINCT destination_id)
         FROM risk_platform.notification_destination_transition_rehearsals)
            AS expected_latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_destination_transition_rehearsals)
            AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_activation_rehearsal_review)
            AS expected_review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_destination_transition_review)
            AS review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_destination_transition_review
         WHERE transition_review_required)
            AS expected_failure_rows,
        (SELECT COUNT(*)
         FROM
            risk_platform.current_notification_destination_transition_review_failures)
            AS failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_destination_transition_review
         WHERE transition_ready)
            AS expected_ready_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_destination_transition_ready)
            AS ready_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_destination_transition_rehearsals
            WHERE (rehearsal_json #>> '{stages,0,request_count}')::INTEGER
                    <> rotate_request_count
               OR (rehearsal_json #>> '{stages,2,request_count}')::INTEGER
                    <> rollback_request_count
               OR (
                    (rehearsal_json #>>
                        '{stages,0,receiver_summary,same_content_duplicate_count}'
                    )::INTEGER
                    +
                    (rehearsal_json #>>
                        '{stages,2,receiver_summary,same_content_duplicate_count}'
                    )::INTEGER
                  ) <> same_content_duplicate_count
        ) AS invalid_request_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_destination_transition_rehearsals
            WHERE rehearsal_json #>> '{stages,1,operation}' <> 'disable'
               OR (rehearsal_json #> '{stages,1,authority_id}') <> 'null'::jsonb
               OR (rehearsal_json #> '{stages,1,checklist_id}') <> 'null'::jsonb
               OR (rehearsal_json #> '{stages,1,receiver_summary}')
                    <> 'null'::jsonb
               OR (rehearsal_json #>> '{stages,1,request_count}')::INTEGER <> 0
               OR (rehearsal_json #> '{stages,1,requested_event_ids}')
                    <> '[]'::jsonb
               OR (rehearsal_json #> '{stages,1,requested_event_types}')
                    <> '[]'::jsonb
               OR (rehearsal_json #>> '{stages,1,target_authority_required}')
                    ::BOOLEAN
        ) AS invalid_disable_stages,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_destination_transition_rehearsals
            WHERE started_at > finished_at
               OR finished_at > recorded_at
               OR (rehearsal_json ->> 'started_at')::TIMESTAMPTZ <> started_at
               OR (rehearsal_json ->> 'finished_at')::TIMESTAMPTZ <> finished_at
        ) AS invalid_timestamps,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_destination_transition_rehearsals
            WHERE (rehearsal_json ->> 'external_request_performed')::BOOLEAN
               OR (rehearsal_json ->> 'socket_opened')::BOOLEAN
               OR (rehearsal_json ->> 'dns_lookup_performed')::BOOLEAN
               OR (rehearsal_json ->> 'delivery_attempt_written')::BOOLEAN
               OR (rehearsal_json ->> 'outbox_mutated')::BOOLEAN
               OR (rehearsal_json ->> 'acknowledgement_mutated')::BOOLEAN
               OR (rehearsal_json ->> 'endpoint_values_recorded')::BOOLEAN
               OR (rehearsal_json ->> 'endpoint_paths_recorded')::BOOLEAN
               OR (rehearsal_json ->> 'payload_bodies_recorded')::BOOLEAN
               OR (rehearsal_json ->> 'response_bodies_recorded')::BOOLEAN
               OR (rehearsal_json ->> 'infrastructure_deployed')::BOOLEAN
        ) AS unsafe_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id
                FROM risk_platform.latest_notification_destination_transition_rehearsals
                GROUP BY destination_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_destinations,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_destination_transition_rehearsals
                latest
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.notification_destination_transition_rehearsals
                    candidate
                WHERE candidate.destination_id = latest.destination_id
                  AND (
                      candidate.finished_at > latest.finished_at
                      OR (
                          candidate.finished_at = latest.finished_at
                          AND candidate.record_id > latest.record_id
                      )
                  )
            )
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT destination_id
                FROM risk_platform.current_notification_destination_transition_review
                GROUP BY destination_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_review_destinations,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_destination_transition_review
            WHERE transition_review_status <>
                CASE
                    WHEN NOT operational_activation_ready
                        THEN 'activation_not_ready'
                    WHEN transition_record_id IS NULL
                        THEN 'transition_rehearsal_missing'
                    WHEN NOT transition_matches_current_activation
                        THEN 'transition_rehearsal_superseded'
                    ELSE 'ready'
                END
        ) AS invalid_review_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_destination_transition_review
            WHERE transition_review_required
                    <> (transition_review_status <> 'ready')
               OR transition_ready <> (transition_review_status = 'ready')
               OR transition_review_priority <>
                    CASE transition_review_status
                        WHEN 'activation_not_ready' THEN 3
                        WHEN 'transition_rehearsal_superseded' THEN 2
                        WHEN 'transition_rehearsal_missing' THEN 1
                        ELSE 0
                    END
        ) AS invalid_review_flags,
        (
            SELECT COUNT(*)
            FROM pg_trigger trigger
            JOIN pg_class relation ON relation.oid = trigger.tgrelid
            JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
            WHERE namespace.nspname = 'risk_platform'
              AND relation.relname =
                    'notification_destination_transition_rehearsals'
              AND trigger.tgenabled <> 'D'
              AND NOT trigger.tgisinternal
              AND trigger.tgname IN (
                  'notification_destination_transition_rehearsals_reject_update',
                  'notification_destination_transition_rehearsals_reject_delete'
              )
        ) AS append_only_trigger_count
)
SELECT
    'notification_destination_transition_request_ids_unique' AS check_name,
    rehearsal_rows::TEXT AS expected,
    distinct_request_ids::TEXT AS actual,
    CASE WHEN rehearsal_rows = distinct_request_ids THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_rehearsal_ids_unique',
    rehearsal_rows::TEXT,
    distinct_rehearsal_ids::TEXT,
    CASE WHEN rehearsal_rows = distinct_rehearsal_ids THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_request_counts_reconcile',
    '0',
    invalid_request_counts::TEXT,
    CASE WHEN invalid_request_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_disable_stages_safe',
    '0',
    invalid_disable_stages::TEXT,
    CASE WHEN invalid_disable_stages = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_timestamps_valid',
    '0',
    invalid_timestamps::TEXT,
    CASE WHEN invalid_timestamps = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_side_effects_safe',
    '0',
    unsafe_rows::TEXT,
    CASE WHEN unsafe_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_latest_rows_match_destinations',
    expected_latest_rows::TEXT,
    latest_rows::TEXT,
    CASE WHEN expected_latest_rows = latest_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_latest_grain_unique',
    '0',
    duplicate_latest_destinations::TEXT,
    CASE WHEN duplicate_latest_destinations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_latest_selection_current',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_review_covers_activation',
    expected_review_rows::TEXT,
    review_rows::TEXT,
    CASE WHEN expected_review_rows = review_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_review_grain_unique',
    '0',
    duplicate_review_destinations::TEXT,
    CASE WHEN duplicate_review_destinations = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_review_status_reconciles',
    '0',
    invalid_review_statuses::TEXT,
    CASE WHEN invalid_review_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_review_flags_reconcile',
    '0',
    invalid_review_flags::TEXT,
    CASE WHEN invalid_review_flags = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_destination_transition_failure_partition_matches',
    expected_failure_rows::TEXT,
    failure_rows::TEXT,
    CASE WHEN expected_failure_rows = failure_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_ready_partition_matches',
    expected_ready_rows::TEXT,
    ready_rows::TEXT,
    CASE WHEN expected_ready_rows = ready_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_destination_transition_append_only_triggers_enabled',
    '2',
    append_only_trigger_count::TEXT,
    CASE WHEN append_only_trigger_count = 2 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
