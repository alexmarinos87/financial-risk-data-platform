-- Reconciliation for destination-aware notification retry serving views.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_execution_event_history)
            AS expected_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_destination_execution_history)
            AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_execution_event_history history
         JOIN risk_platform.portfolio_risk_notification_retry_destination_bindings binding
           ON binding.record_id = history.record_id)
            AS expected_bound_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_destination_execution_history
         WHERE destination_bound)
            AS bound_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_retry_execution_by_event)
            AS expected_latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_retry_destination_by_event)
            AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_follow_up)
            AS expected_follow_up_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_follow_up)
            AS follow_up_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_delivery_failures)
            AS expected_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_failures)
            AS failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_ambiguous_outcomes)
            AS expected_ambiguous_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_ambiguities)
            AS ambiguous_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_follow_up
         WHERE latest_execution_record_id IS NOT NULL
           AND NOT destination_bound)
            AS expected_binding_review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_binding_reviews)
            AS binding_review_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT record_id, event_id
                FROM risk_platform.notification_retry_destination_execution_history
                GROUP BY record_id, event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_history_grains,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.latest_notification_retry_destination_by_event
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_retry_destination_by_event destination
            FULL JOIN risk_platform.latest_notification_retry_execution_by_event latest
              ON latest.event_id = destination.event_id
            WHERE latest.event_id IS NULL
               OR destination.event_id IS NULL
               OR destination.record_id <> latest.record_id
               OR destination.finished_at <> latest.finished_at
               OR destination.event_ordinal <> latest.event_ordinal
        ) AS latest_selection_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_destination_execution_history history
            WHERE history.destination_bound
              AND (
                  history.binding_id IS NULL
                  OR history.destination_authority_id IS NULL
                  OR history.destination_id IS NULL
                  OR history.destination_fingerprint IS NULL
                  OR history.endpoint_environment_variable IS NULL
                  OR history.destination_evaluated_at IS NULL
                  OR history.destination_binding_recorded_at IS NULL
                  OR history.evaluated_event_types_json IS NULL
              )
        ) AS incomplete_bound_history,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_destination_execution_history history
            WHERE NOT history.destination_bound
              AND (
                  history.binding_id IS NOT NULL
                  OR history.destination_authority_id IS NOT NULL
                  OR history.destination_id IS NOT NULL
                  OR history.destination_fingerprint IS NOT NULL
                  OR history.endpoint_environment_variable IS NOT NULL
                  OR history.destination_evaluated_at IS NOT NULL
                  OR history.destination_binding_recorded_at IS NOT NULL
                  OR history.evaluated_event_types_json IS NOT NULL
              )
        ) AS contaminated_unbound_history,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.current_notification_retry_destination_follow_up
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_follow_up_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_destination_follow_up follow_up
            WHERE (
                follow_up.latest_execution_record_id IS NULL
                AND (
                    follow_up.destination_binding_status <> 'not_applicable'
                    OR follow_up.destination_bound
                    OR follow_up.destination_review_required
                )
            ) OR (
                follow_up.latest_execution_record_id IS NOT NULL
                AND follow_up.destination_bound
                AND (
                    follow_up.destination_binding_status <> 'bound'
                    OR follow_up.destination_review_required
                    OR follow_up.binding_id IS NULL
                )
            ) OR (
                follow_up.latest_execution_record_id IS NOT NULL
                AND NOT follow_up.destination_bound
                AND (
                    follow_up.destination_binding_status
                        <> 'destination_binding_missing'
                    OR NOT follow_up.destination_review_required
                    OR follow_up.binding_id IS NOT NULL
                )
            )
        ) AS invalid_follow_up_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_destination_follow_up follow_up
            JOIN risk_platform.latest_notification_retry_destination_by_event latest
              ON latest.event_id = follow_up.event_id
            WHERE follow_up.latest_execution_record_id <> latest.record_id
               OR follow_up.destination_bound <> latest.destination_bound
               OR follow_up.binding_id IS DISTINCT FROM latest.binding_id
               OR follow_up.destination_id IS DISTINCT FROM latest.destination_id
               OR follow_up.destination_fingerprint
                    IS DISTINCT FROM latest.destination_fingerprint
        ) AS follow_up_identity_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_destination_binding_reviews review
            WHERE review.latest_execution_record_id IS NULL
               OR review.destination_bound
               OR review.destination_binding_status
                    <> 'destination_binding_missing'
               OR NOT review.destination_review_required
        ) AS invalid_binding_review_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_destination_failures failure
            WHERE NOT failure.delivery_failure
        ) AS invalid_failure_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_destination_ambiguities ambiguous
            WHERE NOT ambiguous.ambiguous_outcome
               OR ambiguous.follow_up_reason <> 'persistence_review_required'
        ) AS invalid_ambiguous_rows
)
SELECT
    'notification_retry_destination_history_preserves_grain' AS check_name,
    expected_history_rows::TEXT AS expected,
    history_rows::TEXT AS actual,
    CASE WHEN expected_history_rows = history_rows THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_bound_history_matches',
    expected_bound_history_rows::TEXT,
    bound_history_rows::TEXT,
    CASE
        WHEN expected_bound_history_rows = bound_history_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_history_grain_unique',
    '0',
    duplicate_history_grains::TEXT,
    CASE WHEN duplicate_history_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_latest_preserves_grain',
    expected_latest_rows::TEXT,
    latest_rows::TEXT,
    CASE WHEN expected_latest_rows = latest_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_latest_grain_unique',
    '0',
    duplicate_latest_events::TEXT,
    CASE WHEN duplicate_latest_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_latest_selection_matches',
    '0',
    latest_selection_mismatches::TEXT,
    CASE WHEN latest_selection_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_bound_history_complete',
    '0',
    incomplete_bound_history::TEXT,
    CASE WHEN incomplete_bound_history = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_unbound_history_clean',
    '0',
    contaminated_unbound_history::TEXT,
    CASE WHEN contaminated_unbound_history = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_follow_up_covers_current',
    expected_follow_up_rows::TEXT,
    follow_up_rows::TEXT,
    CASE WHEN expected_follow_up_rows = follow_up_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_follow_up_grain_unique',
    '0',
    duplicate_follow_up_events::TEXT,
    CASE WHEN duplicate_follow_up_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_follow_up_status_valid',
    '0',
    invalid_follow_up_statuses::TEXT,
    CASE WHEN invalid_follow_up_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_follow_up_identity_matches',
    '0',
    follow_up_identity_mismatches::TEXT,
    CASE WHEN follow_up_identity_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_binding_review_partition_matches',
    expected_binding_review_rows::TEXT,
    binding_review_rows::TEXT,
    CASE
        WHEN expected_binding_review_rows = binding_review_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_binding_review_rows_valid',
    '0',
    invalid_binding_review_rows::TEXT,
    CASE WHEN invalid_binding_review_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_failure_partition_matches',
    expected_failure_rows::TEXT,
    failure_rows::TEXT,
    CASE WHEN expected_failure_rows = failure_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_failure_rows_valid',
    '0',
    invalid_failure_rows::TEXT,
    CASE WHEN invalid_failure_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_destination_ambiguity_partition_matches',
    expected_ambiguous_rows::TEXT,
    ambiguous_rows::TEXT,
    CASE WHEN expected_ambiguous_rows = ambiguous_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_destination_ambiguity_rows_valid',
    '0',
    invalid_ambiguous_rows::TEXT,
    CASE WHEN invalid_ambiguous_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
