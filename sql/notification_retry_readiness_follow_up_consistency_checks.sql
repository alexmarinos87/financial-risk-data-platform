-- Reconciliation for readiness-aware current notification retry views.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_destination_execution_history)
            AS expected_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_execution_history)
            AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_destination_execution_history history
         JOIN risk_platform.notification_retry_readiness_bindings readiness
           ON readiness.terminal_record_id = history.record_id)
            AS expected_bound_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_readiness_execution_history
         WHERE readiness_bound)
            AS bound_history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_retry_destination_by_event)
            AS expected_latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_notification_retry_readiness_by_event)
            AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_follow_up)
            AS expected_follow_up_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_follow_up)
            AS follow_up_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_failures)
            AS expected_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_failures)
            AS failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_destination_ambiguities)
            AS expected_ambiguity_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_ambiguities)
            AS ambiguity_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_follow_up
         WHERE latest_execution_record_id IS NOT NULL
           AND readiness_binding_status IN (
               'readiness_binding_missing',
               'readiness_destination_mismatch'
           )) AS expected_review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_binding_reviews)
            AS review_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_follow_up
         WHERE readiness_binding_status = 'bound') AS expected_bound_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_readiness_bound)
            AS current_bound_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT record_id, event_id, event_ordinal
                FROM risk_platform.notification_retry_readiness_execution_history
                GROUP BY record_id, event_id, event_ordinal
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_history_grains,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.latest_notification_retry_readiness_by_event
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_retry_readiness_by_event readiness
            FULL JOIN risk_platform.latest_notification_retry_destination_by_event latest
              ON latest.event_id = readiness.event_id
            WHERE latest.event_id IS NULL
               OR readiness.event_id IS NULL
               OR readiness.record_id <> latest.record_id
               OR readiness.finished_at <> latest.finished_at
               OR readiness.event_ordinal <> latest.event_ordinal
        ) AS latest_selection_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_execution_history history
            WHERE history.readiness_bound
              AND (
                  history.readiness_binding_id IS NULL
                  OR history.readiness_record_id IS NULL
                  OR history.readiness_request_id IS NULL
                  OR history.retained_decision_id IS NULL
                  OR history.refreshed_decision_id IS NULL
                  OR history.enforcement_id IS NULL
                  OR history.readiness_destination_id IS NULL
                  OR history.readiness_enforced_at IS NULL
                  OR history.readiness_lock_model_version IS NULL
                  OR history.readiness_lock_scope IS NULL
                  OR history.readiness_lock_key_fingerprint IS NULL
                  OR history.readiness_binding_recorded_at IS NULL
                  OR history.readiness_terminal_document_sha256 IS NULL
                  OR history.readiness_enforcement_sha256 IS NULL
                  OR history.readiness_binding_document_sha256 IS NULL
              )
        ) AS incomplete_bound_history,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_readiness_execution_history history
            WHERE NOT history.readiness_bound
              AND (
                  history.readiness_binding_id IS NOT NULL
                  OR history.readiness_record_id IS NOT NULL
                  OR history.readiness_request_id IS NOT NULL
                  OR history.retained_decision_id IS NOT NULL
                  OR history.refreshed_decision_id IS NOT NULL
                  OR history.enforcement_id IS NOT NULL
                  OR history.readiness_destination_id IS NOT NULL
                  OR history.readiness_enforced_at IS NOT NULL
                  OR history.readiness_binding_recorded_at IS NOT NULL
              )
        ) AS contaminated_unbound_history,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.current_notification_retry_readiness_follow_up
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_follow_up_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_follow_up follow_up
            WHERE follow_up.readiness_binding_status <>
                CASE
                    WHEN follow_up.latest_execution_record_id IS NULL
                        THEN 'not_applicable'
                    WHEN NOT follow_up.readiness_bound
                        THEN 'readiness_binding_missing'
                    WHEN follow_up.destination_bound
                         AND follow_up.readiness_destination_id
                            <> follow_up.destination_id
                        THEN 'readiness_destination_mismatch'
                    ELSE 'bound'
                END
        ) AS invalid_binding_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_follow_up follow_up
            WHERE follow_up.readiness_review_required <>
                CASE
                    WHEN follow_up.latest_execution_record_id IS NULL THEN FALSE
                    WHEN NOT follow_up.readiness_bound THEN TRUE
                    WHEN follow_up.destination_bound
                         AND follow_up.readiness_destination_id
                            <> follow_up.destination_id
                        THEN TRUE
                    ELSE FALSE
                END
               OR follow_up.readiness_destination_matches IS DISTINCT FROM
                CASE
                    WHEN follow_up.latest_execution_record_id IS NULL THEN NULL
                    WHEN NOT follow_up.readiness_bound THEN NULL
                    WHEN NOT follow_up.destination_bound THEN NULL
                    ELSE follow_up.readiness_destination_id
                        = follow_up.destination_id
                END
        ) AS invalid_review_flags,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_follow_up follow_up
            JOIN risk_platform.latest_notification_retry_readiness_by_event latest
              ON latest.event_id = follow_up.event_id
            WHERE follow_up.latest_execution_record_id <> latest.record_id
               OR follow_up.readiness_bound <> latest.readiness_bound
               OR follow_up.readiness_binding_id
                    IS DISTINCT FROM latest.readiness_binding_id
               OR follow_up.readiness_record_id
                    IS DISTINCT FROM latest.readiness_record_id
               OR follow_up.enforcement_id
                    IS DISTINCT FROM latest.enforcement_id
        ) AS follow_up_identity_mismatches,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_binding_reviews review
            WHERE review.latest_execution_record_id IS NULL
               OR NOT review.readiness_review_required
               OR review.readiness_binding_status NOT IN (
                   'readiness_binding_missing',
                   'readiness_destination_mismatch'
               )
        ) AS invalid_review_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_failures failure
            WHERE NOT failure.delivery_failure
        ) AS invalid_failure_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_ambiguities ambiguity
            WHERE NOT ambiguity.ambiguous_outcome
               OR ambiguity.follow_up_reason <> 'persistence_review_required'
        ) AS invalid_ambiguity_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_bound bound
            WHERE NOT bound.readiness_bound
               OR bound.readiness_binding_status <> 'bound'
               OR bound.readiness_review_required
        ) AS invalid_current_bound_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_readiness_follow_up follow_up
            WHERE follow_up.readiness_binding_id IS NOT NULL
              AND follow_up.latest_execution_record_id IS DISTINCT FROM (
                  SELECT binding.terminal_record_id
                  FROM risk_platform.notification_retry_readiness_bindings binding
                  WHERE binding.binding_id = follow_up.readiness_binding_id
              )
        ) AS superseded_binding_leaks
)
SELECT
    'notification_retry_readiness_history_preserves_grain' AS check_name,
    expected_history_rows::TEXT AS expected,
    history_rows::TEXT AS actual,
    CASE WHEN expected_history_rows = history_rows THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_bound_history_matches',
    expected_bound_history_rows::TEXT,
    bound_history_rows::TEXT,
    CASE
        WHEN expected_bound_history_rows = bound_history_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_history_grain_unique',
    '0',
    duplicate_history_grains::TEXT,
    CASE WHEN duplicate_history_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_latest_preserves_grain',
    expected_latest_rows::TEXT,
    latest_rows::TEXT,
    CASE WHEN expected_latest_rows = latest_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_latest_grain_unique',
    '0',
    duplicate_latest_events::TEXT,
    CASE WHEN duplicate_latest_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_latest_selection_matches',
    '0',
    latest_selection_mismatches::TEXT,
    CASE WHEN latest_selection_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_bound_history_complete',
    '0',
    incomplete_bound_history::TEXT,
    CASE WHEN incomplete_bound_history = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_unbound_history_clean',
    '0',
    contaminated_unbound_history::TEXT,
    CASE WHEN contaminated_unbound_history = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_follow_up_covers_current',
    expected_follow_up_rows::TEXT,
    follow_up_rows::TEXT,
    CASE WHEN expected_follow_up_rows = follow_up_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_follow_up_grain_unique',
    '0',
    duplicate_follow_up_events::TEXT,
    CASE WHEN duplicate_follow_up_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_binding_status_valid',
    '0',
    invalid_binding_statuses::TEXT,
    CASE WHEN invalid_binding_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_review_flags_valid',
    '0',
    invalid_review_flags::TEXT,
    CASE WHEN invalid_review_flags = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_follow_up_identity_matches',
    '0',
    follow_up_identity_mismatches::TEXT,
    CASE WHEN follow_up_identity_mismatches = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_review_partition_matches',
    expected_review_rows::TEXT,
    review_rows::TEXT,
    CASE WHEN expected_review_rows = review_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_review_rows_valid',
    '0',
    invalid_review_rows::TEXT,
    CASE WHEN invalid_review_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_failure_partition_matches',
    expected_failure_rows::TEXT,
    failure_rows::TEXT,
    CASE WHEN expected_failure_rows = failure_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_failure_rows_valid',
    '0',
    invalid_failure_rows::TEXT,
    CASE WHEN invalid_failure_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_ambiguity_partition_matches',
    expected_ambiguity_rows::TEXT,
    ambiguity_rows::TEXT,
    CASE WHEN expected_ambiguity_rows = ambiguity_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_ambiguity_rows_valid',
    '0',
    invalid_ambiguity_rows::TEXT,
    CASE WHEN invalid_ambiguity_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_bound_partition_matches',
    expected_bound_rows::TEXT,
    current_bound_rows::TEXT,
    CASE WHEN expected_bound_rows = current_bound_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_readiness_bound_rows_valid',
    '0',
    invalid_current_bound_rows::TEXT,
    CASE WHEN invalid_current_bound_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_readiness_superseded_binding_excluded',
    '0',
    superseded_binding_leaks::TEXT,
    CASE WHEN superseded_binding_leaks = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
