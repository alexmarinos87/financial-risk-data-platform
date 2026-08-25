-- Reconciliation checks for current notification retry operational views.

WITH counts AS (
    SELECT
        (SELECT COALESCE(SUM(request_count), 0)
         FROM risk_platform.portfolio_risk_notification_retry_executions)
            AS expected_requested_events,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_execution_event_history)
            AS event_history_rows,
        (SELECT COALESCE(SUM(attempts_persisted), 0)
         FROM risk_platform.portfolio_risk_notification_retry_executions)
            AS expected_persisted_events,
        (SELECT COUNT(*)
         FROM risk_platform.notification_retry_execution_event_history
         WHERE persisted_event_recorded)
            AS persisted_event_rows,
        (SELECT COUNT(*)
         FROM risk_platform.portfolio_risk_notification_pending)
            AS pending_events,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_follow_up)
            AS follow_up_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_follow_up
         WHERE delivery_failure)
            AS expected_failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_delivery_failures)
            AS failure_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_retry_follow_up
         WHERE ambiguous_outcome)
            AS expected_ambiguous_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_notification_ambiguous_outcomes)
            AS ambiguous_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT record_id, event_id
                FROM risk_platform.notification_retry_execution_event_history
                GROUP BY record_id, event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_history_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.notification_retry_execution_event_history
            WHERE persisted_event_recorded
              AND NOT attempt_evidence_confirmed
        ) AS missing_attempt_evidence,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.latest_notification_retry_execution_by_event
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_notification_retry_execution_by_event latest
            WHERE latest.current_version_rank <> 1
               OR EXISTS (
                   SELECT 1
                   FROM risk_platform.notification_retry_execution_event_history candidate
                   WHERE candidate.event_id = latest.event_id
                     AND (
                         candidate.finished_at,
                         candidate.record_id,
                         candidate.event_ordinal
                     ) > (
                         latest.finished_at,
                         latest.record_id,
                         latest.event_ordinal
                     )
               )
        ) AS stale_latest_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_persistence_uncertainty
                uncertainty
            WHERE uncertainty.terminal_status <> 'persistence_uncertain'
               OR uncertainty.attempt_evidence_confirmed
               OR EXISTS (
                   SELECT 1
                   FROM risk_platform.portfolio_risk_notification_delivery_attempts
                       attempt
                   WHERE attempt.event_id = uncertainty.event_id
                     AND attempt.channel = 'webhook'
                     AND attempt.attempted_at >= uncertainty.started_at
               )
        ) AS invalid_uncertainty_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT event_id
                FROM risk_platform.current_notification_retry_follow_up
                GROUP BY event_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_follow_up_events,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE follow_up.delivered
              AND (
                  follow_up.follow_up_reason <> 'delivered'
                  OR follow_up.follow_up_required
                  OR follow_up.delivery_failure
                  OR follow_up.ambiguous_outcome
              )
        ) AS invalid_delivered_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE NOT follow_up.delivered
              AND follow_up.acknowledgement_id IS NOT NULL
              AND (
                  follow_up.follow_up_reason <> 'acknowledged'
                  OR follow_up.follow_up_required
                  OR follow_up.delivery_failure
                  OR follow_up.ambiguous_outcome
              )
        ) AS invalid_acknowledged_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE follow_up.follow_up_reason = 'persistence_review_required'
              AND (
                  follow_up.uncertainty_record_id IS NULL
                  OR NOT follow_up.follow_up_required
                  OR NOT follow_up.delivery_failure
                  OR NOT follow_up.ambiguous_outcome
              )
        ) AS invalid_persistence_review_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE follow_up.follow_up_reason = 'initial_delivery_required'
              AND (
                  follow_up.attempt_count <> 0
                  OR NOT follow_up.follow_up_required
                  OR follow_up.delivery_failure
              )
        ) AS invalid_initial_delivery_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE follow_up.follow_up_reason = 'retry_plan_required'
              AND (
                  follow_up.attempt_count = 0
                  OR follow_up.failed_attempt_count = 0
                  OR follow_up.delivered
                  OR NOT follow_up.follow_up_required
                  OR NOT follow_up.delivery_failure
              )
        ) AS invalid_retry_plan_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_retry_follow_up follow_up
            WHERE follow_up.follow_up_reason = 'execution_review_required'
              AND (
                  follow_up.latest_execution_terminal_status NOT IN (
                      'failed_after_request',
                      'persistence_uncertain'
                  )
                  OR NOT follow_up.follow_up_required
                  OR NOT follow_up.delivery_failure
              )
        ) AS invalid_execution_review_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_delivery_failures failure
            WHERE NOT failure.delivery_failure
               OR failure.follow_up_reason = 'initial_delivery_required'
        ) AS invalid_failure_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_notification_ambiguous_outcomes ambiguous
            WHERE NOT ambiguous.ambiguous_outcome
               OR ambiguous.follow_up_reason <> 'persistence_review_required'
        ) AS invalid_ambiguous_rows
)
SELECT
    'notification_retry_history_expands_requested_events' AS check_name,
    expected_requested_events::TEXT AS expected,
    event_history_rows::TEXT AS actual,
    CASE
        WHEN expected_requested_events = event_history_rows THEN 'pass'
        ELSE 'fail'
    END AS status
FROM counts

UNION ALL

SELECT
    'notification_retry_history_expands_persisted_events',
    expected_persisted_events::TEXT,
    persisted_event_rows::TEXT,
    CASE
        WHEN expected_persisted_events = persisted_event_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'notification_retry_history_grain_unique',
    '0',
    duplicate_history_grains::TEXT,
    CASE WHEN duplicate_history_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_persisted_events_reference_attempts',
    '0',
    missing_attempt_evidence::TEXT,
    CASE WHEN missing_attempt_evidence = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_latest_event_grain_unique',
    '0',
    duplicate_latest_events::TEXT,
    CASE WHEN duplicate_latest_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_latest_event_selection_current',
    '0',
    stale_latest_events::TEXT,
    CASE WHEN stale_latest_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_current_uncertainty_valid',
    '0',
    invalid_uncertainty_rows::TEXT,
    CASE WHEN invalid_uncertainty_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_follow_up_covers_pending_events',
    pending_events::TEXT,
    follow_up_rows::TEXT,
    CASE WHEN pending_events = follow_up_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_follow_up_grain_unique',
    '0',
    duplicate_follow_up_events::TEXT,
    CASE WHEN duplicate_follow_up_events = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_delivered_rows_closed',
    '0',
    invalid_delivered_rows::TEXT,
    CASE WHEN invalid_delivered_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_acknowledged_rows_closed',
    '0',
    invalid_acknowledged_rows::TEXT,
    CASE WHEN invalid_acknowledged_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_persistence_reviews_valid',
    '0',
    invalid_persistence_review_rows::TEXT,
    CASE WHEN invalid_persistence_review_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_initial_delivery_reviews_valid',
    '0',
    invalid_initial_delivery_rows::TEXT,
    CASE WHEN invalid_initial_delivery_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_plan_reviews_valid',
    '0',
    invalid_retry_plan_rows::TEXT,
    CASE WHEN invalid_retry_plan_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_execution_reviews_valid',
    '0',
    invalid_execution_review_rows::TEXT,
    CASE WHEN invalid_execution_review_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_delivery_failure_partition_matches',
    expected_failure_rows::TEXT,
    failure_rows::TEXT,
    CASE WHEN expected_failure_rows = failure_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_delivery_failure_rows_valid',
    '0',
    invalid_failure_rows::TEXT,
    CASE WHEN invalid_failure_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'notification_retry_ambiguous_partition_matches',
    expected_ambiguous_rows::TEXT,
    ambiguous_rows::TEXT,
    CASE WHEN expected_ambiguous_rows = ambiguous_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'notification_retry_ambiguous_rows_valid',
    '0',
    invalid_ambiguous_rows::TEXT,
    CASE WHEN invalid_ambiguous_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
