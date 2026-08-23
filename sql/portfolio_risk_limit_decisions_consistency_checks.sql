-- Reconciliation for append-only risk-limit decision serving.
-- Run after notification and decision schemas and warehouse loads.

WITH counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.current_portfolio_risk_limit_notifications
        ) AS current_notification_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_lifecycle
        ) AS lifecycle_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.open_portfolio_risk_limit_breaches
        ) AS open_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.acknowledged_portfolio_risk_limit_breaches
        ) AS acknowledged_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.resolved_portfolio_risk_limit_breaches
        ) AS resolved_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.waived_portfolio_risk_limit_breaches
        ) AS waived_rows
), integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_decisions decision_record
            LEFT JOIN risk_platform.portfolio_risk_limit_notifications notification
                ON notification.notification_id = decision_record.notification_id
            WHERE notification.notification_id IS NULL
        ) AS orphan_decisions,
        (
            SELECT COUNT(*)
            FROM (
                SELECT decision_id
                FROM risk_platform.portfolio_risk_limit_decisions
                GROUP BY decision_id
                HAVING COUNT(*) > 1
            ) duplicate
        ) AS duplicate_decision_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT notification_id
                FROM risk_platform.latest_portfolio_risk_limit_decisions
                GROUP BY notification_id
                HAVING COUNT(*) > 1
            ) duplicate
        ) AS duplicate_latest_notification_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_limit_decisions latest
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.portfolio_risk_limit_decisions candidate
                WHERE candidate.notification_id = latest.notification_id
                  AND (
                    candidate.decided_at,
                    candidate.decision_id
                  ) > (
                    latest.decided_at,
                    latest.decision_id
                  )
            )
        ) AS stale_latest_decisions,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_decisions
            WHERE ts_ingest < decided_at
               OR decision NOT IN ('acknowledged', 'resolved', 'waived')
               OR actor = ''
               OR reason = ''
               OR jsonb_typeof(record_json) <> 'object'
        ) AS invalid_decision_contract_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_breach_lifecycle lifecycle
            WHERE lifecycle_status NOT IN (
                    'open', 'acknowledged', 'resolved', 'waived'
                )
               OR requires_operator_decision
                    <> (latest_decision_id IS NULL)
               OR operationally_closed
                    <> (lifecycle_status IN ('resolved', 'waived'))
               OR (
                    latest_decision_id IS NULL
                    AND lifecycle_status <> 'open'
                )
               OR (
                    latest_decision = 'acknowledged'
                    AND lifecycle_status <> 'acknowledged'
                )
               OR (
                    latest_decision = 'resolved'
                    AND lifecycle_status <> 'resolved'
                )
               OR (
                    latest_decision = 'waived'
                    AND lifecycle_status <> 'waived'
                )
        ) AS invalid_lifecycle_rows
)
SELECT
    check_name,
    expected,
    actual,
    status
FROM (
    SELECT
        'risk_limit_decisions_reference_notifications' AS check_name,
        '0' AS expected,
        orphan_decisions::TEXT AS actual,
        CASE WHEN orphan_decisions = 0 THEN 'pass' ELSE 'fail' END AS status
    FROM integrity

    UNION ALL

    SELECT
        'risk_limit_decision_ids_unique',
        '0',
        duplicate_decision_ids::TEXT,
        CASE WHEN duplicate_decision_ids = 0 THEN 'pass' ELSE 'fail' END
    FROM integrity

    UNION ALL

    SELECT
        'latest_decision_notification_grain_unique',
        '0',
        duplicate_latest_notification_rows::TEXT,
        CASE
            WHEN duplicate_latest_notification_rows = 0 THEN 'pass'
            ELSE 'fail'
        END
    FROM integrity

    UNION ALL

    SELECT
        'latest_decision_selects_current_version',
        '0',
        stale_latest_decisions::TEXT,
        CASE WHEN stale_latest_decisions = 0 THEN 'pass' ELSE 'fail' END
    FROM integrity

    UNION ALL

    SELECT
        'risk_limit_decision_contract_valid',
        '0',
        invalid_decision_contract_rows::TEXT,
        CASE
            WHEN invalid_decision_contract_rows = 0 THEN 'pass'
            ELSE 'fail'
        END
    FROM integrity

    UNION ALL

    SELECT
        'risk_limit_decision_lifecycle_valid',
        '0',
        invalid_lifecycle_rows::TEXT,
        CASE WHEN invalid_lifecycle_rows = 0 THEN 'pass' ELSE 'fail' END
    FROM integrity

    UNION ALL

    SELECT
        'decision_lifecycle_rows_match_current_notifications',
        current_notification_rows::TEXT,
        lifecycle_rows::TEXT,
        CASE
            WHEN current_notification_rows = lifecycle_rows THEN 'pass'
            ELSE 'fail'
        END
    FROM counts

    UNION ALL

    SELECT
        'decision_lifecycle_partitions_current_notifications',
        current_notification_rows::TEXT,
        (open_rows + acknowledged_rows + resolved_rows + waived_rows)::TEXT,
        CASE
            WHEN current_notification_rows =
                open_rows + acknowledged_rows + resolved_rows + waived_rows
            THEN 'pass'
            ELSE 'fail'
        END
    FROM counts
) checks
ORDER BY check_name;
