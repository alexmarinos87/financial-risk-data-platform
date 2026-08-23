-- Reconciliation checks for append-only operational service-level history.

WITH report_counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.operational_service_level_reports) AS report_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_operational_service_level_reports) AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_service_level_metric_history) AS metric_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_metric_status)
            AS current_metric_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_exceptions)
            AS current_exception_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_metric_status
         WHERE metric_status IN ('warning', 'critical'))
            AS expected_current_exception_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_reports report
            WHERE jsonb_array_length(report.metrics_json) <> 4
                OR (
                    SELECT COUNT(DISTINCT metric.value ->> 'metric_name')
                    FROM jsonb_array_elements(report.metrics_json) metric(value)
                ) <> 4
                OR NOT (
                    SELECT ARRAY_AGG(
                        metric.value ->> 'metric_name'
                        ORDER BY metric.ordinality
                    )
                    FROM jsonb_array_elements(report.metrics_json)
                        WITH ORDINALITY metric(value, ordinality)
                ) = ARRAY[
                    'schedule_lag_sessions',
                    'market_freshness_exception_count',
                    'notification_retry_exhausted_count',
                    'notification_oldest_dead_letter_age_seconds'
                ]::TEXT[]
        ) AS invalid_metric_contracts,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_reports report
            WHERE report.overall_status <>
                CASE (
                    SELECT MAX(
                        CASE metric.value ->> 'status'
                            WHEN 'critical' THEN 2
                            WHEN 'warning' THEN 1
                            WHEN 'ok' THEN 0
                            ELSE 99
                        END
                    )
                    FROM jsonb_array_elements(report.metrics_json) metric(value)
                )
                    WHEN 2 THEN 'critical'
                    WHEN 1 THEN 'warning'
                    WHEN 0 THEN 'ok'
                    ELSE 'invalid'
                END
        ) AS invalid_overall_statuses,
        (
            SELECT COUNT(*)
            FROM (
                SELECT calculation_id
                FROM risk_platform.operational_service_level_reports
                GROUP BY calculation_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    policy_id,
                    policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    portfolio_id,
                    mandate_fingerprint
                FROM risk_platform.latest_operational_service_level_reports
                GROUP BY
                    policy_id,
                    policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    portfolio_id,
                    mandate_fingerprint
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_operational_service_level_reports latest
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.operational_service_level_reports candidate
                WHERE candidate.policy_id = latest.policy_id
                  AND candidate.policy_fingerprint = latest.policy_fingerprint
                  AND candidate.schedule_id = latest.schedule_id
                  AND candidate.schedule_fingerprint = latest.schedule_fingerprint
                  AND candidate.portfolio_id = latest.portfolio_id
                  AND candidate.mandate_fingerprint = latest.mandate_fingerprint
                  AND (candidate.as_of, candidate.calculation_id)
                      > (latest.as_of, latest.calculation_id)
            )
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_metric_history metric
            WHERE metric.metric_name NOT IN (
                    'schedule_lag_sessions',
                    'market_freshness_exception_count',
                    'notification_retry_exhausted_count',
                    'notification_oldest_dead_letter_age_seconds'
                )
               OR metric.metric_status NOT IN ('ok', 'warning', 'critical')
               OR metric.warning_threshold < 0
               OR metric.critical_threshold <= metric.warning_threshold
               OR (
                    metric.observed_value IS NULL
                    AND NOT (
                        metric.metric_name = 'schedule_lag_sessions'
                        AND metric.metric_status = 'critical'
                        AND metric.reason = 'checkpoint_missing'
                    )
               )
               OR (
                    metric.observed_value IS NOT NULL
                    AND (
                        metric.observed_value < 0
                        OR metric.reason IS NOT NULL
                        OR metric.metric_status <>
                            CASE
                                WHEN metric.observed_value
                                    >= metric.critical_threshold THEN 'critical'
                                WHEN metric.observed_value
                                    >= metric.warning_threshold THEN 'warning'
                                ELSE 'ok'
                            END
                    )
               )
        ) AS invalid_metric_values
)
SELECT
    'operational_service_level_reports_present' AS check_name,
    'at least 1' AS expected,
    report_rows::TEXT AS actual,
    CASE WHEN report_rows >= 1 THEN 'pass' ELSE 'fail' END AS status
FROM report_counts

UNION ALL

SELECT
    'operational_service_level_metric_contracts_valid',
    '0',
    invalid_metric_contracts::TEXT,
    CASE WHEN invalid_metric_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_service_level_metric_values_valid',
    '0',
    invalid_metric_values::TEXT,
    CASE WHEN invalid_metric_values = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_service_level_overall_status_reconciles',
    '0',
    invalid_overall_statuses::TEXT,
    CASE WHEN invalid_overall_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_service_level_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::TEXT,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_service_level_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_service_level_selects_current_report',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_service_level_metric_history_row_count',
    (report_rows * 4)::TEXT,
    metric_rows::TEXT,
    CASE WHEN metric_rows = report_rows * 4 THEN 'pass' ELSE 'fail' END
FROM report_counts

UNION ALL

SELECT
    'current_operational_service_level_metric_row_count',
    (latest_rows * 4)::TEXT,
    current_metric_rows::TEXT,
    CASE WHEN current_metric_rows = latest_rows * 4 THEN 'pass' ELSE 'fail' END
FROM report_counts

UNION ALL

SELECT
    'current_operational_service_level_exception_count',
    expected_current_exception_rows::TEXT,
    current_exception_rows::TEXT,
    CASE
        WHEN current_exception_rows = expected_current_exception_rows
            THEN 'pass'
        ELSE 'fail'
    END
FROM report_counts

ORDER BY check_name;
