-- Reconciliation checks for rolling operational SLO objective history and views.
-- Run after operational service-level source reports and objective reports exist.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.operational_service_level_objective_reports)
            AS report_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_operational_service_level_objective_reports)
            AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.operational_service_level_objective_metric_history)
            AS history_metric_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_objective_status)
            AS current_metric_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_objective_exceptions)
            AS current_exception_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_operational_service_level_objective_status
         WHERE objective_status IN ('missed', 'insufficient'))
            AS expected_exception_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            CROSS JOIN LATERAL jsonb_array_elements_text(
                report.input_report_calculation_ids
            ) WITH ORDINALITY input_id(value, ordinality)
            JOIN LATERAL jsonb_array_elements_text(
                report.input_report_document_sha256
            ) WITH ORDINALITY input_digest(value, ordinality)
                USING (ordinality)
            LEFT JOIN risk_platform.operational_service_level_reports source_report
                ON source_report.calculation_id = input_id.value
            WHERE
                source_report.calculation_id IS NULL
                OR source_report.document_sha256 <> input_digest.value
        ) AS invalid_source_report_references,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            CROSS JOIN LATERAL jsonb_array_elements_text(
                report.input_report_calculation_ids
            ) input_id
            JOIN risk_platform.operational_service_level_reports source_report
                ON source_report.calculation_id = input_id.value
            WHERE
                source_report.policy_id <> report.operational_policy_id
                OR source_report.policy_fingerprint
                    <> report.operational_policy_fingerprint
                OR source_report.schedule_id <> report.schedule_id
                OR source_report.schedule_fingerprint
                    <> report.schedule_fingerprint
                OR source_report.calendar_id <> report.calendar_id
                OR source_report.portfolio_id <> report.portfolio_id
                OR source_report.risk_limit_policy_id
                    <> report.risk_limit_policy_id
                OR source_report.mandate_fingerprint
                    <> report.mandate_fingerprint
                OR source_report.latest_expected_session
                    < report.window_start_session
                OR source_report.latest_expected_session
                    > report.window_end_session
        ) AS invalid_source_report_contracts,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            WHERE
                jsonb_array_length(report.objectives_json) <> 4
                OR (
                    SELECT COUNT(DISTINCT objective.value ->> 'objective_name')
                    FROM jsonb_array_elements(report.objectives_json) objective
                ) <> 4
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(report.objectives_json) objective
                    WHERE
                        (objective.value ->> 'observations_available')::INTEGER
                            <> report.observations_available
                        OR
                        (objective.value ->> 'observations_expected')::INTEGER
                            <> report.observations_expected
                        OR
                        (objective.value ->> 'missing_report_observations')::INTEGER
                            <> jsonb_array_length(report.missing_report_sessions)
                        OR
                        (objective.value ->> 'successful_observations')::INTEGER
                            +
                            (objective.value ->> 'failed_observations')::INTEGER
                            <> report.observations_available
                        OR ABS(
                            (objective.value ->> 'attainment_ratio')::DOUBLE PRECISION
                            - (
                                (objective.value ->> 'successful_observations')::DOUBLE PRECISION
                                / report.observations_expected
                            )
                        ) > 0.000000000001
                )
        ) AS invalid_objective_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            WHERE EXISTS (
                SELECT 1
                FROM jsonb_array_elements(report.objectives_json) objective
                WHERE
                    CASE objective.value ->> 'objective_name'
                        WHEN 'schedule_completion_attainment'
                            THEN objective.value ->> 'source_metric_name'
                                <> 'schedule_lag_sessions'
                                OR objective.value ->> 'source_unit'
                                    <> 'sessions'
                        WHEN 'market_freshness_attainment'
                            THEN objective.value ->> 'source_metric_name'
                                <> 'market_freshness_exception_count'
                                OR objective.value ->> 'source_unit'
                                    <> 'constituents'
                        WHEN 'notification_retry_exhaustion_free_attainment'
                            THEN objective.value ->> 'source_metric_name'
                                <> 'notification_retry_exhausted_count'
                                OR objective.value ->> 'source_unit'
                                    <> 'events'
                        WHEN 'notification_dead_letter_duration_attainment'
                            THEN objective.value ->> 'source_metric_name'
                                <> 'notification_oldest_dead_letter_age_seconds'
                                OR objective.value ->> 'source_unit'
                                    <> 'seconds'
                        ELSE TRUE
                    END
            )
        ) AS invalid_objective_sources,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            WHERE EXISTS (
                SELECT 1
                FROM jsonb_array_elements(report.objectives_json) objective
                WHERE
                    objective.value ->> 'status'
                    <>
                    CASE
                        WHEN report.history_status = 'insufficient'
                            THEN 'insufficient'
                        WHEN
                            (objective.value ->> 'attainment_ratio')::DOUBLE PRECISION
                            >=
                            (objective.value ->> 'target_ratio')::DOUBLE PRECISION
                            THEN 'met'
                        ELSE 'missed'
                    END
            )
        ) AS invalid_objective_statuses,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports report
            WHERE report.overall_status <>
                CASE
                    WHEN report.history_status = 'insufficient'
                        THEN 'insufficient'
                    WHEN EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(report.objectives_json) objective
                        WHERE objective.value ->> 'status' = 'missed'
                    ) THEN 'missed'
                    ELSE 'met'
                END
        ) AS invalid_overall_statuses,
        (
            SELECT COUNT(*)
            FROM (
                SELECT calculation_id
                FROM risk_platform.operational_service_level_objective_reports
                GROUP BY calculation_id
                HAVING COUNT(*) > 1
            ) duplicate_ids
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    model_version,
                    objective_policy_id,
                    objective_policy_fingerprint,
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    through_session
                FROM risk_platform.latest_operational_service_level_objective_reports
                GROUP BY
                    model_version,
                    objective_policy_id,
                    objective_policy_fingerprint,
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    through_session
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_operational_service_level_objective_reports latest
            JOIN risk_platform.operational_service_level_objective_reports candidate
                ON candidate.model_version = latest.model_version
                AND candidate.objective_policy_id = latest.objective_policy_id
                AND candidate.objective_policy_fingerprint
                    = latest.objective_policy_fingerprint
                AND candidate.operational_policy_id
                    = latest.operational_policy_id
                AND candidate.operational_policy_fingerprint
                    = latest.operational_policy_fingerprint
                AND candidate.schedule_id = latest.schedule_id
                AND candidate.schedule_fingerprint = latest.schedule_fingerprint
                AND candidate.calendar_id = latest.calendar_id
                AND candidate.portfolio_id = latest.portfolio_id
                AND candidate.risk_limit_policy_id
                    = latest.risk_limit_policy_id
                AND candidate.mandate_fingerprint = latest.mandate_fingerprint
                AND candidate.through_session = latest.through_session
            WHERE
                (candidate.calculated_at, candidate.calculation_id)
                    > (latest.calculated_at, latest.calculation_id)
        ) AS stale_latest_rows
)
SELECT
    'operational_slo_objective_source_report_references_valid' AS check_name,
    '0' AS expected,
    invalid_source_report_references::TEXT AS actual,
    CASE
        WHEN invalid_source_report_references = 0 THEN 'pass'
        ELSE 'fail'
    END AS status
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_source_contracts_align',
    '0',
    invalid_source_report_contracts::TEXT,
    CASE WHEN invalid_source_report_contracts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_counts_reconcile',
    '0',
    invalid_objective_counts::TEXT,
    CASE WHEN invalid_objective_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_sources_align',
    '0',
    invalid_objective_sources::TEXT,
    CASE WHEN invalid_objective_sources = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_statuses_reconcile',
    '0',
    invalid_objective_statuses::TEXT,
    CASE WHEN invalid_objective_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_overall_status_reconciles',
    '0',
    invalid_overall_statuses::TEXT,
    CASE WHEN invalid_overall_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::TEXT,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_slo_objective_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_operational_slo_objective_selects_current_version',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_slo_objective_history_rows_match_reports',
    (report_rows * 4)::TEXT,
    history_metric_rows::TEXT,
    CASE WHEN history_metric_rows = report_rows * 4 THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'current_operational_slo_objective_rows_match_latest',
    (latest_rows * 4)::TEXT,
    current_metric_rows::TEXT,
    CASE WHEN current_metric_rows = latest_rows * 4 THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'current_operational_slo_objective_exceptions_match_status',
    expected_exception_rows::TEXT,
    current_exception_rows::TEXT,
    CASE
        WHEN current_exception_rows = expected_exception_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

ORDER BY check_name;
