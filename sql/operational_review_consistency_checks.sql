-- Reconciliation for dashboard-ready operational review views.

WITH contract_keys AS (
    SELECT DISTINCT
        policy_id AS operational_policy_id,
        policy_fingerprint AS operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session
    FROM risk_platform.latest_operational_service_level_reports
    UNION
    SELECT DISTINCT
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
    UNION
    SELECT DISTINCT
        operational_policy_id,
        operational_policy_fingerprint,
        schedule_id,
        schedule_fingerprint,
        calendar_id,
        portfolio_id,
        risk_limit_policy_id,
        mandate_fingerprint,
        latest_expected_session
    FROM risk_platform.latest_operational_readiness_decisions
),
counts AS (
    SELECT
        (SELECT COUNT(*) FROM contract_keys) AS expected_health_rows,
        (SELECT COUNT(*) FROM risk_platform.current_operational_health_summary)
            AS health_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_service_level_exceptions
        ) + (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_service_level_objective_exceptions
        ) + (
            SELECT COALESCE(SUM(jsonb_array_length(reasons)), 0)
            FROM risk_platform.current_blocked_operational_readiness_decisions
        ) AS expected_exception_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_exception_summary
        ) AS exception_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decision_history
        ) AS expected_recent_decision_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.recent_operational_readiness_decisions
        ) AS recent_decision_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_metric_history
        ) AS expected_objective_trend_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.rolling_operational_objective_attainment
        ) AS objective_trend_rows,
        (
            SELECT COUNT(*) FROM risk_platform.operational_service_level_reports
        ) + (
            SELECT COUNT(*)
            FROM risk_platform.operational_service_level_objective_reports
        ) + (
            SELECT COUNT(*)
            FROM risk_platform.operational_readiness_decisions
        ) AS expected_drillthrough_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_evidence_drillthrough
        ) AS drillthrough_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    latest_expected_session
                FROM risk_platform.current_operational_health_summary
                GROUP BY
                    operational_policy_id,
                    operational_policy_fingerprint,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    latest_expected_session
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_health_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_operational_health_summary health
            WHERE health.health_status <> CASE
                WHEN health.readiness_decision = 'block' THEN 'blocked'
                WHEN health.service_level_status = 'critical' THEN 'critical'
                WHEN health.objective_overall_status = 'missed' THEN 'missed'
                WHEN health.readiness_decision IS NULL THEN 'readiness_missing'
                WHEN health.service_level_report_calculation_id IS NULL
                    THEN 'service_level_missing'
                WHEN health.objective_policy_count IS NULL
                    THEN 'objective_missing'
                WHEN health.service_level_status = 'warning' THEN 'warning'
                WHEN health.objective_overall_status = 'insufficient'
                    THEN 'insufficient'
                ELSE 'ok'
            END
        ) AS invalid_health_statuses,
        (
            SELECT COUNT(*)
            FROM (
                SELECT exception_type, evidence_id, exception_name
                FROM risk_platform.current_operational_exception_summary
                GROUP BY exception_type, evidence_id, exception_name
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_exception_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.recent_operational_readiness_decisions recent
            WHERE recent.decision_recency_rank = 1
              AND NOT EXISTS (
                    SELECT 1
                    FROM risk_platform.latest_operational_readiness_decisions latest
                    WHERE latest.decision_id = recent.decision_id
              )
        ) + (
            SELECT COUNT(*)
            FROM risk_platform.latest_operational_readiness_decisions latest
            WHERE NOT EXISTS (
                    SELECT 1
                    FROM risk_platform.recent_operational_readiness_decisions recent
                    WHERE recent.decision_id = latest.decision_id
                      AND recent.decision_recency_rank = 1
              )
        ) AS invalid_readiness_ranks,
        (
            SELECT COUNT(*)
            FROM risk_platform.rolling_operational_objective_attainment objective
            WHERE ABS(
                objective.attainment_gap
                - (objective.attainment_ratio - objective.target_ratio)
            ) > 0.000000001
               OR objective.objective_status_rank <> CASE objective.objective_status
                    WHEN 'missed' THEN 2
                    WHEN 'insufficient' THEN 1
                    ELSE 0
                  END
        ) AS invalid_objective_trends,
        (
            SELECT COUNT(*)
            FROM (
                SELECT evidence_type, evidence_id
                FROM risk_platform.operational_evidence_drillthrough
                GROUP BY evidence_type, evidence_id
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_drillthrough_evidence,
        (
            SELECT COUNT(*)
            FROM risk_platform.operational_evidence_drillthrough evidence
            CROSS JOIN LATERAL jsonb_array_elements_text(
                evidence.parent_evidence_ids
            ) parent(parent_id)
            LEFT JOIN risk_platform.operational_service_level_reports source
                ON source.calculation_id = parent.parent_id
            WHERE evidence.evidence_type IN (
                    'service_level_objective_report',
                    'readiness_decision'
                  )
              AND source.calculation_id IS NULL
        ) AS orphan_drillthrough_parents
)
SELECT
    'operational_review_health_rows_match_contracts' AS check_name,
    expected_health_rows::TEXT AS expected,
    health_rows::TEXT AS actual,
    CASE WHEN expected_health_rows = health_rows THEN 'pass' ELSE 'fail' END
        AS status
FROM counts

UNION ALL

SELECT
    'operational_review_health_grain_unique',
    '0',
    duplicate_health_grains::TEXT,
    CASE WHEN duplicate_health_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_health_status_reconciles',
    '0',
    invalid_health_statuses::TEXT,
    CASE WHEN invalid_health_statuses = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_exception_rows_match_sources',
    expected_exception_rows::TEXT,
    exception_rows::TEXT,
    CASE WHEN expected_exception_rows = exception_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'operational_review_exception_identity_unique',
    '0',
    duplicate_exception_rows::TEXT,
    CASE WHEN duplicate_exception_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_recent_decisions_match_history',
    expected_recent_decision_rows::TEXT,
    recent_decision_rows::TEXT,
    CASE
        WHEN expected_recent_decision_rows = recent_decision_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'operational_review_recent_decision_rank_matches_latest',
    '0',
    invalid_readiness_ranks::TEXT,
    CASE WHEN invalid_readiness_ranks = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_objective_trend_rows_match_history',
    expected_objective_trend_rows::TEXT,
    objective_trend_rows::TEXT,
    CASE
        WHEN expected_objective_trend_rows = objective_trend_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'operational_review_objective_trend_arithmetic_reconciles',
    '0',
    invalid_objective_trends::TEXT,
    CASE WHEN invalid_objective_trends = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_drillthrough_rows_match_evidence',
    expected_drillthrough_rows::TEXT,
    drillthrough_rows::TEXT,
    CASE
        WHEN expected_drillthrough_rows = drillthrough_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'operational_review_drillthrough_identity_unique',
    '0',
    duplicate_drillthrough_evidence::TEXT,
    CASE WHEN duplicate_drillthrough_evidence = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'operational_review_drillthrough_parents_exist',
    '0',
    orphan_drillthrough_parents::TEXT,
    CASE WHEN orphan_drillthrough_parents = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
