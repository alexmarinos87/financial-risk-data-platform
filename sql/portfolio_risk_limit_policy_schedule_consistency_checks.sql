-- Reconciliation checks for effective-dated portfolio risk-limit policy evidence.
-- Run after the policy-schedule schema has been applied and evaluation Parquet
-- has been loaded.

WITH observed_periods AS (
    SELECT DISTINCT
        policy_id,
        portfolio_id,
        policy_fingerprint,
        policy_effective_from,
        policy_effective_to,
        policy_period_source
    FROM risk_platform.portfolio_risk_limit_evaluations
),
ordered_periods AS (
    SELECT
        period.*,
        LAG(policy_effective_to) OVER (
            PARTITION BY policy_id, portfolio_id
            ORDER BY policy_effective_from, policy_fingerprint
        ) AS previous_effective_to,
        ROW_NUMBER() OVER (
            PARTITION BY policy_id, portfolio_id
            ORDER BY policy_effective_from, policy_fingerprint
        ) AS period_rank
    FROM observed_periods period
    WHERE policy_period_source = 'configured'
),
expected_snapshot_status AS (
    SELECT
        policy_id,
        policy_fingerprint,
        policy_effective_from,
        policy_effective_to,
        policy_period_source,
        portfolio_id,
        base_currency,
        definition_fingerprint,
        attribution_calculation_id,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        ts_event AS metric_ts,
        MAX(ts_ingest) AS calculation_ts,
        COUNT(*) AS metric_count,
        COUNT(*) FILTER (WHERE is_breach) AS breach_count,
        CASE MAX(
            CASE status WHEN 'critical' THEN 2 WHEN 'warning' THEN 1 ELSE 0 END
        )
            WHEN 2 THEN 'critical'
            WHEN 1 THEN 'warning'
            ELSE 'ok'
        END AS overall_status
    FROM risk_platform.latest_portfolio_risk_limit_policy_evaluations
    GROUP BY
        policy_id,
        policy_fingerprint,
        policy_effective_from,
        policy_effective_to,
        policy_period_source,
        portfolio_id,
        base_currency,
        definition_fingerprint,
        attribution_calculation_id,
        attribution_model_version,
        weighting_method,
        covariance_method,
        correlation_method,
        covariance_window,
        annualization_days,
        ts_event
),
counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
        ) AS latest_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_limit_policy_evaluations
        ) AS latest_policy_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_limit_policy_evaluations
            WHERE is_breach
        ) AS expected_policy_breach_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_policy_breaches
        ) AS policy_breach_rows,
        (SELECT COUNT(*) FROM expected_snapshot_status) AS expected_snapshot_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_policy_snapshot_status
        ) AS snapshot_rows
),
invalid AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_evaluations evaluation
            WHERE
                evaluation.policy_effective_from IS NULL
                OR evaluation.policy_period_source NOT IN (
                    'configured',
                    'legacy_unbounded',
                    'inferred_event_date'
                )
                OR (evaluation.ts_event AT TIME ZONE 'UTC')::DATE
                    < evaluation.policy_effective_from
                OR (
                    evaluation.policy_effective_to IS NOT NULL
                    AND (evaluation.ts_event AT TIME ZONE 'UTC')::DATE
                        >= evaluation.policy_effective_to
                )
                OR (
                    evaluation.policy_effective_to IS NOT NULL
                    AND evaluation.policy_effective_to
                        <= evaluation.policy_effective_from
                )
        ) AS invalid_period_coverage,
        (
            SELECT COUNT(*)
            FROM (
                SELECT policy_fingerprint
                FROM risk_platform.portfolio_risk_limit_evaluations
                WHERE policy_period_source <> 'inferred_event_date'
                GROUP BY policy_fingerprint
                HAVING COUNT(DISTINCT (
                    policy_effective_from,
                    policy_effective_to
                )) > 1
            ) conflicting_periods
        ) AS fingerprints_with_multiple_periods,
        (
            SELECT COUNT(*)
            FROM ordered_periods
            WHERE
                period_rank > 1
                AND (
                    previous_effective_to IS NULL
                    OR previous_effective_to > policy_effective_from
                )
        ) AS overlapping_periods,
        (
            SELECT COUNT(*)
            FROM expected_snapshot_status expected
            FULL OUTER JOIN
                risk_platform.portfolio_risk_limit_policy_snapshot_status actual
                ON expected.policy_id = actual.policy_id
                AND expected.policy_fingerprint = actual.policy_fingerprint
                AND expected.policy_effective_from = actual.policy_effective_from
                AND expected.policy_effective_to
                    IS NOT DISTINCT FROM actual.policy_effective_to
                AND expected.policy_period_source = actual.policy_period_source
                AND expected.portfolio_id = actual.portfolio_id
                AND expected.base_currency = actual.base_currency
                AND expected.definition_fingerprint
                    = actual.definition_fingerprint
                AND expected.attribution_calculation_id
                    = actual.attribution_calculation_id
                AND expected.attribution_model_version
                    = actual.attribution_model_version
                AND expected.weighting_method = actual.weighting_method
                AND expected.covariance_method = actual.covariance_method
                AND expected.correlation_method = actual.correlation_method
                AND expected.covariance_window = actual.covariance_window
                AND expected.annualization_days = actual.annualization_days
                AND expected.metric_ts = actual.metric_ts
            WHERE
                expected.policy_id IS NULL
                OR actual.policy_id IS NULL
                OR expected.calculation_ts <> actual.calculation_ts
                OR expected.metric_count <> actual.metric_count
                OR expected.breach_count <> actual.breach_count
                OR expected.overall_status <> actual.overall_status
        ) AS invalid_snapshot_status_rows
)
SELECT
    'portfolio_risk_limit_policy_periods_cover_evaluations' AS check_name,
    '0' AS expected,
    invalid_period_coverage::TEXT AS actual,
    CASE WHEN invalid_period_coverage = 0 THEN 'pass' ELSE 'fail' END AS status
FROM invalid

UNION ALL

SELECT
    'portfolio_risk_limit_policy_fingerprint_has_one_period',
    '0',
    fingerprints_with_multiple_periods::TEXT,
    CASE WHEN fingerprints_with_multiple_periods = 0 THEN 'pass' ELSE 'fail' END
FROM invalid

UNION ALL

SELECT
    'portfolio_risk_limit_policy_periods_do_not_overlap',
    '0',
    overlapping_periods::TEXT,
    CASE WHEN overlapping_periods = 0 THEN 'pass' ELSE 'fail' END
FROM invalid

UNION ALL

SELECT
    'latest_policy_evaluation_rows_match_latest',
    latest_rows::TEXT,
    latest_policy_rows::TEXT,
    CASE WHEN latest_rows = latest_policy_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'policy_breach_view_matches_latest_breaches',
    expected_policy_breach_rows::TEXT,
    policy_breach_rows::TEXT,
    CASE
        WHEN expected_policy_breach_rows = policy_breach_rows THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'policy_snapshot_status_rows_match_current_snapshots',
    expected_snapshot_rows::TEXT,
    snapshot_rows::TEXT,
    CASE WHEN expected_snapshot_rows = snapshot_rows THEN 'pass' ELSE 'fail' END
FROM counts

UNION ALL

SELECT
    'policy_snapshot_status_values_reconcile',
    '0',
    invalid_snapshot_status_rows::TEXT,
    CASE WHEN invalid_snapshot_status_rows = 0 THEN 'pass' ELSE 'fail' END
FROM invalid

ORDER BY check_name;
