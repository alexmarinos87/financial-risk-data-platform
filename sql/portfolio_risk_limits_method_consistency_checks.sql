-- Reconciliation for method-aware portfolio risk-limit evaluations.
-- Run after sql/portfolio_risk_limits_method_schema.sql.

WITH current_v2 AS (
    SELECT *
    FROM risk_platform.latest_portfolio_risk_limit_evaluations
    WHERE model_version = 'portfolio-risk-limits-v2'
),
invalid_contracts AS (
    SELECT COUNT(*) AS invalid_rows
    FROM current_v2
    WHERE
        weighting_method <> 'constant_weight_daily_rebalanced'
        OR NOT (
            (
                attribution_model_version = 'portfolio-attribution-v1'
                AND covariance_method = 'sample_annualized'
                AND correlation_method = 'pearson'
            )
            OR (
                attribution_model_version = 'portfolio-attribution-ewma-v1'
                AND covariance_method =
                    'ewma_zero_mean_lambda_0_94_annualized'
                AND correlation_method =
                    'implied_from_ewma_covariance'
            )
        )
),
exact_pairs AS (
    SELECT
        sample.policy_id,
        sample.portfolio_id,
        sample.base_currency,
        sample.definition_fingerprint,
        sample.ts_event,
        sample.metric_name,
        sample.unit,
        sample.covariance_window,
        sample.annualization_days,
        sample.warning_threshold,
        sample.critical_threshold,
        sample.calculation_id AS sample_calculation_id,
        ewma.calculation_id AS ewma_calculation_id
    FROM current_v2 sample
    JOIN current_v2 ewma
        ON ewma.policy_id = sample.policy_id
        AND ewma.portfolio_id = sample.portfolio_id
        AND ewma.base_currency = sample.base_currency
        AND ewma.definition_fingerprint = sample.definition_fingerprint
        AND ewma.ts_event = sample.ts_event
        AND ewma.metric_name = sample.metric_name
        AND ewma.unit = sample.unit
        AND ewma.covariance_window = sample.covariance_window
        AND ewma.annualization_days = sample.annualization_days
        AND ewma.warning_threshold = sample.warning_threshold
        AND ewma.critical_threshold = sample.critical_threshold
    WHERE
        sample.attribution_model_version = 'portfolio-attribution-v1'
        AND sample.covariance_method = 'sample_annualized'
        AND sample.correlation_method = 'pearson'
        AND ewma.attribution_model_version = 'portfolio-attribution-ewma-v1'
        AND ewma.covariance_method =
            'ewma_zero_mean_lambda_0_94_annualized'
        AND ewma.correlation_method =
            'implied_from_ewma_covariance'
),
comparison_integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM exact_pairs
        ) AS expected_pairs,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison
        ) AS actual_pairs,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison comparison
            WHERE ABS(
                comparison.observed_difference_ewma_minus_sample
                - (
                    comparison.ewma_observed_value
                    - comparison.sample_observed_value
                )
            ) > 0.0000000001
        ) AS invalid_differences,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison comparison
            WHERE ABS(
                comparison.absolute_observed_difference
                - ABS(
                    comparison.ewma_observed_value
                    - comparison.sample_observed_value
                )
            ) > 0.0000000001
        ) AS invalid_absolute_differences,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison comparison
            WHERE comparison.status_disagreement
                <> (comparison.sample_status <> comparison.ewma_status)
        ) AS invalid_disagreement_flags,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    policy_id,
                    portfolio_id,
                    definition_fingerprint,
                    metric_ts,
                    metric_name,
                    covariance_window,
                    annualization_days,
                    warning_threshold,
                    critical_threshold,
                    COUNT(*) AS row_count
                FROM risk_platform.portfolio_risk_limit_method_comparison
                GROUP BY
                    policy_id,
                    portfolio_id,
                    definition_fingerprint,
                    metric_ts,
                    metric_name,
                    covariance_window,
                    annualization_days,
                    warning_threshold,
                    critical_threshold
                HAVING COUNT(*) <> 1
            ) duplicate_grain
        ) AS duplicate_comparison_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison comparison
            WHERE comparison.higher_observed_method <> CASE
                WHEN comparison.sample_observed_value
                    > comparison.ewma_observed_value THEN 'sample'
                WHEN comparison.ewma_observed_value
                    > comparison.sample_observed_value THEN 'ewma'
                ELSE 'equal'
            END
        ) AS invalid_higher_method,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_limit_method_comparison comparison
            WHERE comparison.more_severe_method <> CASE
                WHEN (
                    CASE comparison.sample_status
                        WHEN 'critical' THEN 2
                        WHEN 'warning' THEN 1
                        ELSE 0
                    END
                ) > (
                    CASE comparison.ewma_status
                        WHEN 'critical' THEN 2
                        WHEN 'warning' THEN 1
                        ELSE 0
                    END
                ) THEN 'sample'
                WHEN (
                    CASE comparison.ewma_status
                        WHEN 'critical' THEN 2
                        WHEN 'warning' THEN 1
                        ELSE 0
                    END
                ) > (
                    CASE comparison.sample_status
                        WHEN 'critical' THEN 2
                        WHEN 'warning' THEN 1
                        ELSE 0
                    END
                ) THEN 'ewma'
                ELSE 'equal'
            END
        ) AS invalid_severity_method
)
SELECT
    'method_aware_risk_limit_contracts_supported' AS check_name,
    '0' AS expected,
    invalid_rows::TEXT AS actual,
    CASE WHEN invalid_rows = 0 THEN 'pass' ELSE 'fail' END AS status
FROM invalid_contracts

UNION ALL

SELECT
    'method_aware_risk_limit_exact_pairs_exposed',
    expected_pairs::TEXT,
    actual_pairs::TEXT,
    CASE WHEN expected_pairs = actual_pairs THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_difference_reconciles',
    '0',
    invalid_differences::TEXT,
    CASE WHEN invalid_differences = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_absolute_difference_reconciles',
    '0',
    invalid_absolute_differences::TEXT,
    CASE WHEN invalid_absolute_differences = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_disagreement_flag_reconciles',
    '0',
    invalid_disagreement_flags::TEXT,
    CASE WHEN invalid_disagreement_flags = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_comparison_grain_unique',
    '0',
    duplicate_comparison_grains::TEXT,
    CASE WHEN duplicate_comparison_grains = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_higher_method_reconciles',
    '0',
    invalid_higher_method::TEXT,
    CASE WHEN invalid_higher_method = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

UNION ALL

SELECT
    'method_aware_risk_limit_severity_method_reconciles',
    '0',
    invalid_severity_method::TEXT,
    CASE WHEN invalid_severity_method = 0 THEN 'pass' ELSE 'fail' END
FROM comparison_integrity

ORDER BY check_name;
