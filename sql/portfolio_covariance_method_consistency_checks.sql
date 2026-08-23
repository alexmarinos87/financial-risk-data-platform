-- Reconciliation checks for the aligned sample-versus-EWMA covariance view.
-- Run after attribution history has been loaded into PostgreSQL.

WITH expected_pairs AS (
    SELECT COUNT(*) AS row_count
    FROM risk_platform.latest_portfolio_risk_attribution sample
    JOIN risk_platform.latest_portfolio_risk_attribution ewma
        ON ewma.portfolio_id = sample.portfolio_id
        AND ewma.base_currency = sample.base_currency
        AND ewma.definition_fingerprint = sample.definition_fingerprint
        AND ewma.weighting_method = sample.weighting_method
        AND ewma.covariance_window = sample.covariance_window
        AND ewma.window_start = sample.window_start
        AND ewma.window_end = sample.window_end
        AND ewma.window_observations = sample.window_observations
        AND ewma.annualization_days = sample.annualization_days
        AND ewma.ts_event = sample.ts_event
        AND ewma.input_calculation_ids_json
            = sample.input_calculation_ids_json
    WHERE
        sample.model_version = 'portfolio-attribution-v1'
        AND sample.covariance_method = 'sample_annualized'
        AND sample.correlation_method = 'pearson'
        AND ewma.model_version = 'portfolio-attribution-ewma-v1'
        AND ewma.covariance_method
            = 'ewma_zero_mean_lambda_0_94_annualized'
        AND ewma.correlation_method = 'implied_from_ewma_covariance'
),
comparison_counts AS (
    SELECT COUNT(*) AS row_count
    FROM risk_platform.portfolio_covariance_method_comparison
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_covariance_method_comparison
            WHERE
                ABS(
                    ewma_minus_sample_volatility
                    - (
                        ewma_portfolio_volatility_annualized
                        - sample_portfolio_volatility_annualized
                    )
                ) > 0.000000000001
        ) AS invalid_differences,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_covariance_method_comparison
            WHERE
                sample_portfolio_volatility_annualized > 0
                AND ABS(
                    ewma_to_sample_volatility_ratio
                    - (
                        ewma_portfolio_volatility_annualized
                        / sample_portfolio_volatility_annualized
                    )
                ) > 0.000000000001
        ) AS invalid_ratios,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_covariance_method_comparison
            WHERE
                (
                    higher_volatility_model = 'ewma'
                    AND ewma_portfolio_volatility_annualized
                        <= sample_portfolio_volatility_annualized
                )
                OR (
                    higher_volatility_model = 'sample'
                    AND ewma_portfolio_volatility_annualized
                        >= sample_portfolio_volatility_annualized
                )
                OR (
                    higher_volatility_model = 'equal'
                    AND ewma_portfolio_volatility_annualized
                        <> sample_portfolio_volatility_annualized
                )
        ) AS invalid_higher_model_labels,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    portfolio_id,
                    definition_fingerprint,
                    metric_ts,
                    weighting_method,
                    covariance_window,
                    annualization_days,
                    COUNT(*) AS row_count
                FROM risk_platform.portfolio_covariance_method_comparison
                GROUP BY
                    portfolio_id,
                    definition_fingerprint,
                    metric_ts,
                    weighting_method,
                    covariance_window,
                    annualization_days
                HAVING COUNT(*) > 1
            ) duplicates
        ) AS duplicate_comparison_grains
)
SELECT
    'portfolio_covariance_method_pairs_complete' AS check_name,
    expected_pairs.row_count::TEXT AS expected,
    comparison_counts.row_count::TEXT AS actual,
    CASE
        WHEN expected_pairs.row_count = comparison_counts.row_count
        THEN 'pass'
        ELSE 'fail'
    END AS status
FROM expected_pairs
CROSS JOIN comparison_counts

UNION ALL

SELECT
    'portfolio_covariance_method_difference_reconciles',
    '0',
    invalid_differences::TEXT,
    CASE WHEN invalid_differences = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_covariance_method_ratio_reconciles',
    '0',
    invalid_ratios::TEXT,
    CASE WHEN invalid_ratios = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_covariance_method_higher_label_reconciles',
    '0',
    invalid_higher_model_labels::TEXT,
    CASE WHEN invalid_higher_model_labels = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_covariance_method_grain_unique',
    '0',
    duplicate_comparison_grains::TEXT,
    CASE WHEN duplicate_comparison_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

ORDER BY check_name;
