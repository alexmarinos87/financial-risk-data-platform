-- Reconciliation checks for portfolio covariance and volatility-attribution serving.
-- Run after the core, portfolio and attribution schemas are applied and local
-- Parquet outputs have been loaded into PostgreSQL.

WITH attribution_counts AS (
    SELECT
        (SELECT COUNT(*) FROM risk_platform.portfolio_risk_attribution)
            AS attribution_rows,
        (SELECT COUNT(*) FROM risk_platform.latest_portfolio_risk_attribution)
            AS latest_rows,
        (SELECT COUNT(*) FROM risk_platform.portfolio_attribution_semantic_model)
            AS semantic_rows,
        (SELECT COUNT(*) FROM risk_platform.portfolio_covariance_model)
            AS covariance_rows,
        (SELECT COUNT(*) FROM risk_platform.portfolio_correlation_model)
            AS correlation_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_volatility_contribution_model
        ) AS contribution_rows,
        (
            SELECT COALESCE(SUM(constituent_count), 0)
            FROM risk_platform.latest_portfolio_risk_attribution
        ) AS expected_contribution_rows,
        (
            SELECT COALESCE(SUM(constituent_count * constituent_count), 0)
            FROM risk_platform.latest_portfolio_risk_attribution
        ) AS expected_matrix_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            CROSS JOIN LATERAL jsonb_array_elements_text(
                attribution.input_calculation_ids_json
            ) input_id
            LEFT JOIN risk_platform.portfolio_daily_returns portfolio_return
                ON portfolio_return.calculation_id = input_id.value
            WHERE portfolio_return.calculation_id IS NULL
        ) AS orphan_input_calculation_ids,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_attribution attribution
            CROSS JOIN LATERAL jsonb_array_elements_text(
                attribution.input_calculation_ids_json
            ) input_id
            LEFT JOIN risk_platform.latest_portfolio_daily_returns portfolio_return
                ON portfolio_return.calculation_id = input_id.value
            WHERE portfolio_return.calculation_id IS NULL
        ) AS stale_latest_input_calculation_ids,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            WHERE
                attribution.input_first_calculation_id
                    <> attribution.input_calculation_ids_json ->> 0
                OR attribution.input_last_calculation_id
                    <> attribution.input_calculation_ids_json ->> (
                        jsonb_array_length(
                            attribution.input_calculation_ids_json
                        ) - 1
                    )
                OR (
                    SELECT MIN(portfolio_return.ts_event)
                    FROM jsonb_array_elements_text(
                        attribution.input_calculation_ids_json
                    ) input_id
                    JOIN risk_platform.portfolio_daily_returns portfolio_return
                        ON portfolio_return.calculation_id = input_id.value
                ) <> attribution.window_start
                OR (
                    SELECT MAX(portfolio_return.ts_event)
                    FROM jsonb_array_elements_text(
                        attribution.input_calculation_ids_json
                    ) input_id
                    JOIN risk_platform.portfolio_daily_returns portfolio_return
                        ON portfolio_return.calculation_id = input_id.value
                ) <> attribution.window_end
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements_text(
                        attribution.input_calculation_ids_json
                    ) input_id
                    JOIN risk_platform.portfolio_daily_returns portfolio_return
                        ON portfolio_return.calculation_id = input_id.value
                    WHERE
                        portfolio_return.portfolio_id
                            <> attribution.portfolio_id
                        OR portfolio_return.definition_fingerprint
                            <> attribution.definition_fingerprint
                        OR portfolio_return.weighting_method
                            <> attribution.weighting_method
                        OR portfolio_return.base_currency
                            <> attribution.base_currency
                )
        ) AS invalid_input_windows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            WHERE
                (
                    SELECT COUNT(*)
                    FROM jsonb_object_keys(attribution.weights_json)
                ) <> attribution.constituent_count
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_object_keys(
                        attribution.constituent_volatility_annualized_json
                    )
                ) <> attribution.constituent_count
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_object_keys(
                        attribution.marginal_volatility_contribution_json
                    )
                ) <> attribution.constituent_count
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_object_keys(
                        attribution.component_volatility_contribution_json
                    )
                ) <> attribution.constituent_count
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_object_keys(
                        attribution.component_contribution_share_json
                    )
                ) <> attribution.constituent_count
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(attribution.weights_json)
                        AS weight_keys(key)
                    WHERE NOT (
                        attribution.constituent_volatility_annualized_json
                        ? weight_keys.key
                    )
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(attribution.weights_json)
                        AS weight_keys(key)
                    WHERE NOT (
                        attribution.marginal_volatility_contribution_json
                        ? weight_keys.key
                    )
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(attribution.weights_json)
                        AS weight_keys(key)
                    WHERE NOT (
                        attribution.component_volatility_contribution_json
                        ? weight_keys.key
                    )
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(attribution.weights_json)
                        AS weight_keys(key)
                    WHERE NOT (
                        attribution.component_contribution_share_json
                        ? weight_keys.key
                    )
                )
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_each(
                        attribution.covariance_annualized_json
                    ) matrix_row
                    CROSS JOIN LATERAL jsonb_each(matrix_row.value) matrix_cell
                ) <> attribution.constituent_count * attribution.constituent_count
                OR (
                    SELECT COUNT(*)
                    FROM jsonb_each(attribution.correlation_json) matrix_row
                    CROSS JOIN LATERAL jsonb_each(matrix_row.value) matrix_cell
                ) <> attribution.constituent_count * attribution.constituent_count
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_each(
                        attribution.covariance_annualized_json
                    ) matrix_row
                    WHERE jsonb_typeof(matrix_row.value) <> 'object'
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_each(attribution.correlation_json) matrix_row
                    WHERE jsonb_typeof(matrix_row.value) <> 'object'
                )
        ) AS invalid_json_shapes,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            WHERE ABS(
                (
                    SELECT SUM(weight.value::TEXT::DOUBLE PRECISION)
                    FROM jsonb_each(attribution.weights_json) weight
                ) - 1.0
            ) > 0.000000001
        ) AS invalid_weight_totals,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_covariance_model covariance
            LEFT JOIN risk_platform.portfolio_covariance_model reverse_covariance
                ON reverse_covariance.attribution_calculation_id
                    = covariance.attribution_calculation_id
                AND reverse_covariance.row_constituent_key
                    = covariance.column_constituent_key
                AND reverse_covariance.column_constituent_key
                    = covariance.row_constituent_key
            WHERE
                reverse_covariance.attribution_calculation_id IS NULL
                OR ABS(
                    covariance.covariance_annualized
                    - reverse_covariance.covariance_annualized
                ) > 0.0000000001
                OR (
                    covariance.row_constituent_key
                        = covariance.column_constituent_key
                    AND covariance.covariance_annualized < -0.000000000001
                )
        ) AS invalid_covariance_cells,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_correlation_model correlation
            LEFT JOIN risk_platform.portfolio_correlation_model reverse_correlation
                ON reverse_correlation.attribution_calculation_id
                    = correlation.attribution_calculation_id
                AND reverse_correlation.row_constituent_key
                    = correlation.column_constituent_key
                AND reverse_correlation.column_constituent_key
                    = correlation.row_constituent_key
            WHERE
                reverse_correlation.attribution_calculation_id IS NULL
                OR (
                    (correlation.correlation IS NULL)
                    <> (reverse_correlation.correlation IS NULL)
                )
                OR (
                    correlation.correlation IS NOT NULL
                    AND reverse_correlation.correlation IS NOT NULL
                    AND ABS(
                        correlation.correlation
                        - reverse_correlation.correlation
                    ) > 0.0000000001
                )
                OR (
                    correlation.correlation IS NOT NULL
                    AND (
                        correlation.correlation < -1.0000000001
                        OR correlation.correlation > 1.0000000001
                    )
                )
                OR (
                    correlation.row_constituent_key
                        = correlation.column_constituent_key
                    AND correlation.correlation IS NOT NULL
                    AND ABS(correlation.correlation - 1.0)
                        > 0.0000000001
                )
        ) AS invalid_correlation_cells,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            WHERE
                (
                    SELECT COUNT(*)
                    FROM risk_platform.portfolio_correlation_model correlation
                    WHERE
                        correlation.attribution_calculation_id
                            = attribution.calculation_id
                        AND correlation.correlation_undefined
                ) <> attribution.undefined_correlation_cells
        ) AS invalid_undefined_correlation_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_attribution attribution
            WHERE ABS(
                attribution.portfolio_variance_annualized
                - POWER(attribution.portfolio_volatility_annualized, 2)
            ) > 0.0000000001
        ) AS invalid_variance_volatility_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_attribution attribution
            LEFT JOIN LATERAL (
                SELECT
                    SUM(contribution.component_volatility_contribution)
                        AS component_total,
                    SUM(contribution.contribution_share) AS share_total,
                    SUM(contribution.weight) AS weight_total,
                    COUNT(*) AS component_count
                FROM risk_platform.portfolio_volatility_contribution_model
                    contribution
                WHERE
                    contribution.attribution_calculation_id
                        = attribution.calculation_id
            ) totals ON true
            WHERE
                totals.component_count <> attribution.constituent_count
                OR ABS(
                    totals.component_total
                    - attribution.portfolio_volatility_annualized
                ) > 0.000000001
                OR ABS(totals.weight_total - 1.0) > 0.000000001
                OR (
                    attribution.volatility_status = 'positive'
                    AND ABS(totals.share_total - 1.0) > 0.000000001
                )
                OR (
                    attribution.volatility_status = 'zero'
                    AND ABS(totals.share_total) > 0.000000001
                )
                OR ABS(attribution.euler_residual) > 0.00000001
        ) AS invalid_euler_rows,
        (
            SELECT COUNT(*) - COUNT(DISTINCT calculation_id)
            FROM risk_platform.portfolio_risk_attribution
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method,
                    covariance_method,
                    correlation_method,
                    covariance_window,
                    annualization_days
                FROM risk_platform.latest_portfolio_risk_attribution
                GROUP BY
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method,
                    covariance_method,
                    correlation_method,
                    covariance_window,
                    annualization_days
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_risk_attribution latest
            JOIN risk_platform.portfolio_risk_attribution candidate
                ON candidate.portfolio_id = latest.portfolio_id
                AND candidate.definition_fingerprint
                    = latest.definition_fingerprint
                AND candidate.ts_event = latest.ts_event
                AND candidate.model_version = latest.model_version
                AND candidate.weighting_method = latest.weighting_method
                AND candidate.covariance_method = latest.covariance_method
                AND candidate.correlation_method = latest.correlation_method
                AND candidate.covariance_window = latest.covariance_window
                AND candidate.annualization_days = latest.annualization_days
                AND (
                    candidate.ts_ingest > latest.ts_ingest
                    OR (
                        candidate.ts_ingest = latest.ts_ingest
                        AND candidate.calculation_id > latest.calculation_id
                    )
                )
        ) AS stale_latest_rows
)
SELECT
    'portfolio_attribution_rows_present' AS check_name,
    'at least 1' AS expected,
    attribution_rows::TEXT AS actual,
    CASE WHEN attribution_rows > 0 THEN 'pass' ELSE 'fail' END AS status
FROM attribution_counts

UNION ALL

SELECT
    'portfolio_attribution_inputs_reference_portfolio_returns',
    '0',
    orphan_input_calculation_ids::TEXT,
    CASE WHEN orphan_input_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_attribution_uses_current_returns',
    '0',
    stale_latest_input_calculation_ids::TEXT,
    CASE WHEN stale_latest_input_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_attribution_window_evidence_aligns',
    '0',
    invalid_input_windows::TEXT,
    CASE WHEN invalid_input_windows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_attribution_json_shapes_align',
    '0',
    invalid_json_shapes::TEXT,
    CASE WHEN invalid_json_shapes = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_attribution_weights_sum_to_one',
    '0',
    invalid_weight_totals::TEXT,
    CASE WHEN invalid_weight_totals = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_covariance_is_symmetric_and_nonnegative_on_diagonal',
    '0',
    invalid_covariance_cells::TEXT,
    CASE WHEN invalid_covariance_cells = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_correlation_is_symmetric_and_bounded',
    '0',
    invalid_correlation_cells::TEXT,
    CASE WHEN invalid_correlation_cells = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_correlation_null_count_matches_status',
    '0',
    invalid_undefined_correlation_counts::TEXT,
    CASE
        WHEN invalid_undefined_correlation_counts = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'portfolio_variance_matches_squared_volatility',
    '0',
    invalid_variance_volatility_rows::TEXT,
    CASE
        WHEN invalid_variance_volatility_rows = 0 THEN 'pass'
        ELSE 'fail'
    END
FROM integrity

UNION ALL

SELECT
    'portfolio_volatility_contributions_reconcile',
    '0',
    invalid_euler_rows::TEXT,
    CASE WHEN invalid_euler_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_attribution_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::TEXT,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_attribution_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_attribution_selects_current_version',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_attribution_semantic_rows_match_latest',
    latest_rows::TEXT,
    semantic_rows::TEXT,
    CASE WHEN latest_rows = semantic_rows THEN 'pass' ELSE 'fail' END
FROM attribution_counts

UNION ALL

SELECT
    'portfolio_covariance_rows_match_matrix_grain',
    expected_matrix_rows::TEXT,
    covariance_rows::TEXT,
    CASE WHEN expected_matrix_rows = covariance_rows THEN 'pass' ELSE 'fail' END
FROM attribution_counts

UNION ALL

SELECT
    'portfolio_correlation_rows_match_matrix_grain',
    expected_matrix_rows::TEXT,
    correlation_rows::TEXT,
    CASE WHEN expected_matrix_rows = correlation_rows THEN 'pass' ELSE 'fail' END
FROM attribution_counts

UNION ALL

SELECT
    'portfolio_attribution_contribution_rows_match_constituents',
    expected_contribution_rows::TEXT,
    contribution_rows::TEXT,
    CASE
        WHEN expected_contribution_rows = contribution_rows THEN 'pass'
        ELSE 'fail'
    END
FROM attribution_counts

ORDER BY check_name;
