-- Reconciliation checks for versioned portfolio daily risk serving.
-- Run after sql/postgres_schema.sql and sql/portfolio_schema.sql have been applied
-- and portfolio Parquet outputs have been loaded with src.warehouse.postgres_loader.

WITH portfolio_counts AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_returns
        ) AS portfolio_returns,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_risk_summary
        ) AS portfolio_summaries,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_daily_returns
        ) AS latest_returns,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_daily_risk_summary
        ) AS latest_summaries,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_risk_semantic_model
        ) AS semantic_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_contribution_model
        ) AS contribution_rows,
        (
            SELECT COALESCE(SUM(constituent_count), 0)
            FROM risk_platform.latest_portfolio_daily_returns
        ) AS expected_contribution_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_returns portfolio_return
            CROSS JOIN LATERAL jsonb_each_text(
                portfolio_return.component_calculation_ids_json
            ) component
            LEFT JOIN risk_platform.daily_returns daily_return
                ON daily_return.calculation_id = component.value
            WHERE daily_return.calculation_id IS NULL
        ) AS orphan_component_returns,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_returns portfolio_return
            WHERE (
                SELECT COUNT(*)
                FROM jsonb_object_keys(portfolio_return.weights_json)
            ) <> portfolio_return.constituent_count
            OR (
                SELECT COUNT(*)
                FROM jsonb_object_keys(
                    portfolio_return.component_calculation_ids_json
                )
            ) <> portfolio_return.constituent_count
            OR (
                SELECT COUNT(*)
                FROM jsonb_object_keys(portfolio_return.component_returns_json)
            ) <> portfolio_return.constituent_count
            OR (
                SELECT COUNT(*)
                FROM jsonb_object_keys(portfolio_return.contributions_json)
            ) <> portfolio_return.constituent_count
            OR EXISTS (
                SELECT 1
                FROM jsonb_object_keys(
                    portfolio_return.weights_json
                ) AS weight_key(key)
                WHERE NOT (
                    portfolio_return.component_calculation_ids_json
                    ? weight_key.key
                )
                OR NOT (
                    portfolio_return.component_returns_json
                    ? weight_key.key
                )
                OR NOT (
                    portfolio_return.contributions_json
                    ? weight_key.key
                )
            )
        ) AS invalid_component_key_sets,
        (
            SELECT COUNT(*)
            FROM (
                SELECT portfolio_return.calculation_id
                FROM risk_platform.portfolio_daily_returns portfolio_return
                CROSS JOIN LATERAL jsonb_each_text(
                    portfolio_return.weights_json
                ) weight
                GROUP BY portfolio_return.calculation_id
                HAVING ABS(
                    SUM(weight.value::DOUBLE PRECISION) - 1.0
                ) > 1e-9
            ) invalid_weights
        ) AS invalid_weight_sums,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    portfolio_return.calculation_id,
                    portfolio_return.portfolio_return_1d
                FROM risk_platform.portfolio_daily_returns portfolio_return
                CROSS JOIN LATERAL jsonb_each_text(
                    portfolio_return.contributions_json
                ) contribution
                GROUP BY
                    portfolio_return.calculation_id,
                    portfolio_return.portfolio_return_1d
                HAVING ABS(
                    SUM(contribution.value::DOUBLE PRECISION)
                    - portfolio_return.portfolio_return_1d
                ) > 1e-12
            ) invalid_contributions
        ) AS invalid_contribution_sums,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_returns portfolio_return
            CROSS JOIN LATERAL jsonb_each_text(
                portfolio_return.contributions_json
            ) contribution
            WHERE ABS(
                contribution.value::DOUBLE PRECISION
                - (
                    portfolio_return.weights_json
                    ->> contribution.key
                )::DOUBLE PRECISION
                * (
                    portfolio_return.component_returns_json
                    ->> contribution.key
                )::DOUBLE PRECISION
            ) > 1e-12
        ) AS invalid_contribution_math,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_risk_summary summary
            LEFT JOIN risk_platform.portfolio_daily_returns portfolio_return
                ON portfolio_return.calculation_id
                    = summary.portfolio_return_calculation_id
                AND portfolio_return.portfolio_id = summary.portfolio_id
                AND portfolio_return.definition_fingerprint
                    = summary.definition_fingerprint
                AND portfolio_return.model_version = summary.model_version
                AND portfolio_return.weighting_method
                    = summary.weighting_method
                AND portfolio_return.ts_event = summary.ts_event
            WHERE portfolio_return.calculation_id IS NULL
        ) AS orphan_summaries,
        (
            SELECT
                COUNT(*) - COUNT(DISTINCT calculation_id)
            FROM risk_platform.portfolio_daily_returns
        ) + (
            SELECT
                COUNT(*) - COUNT(DISTINCT calculation_id)
            FROM risk_platform.portfolio_daily_risk_summary
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method
                FROM risk_platform.latest_portfolio_daily_returns
                GROUP BY
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_return_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_daily_returns latest
            JOIN risk_platform.portfolio_daily_returns candidate
                ON candidate.portfolio_id = latest.portfolio_id
                AND candidate.definition_fingerprint
                    = latest.definition_fingerprint
                AND candidate.ts_event = latest.ts_event
                AND candidate.model_version = latest.model_version
                AND candidate.weighting_method = latest.weighting_method
                AND (
                    candidate.ts_ingest > latest.ts_ingest
                    OR (
                        candidate.ts_ingest = latest.ts_ingest
                        AND candidate.calculation_id > latest.calculation_id
                    )
                )
        ) AS stale_latest_return_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method,
                    volatility_window,
                    var_window,
                    var_confidence,
                    annualization_days
                FROM risk_platform.latest_portfolio_daily_risk_summary
                GROUP BY
                    portfolio_id,
                    definition_fingerprint,
                    ts_event,
                    model_version,
                    weighting_method,
                    volatility_window,
                    var_window,
                    var_confidence,
                    annualization_days
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_summary_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_portfolio_daily_risk_summary latest
            JOIN risk_platform.portfolio_daily_risk_summary candidate
                ON candidate.portfolio_id = latest.portfolio_id
                AND candidate.definition_fingerprint
                    = latest.definition_fingerprint
                AND candidate.ts_event = latest.ts_event
                AND candidate.model_version = latest.model_version
                AND candidate.weighting_method = latest.weighting_method
                AND candidate.volatility_window = latest.volatility_window
                AND candidate.var_window = latest.var_window
                AND candidate.var_confidence = latest.var_confidence
                AND candidate.annualization_days = latest.annualization_days
                AND (
                    candidate.ts_ingest > latest.ts_ingest
                    OR (
                        candidate.ts_ingest = latest.ts_ingest
                        AND candidate.calculation_id > latest.calculation_id
                    )
                )
        ) AS stale_latest_summary_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.portfolio_daily_risk_summary
            WHERE history_status = 'ready'
                AND (
                    aligned_observations
                        < GREATEST(volatility_window, var_window)
                    OR var_observations < var_window
                    OR volatility_annualized IS NULL
                )
        ) AS invalid_ready_rows
)
SELECT
    'portfolio_summary_rows_present' AS check_name,
    'at least 1' AS expected,
    portfolio_summaries::TEXT AS actual,
    CASE WHEN portfolio_summaries > 0 THEN 'pass' ELSE 'fail' END AS status
FROM portfolio_counts

UNION ALL

SELECT
    'portfolio_components_reference_daily_returns',
    '0',
    orphan_component_returns::TEXT,
    CASE WHEN orphan_component_returns = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_component_json_keys_align',
    '0',
    invalid_component_key_sets::TEXT,
    CASE WHEN invalid_component_key_sets = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_weights_sum_to_one',
    '0',
    invalid_weight_sums::TEXT,
    CASE WHEN invalid_weight_sums = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_contributions_sum_to_return',
    '0',
    invalid_contribution_sums::TEXT,
    CASE WHEN invalid_contribution_sums = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_contributions_match_weighted_components',
    '0',
    invalid_contribution_math::TEXT,
    CASE WHEN invalid_contribution_math = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_summaries_reference_portfolio_returns',
    '0',
    orphan_summaries::TEXT,
    CASE WHEN orphan_summaries = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::TEXT,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_return_grain_unique',
    '0',
    duplicate_latest_return_grains::TEXT,
    CASE WHEN duplicate_latest_return_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_return_selects_current_version',
    '0',
    stale_latest_return_rows::TEXT,
    CASE WHEN stale_latest_return_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_summary_parameter_grain_unique',
    '0',
    duplicate_latest_summary_grains::TEXT,
    CASE WHEN duplicate_latest_summary_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_portfolio_summary_selects_current_version',
    '0',
    stale_latest_summary_rows::TEXT,
    CASE WHEN stale_latest_summary_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'ready_portfolio_history_has_required_observations',
    '0',
    invalid_ready_rows::TEXT,
    CASE WHEN invalid_ready_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'portfolio_semantic_rows_match_latest_summary',
    latest_summaries::TEXT,
    semantic_rows::TEXT,
    CASE WHEN latest_summaries = semantic_rows THEN 'pass' ELSE 'fail' END
FROM portfolio_counts

UNION ALL

SELECT
    'portfolio_contribution_rows_match_constituent_counts',
    expected_contribution_rows::TEXT,
    contribution_rows::TEXT,
    CASE
        WHEN expected_contribution_rows = contribution_rows THEN 'pass'
        ELSE 'fail'
    END
FROM portfolio_counts

ORDER BY check_name;
