-- Reconciliation checks for the Alpha Vantage daily raw-to-curated-to-warehouse path.
-- Run after sql/postgres_schema.sql has been applied and the local Parquet outputs
-- have been loaded with src.warehouse.postgres_loader.

WITH daily_counts AS (
    SELECT
        (SELECT COUNT(*) FROM risk_platform.daily_returns) AS daily_returns,
        (SELECT COUNT(*) FROM risk_platform.daily_volatility) AS daily_volatility,
        (SELECT COUNT(*) FROM risk_platform.daily_risk_summary) AS daily_risk_summary,
        (SELECT COUNT(*) FROM risk_platform.latest_daily_risk_summary) AS latest_daily_risk,
        (SELECT COUNT(*) FROM risk_platform.daily_risk_semantic_model) AS semantic_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_returns returns
            LEFT JOIN risk_platform.market_events_raw raw
                ON raw.event_id = returns.source_event_id
                AND raw.source = returns.source
                AND raw.symbol = returns.symbol
                AND raw.ts_event = returns.ts_event
            WHERE raw.event_id IS NULL
        ) AS orphan_returns,
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_volatility volatility
            LEFT JOIN risk_platform.market_events_raw raw
                ON raw.event_id = volatility.source_event_id
                AND raw.source = volatility.source
                AND raw.symbol = volatility.symbol
                AND raw.ts_event = volatility.ts_event
            LEFT JOIN risk_platform.daily_returns returns
                ON returns.source = volatility.source
                AND returns.symbol = volatility.symbol
                AND returns.ts_event = volatility.ts_event
                AND returns.model_version = volatility.model_version
            WHERE raw.event_id IS NULL OR returns.calculation_id IS NULL
        ) AS orphan_volatility,
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_risk_summary summary
            LEFT JOIN risk_platform.market_events_raw raw
                ON raw.event_id = summary.source_event_id
                AND raw.source = summary.source
                AND raw.symbol = summary.symbol
                AND raw.ts_event = summary.ts_event
            WHERE raw.event_id IS NULL
        ) AS orphan_summaries,
        (
            SELECT
                (COUNT(*) - COUNT(DISTINCT calculation_id))
            FROM risk_platform.daily_returns
        ) + (
            SELECT
                (COUNT(*) - COUNT(DISTINCT calculation_id))
            FROM risk_platform.daily_volatility
        ) + (
            SELECT
                (COUNT(*) - COUNT(DISTINCT calculation_id))
            FROM risk_platform.daily_risk_summary
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    source,
                    symbol,
                    ts_event,
                    model_version,
                    volatility_window,
                    var_window,
                    var_confidence,
                    annualization_days
                FROM risk_platform.latest_daily_risk_summary
                GROUP BY
                    source,
                    symbol,
                    ts_event,
                    model_version,
                    volatility_window,
                    var_window,
                    var_confidence,
                    annualization_days
                HAVING COUNT(*) > 1
            ) duplicate_grains
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_daily_risk_summary latest
            JOIN risk_platform.daily_risk_summary candidate
                ON candidate.source = latest.source
                AND candidate.symbol = latest.symbol
                AND candidate.ts_event = latest.ts_event
                AND candidate.model_version = latest.model_version
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
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_risk_summary
            WHERE history_status = 'ready'
                AND (
                    return_observations < GREATEST(volatility_window, var_window)
                    OR var_observations < var_window
                    OR volatility_annualized IS NULL
                )
        ) AS invalid_ready_rows
)
SELECT
    'daily_summary_rows_present' AS check_name,
    'at least 1' AS expected,
    daily_risk_summary::text AS actual,
    CASE WHEN daily_risk_summary > 0 THEN 'pass' ELSE 'fail' END AS status
FROM daily_counts

UNION ALL

SELECT
    'daily_returns_reference_raw_events',
    '0',
    orphan_returns::text,
    CASE WHEN orphan_returns = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_volatility_references_raw_and_return_date',
    '0',
    orphan_volatility::text,
    CASE WHEN orphan_volatility = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_summaries_reference_raw_events',
    '0',
    orphan_summaries::text,
    CASE WHEN orphan_summaries = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::text,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_daily_summary_parameter_grain_unique',
    '0',
    duplicate_latest_grains::text,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_daily_summary_selects_current_version',
    '0',
    stale_latest_rows::text,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'ready_daily_history_has_required_observations',
    '0',
    invalid_ready_rows::text,
    CASE WHEN invalid_ready_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_semantic_rows_match_latest_summary',
    latest_daily_risk::text,
    semantic_rows::text,
    CASE WHEN latest_daily_risk = semantic_rows THEN 'pass' ELSE 'fail' END
FROM daily_counts

ORDER BY check_name;
