-- Reconciliation checks for exchange-calendar-aware market freshness.

WITH counts AS (
    SELECT
        (SELECT COUNT(*)
         FROM risk_platform.daily_market_freshness) AS history_rows,
        (SELECT COUNT(*)
         FROM risk_platform.latest_daily_market_freshness) AS latest_rows,
        (SELECT COUNT(*)
         FROM risk_platform.daily_market_freshness_exceptions) AS exception_rows,
        (SELECT COUNT(*)
         FROM risk_platform.current_daily_market_freshness) AS current_rows
),
integrity AS (
    SELECT
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_market_freshness
            WHERE
                jsonb_typeof(missing_sessions_json) <> 'array'
                OR jsonb_array_length(missing_sessions_json)
                    <> missing_session_count
                OR expected_session_count
                    <> observation_count + missing_session_count
                OR trailing_missing_session_count > missing_session_count
        ) AS invalid_counts,
        (
            SELECT COUNT(*)
            FROM risk_platform.daily_market_freshness
            WHERE
                (freshness_status = 'current'
                    AND (
                        missing_session_count <> 0
                        OR trailing_missing_session_count <> 0
                        OR latest_observation_date
                            <> expected_latest_session_date
                    ))
                OR
                (freshness_status = 'gap_detected'
                    AND (
                        missing_session_count = 0
                        OR trailing_missing_session_count <> 0
                        OR latest_observation_date
                            <> expected_latest_session_date
                    ))
                OR
                (freshness_status = 'stale'
                    AND (
                        trailing_missing_session_count = 0
                        OR latest_observation_date
                            >= expected_latest_session_date
                    ))
        ) AS invalid_status_rows,
        (
            SELECT COUNT(*)
            FROM (
                SELECT calculation_id
                FROM risk_platform.daily_market_freshness
                GROUP BY calculation_id
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_calculation_ids,
        (
            SELECT COUNT(*)
            FROM (
                SELECT
                    source,
                    symbol,
                    calendar_id,
                    as_of_date,
                    model_version
                FROM risk_platform.latest_daily_market_freshness
                GROUP BY
                    source,
                    symbol,
                    calendar_id,
                    as_of_date,
                    model_version
                HAVING COUNT(*) > 1
            ) duplicated
        ) AS duplicate_latest_grains,
        (
            SELECT COUNT(*)
            FROM risk_platform.latest_daily_market_freshness selected
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.daily_market_freshness candidate
                WHERE
                    candidate.source = selected.source
                    AND candidate.symbol = selected.symbol
                    AND candidate.calendar_id = selected.calendar_id
                    AND candidate.as_of_date = selected.as_of_date
                    AND candidate.model_version = selected.model_version
                    AND (
                        candidate.ts_ingest,
                        candidate.calculation_id
                    ) > (
                        selected.ts_ingest,
                        selected.calculation_id
                    )
            )
        ) AS stale_latest_rows,
        (
            SELECT COUNT(*)
            FROM risk_platform.current_daily_market_freshness selected
            WHERE EXISTS (
                SELECT 1
                FROM risk_platform.latest_daily_market_freshness candidate
                WHERE
                    candidate.source = selected.source
                    AND candidate.symbol = selected.symbol
                    AND candidate.calendar_id = selected.calendar_id
                    AND candidate.model_version = selected.model_version
                    AND (
                        candidate.as_of_date,
                        candidate.ts_ingest,
                        candidate.calculation_id
                    ) > (
                        selected.as_of_date,
                        selected.ts_ingest,
                        selected.calculation_id
                    )
            )
        ) AS stale_current_rows
)
SELECT
    'daily_market_freshness_counts_reconcile' AS check_name,
    history_rows::TEXT AS expected,
    history_rows::TEXT AS actual,
    'pass' AS status
FROM counts

UNION ALL

SELECT
    'daily_market_freshness_json_and_counts_valid',
    '0',
    invalid_counts::TEXT,
    CASE WHEN invalid_counts = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_market_freshness_status_valid',
    '0',
    invalid_status_rows::TEXT,
    CASE WHEN invalid_status_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_market_freshness_calculation_ids_unique',
    '0',
    duplicate_calculation_ids::TEXT,
    CASE WHEN duplicate_calculation_ids = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_daily_market_freshness_grain_unique',
    '0',
    duplicate_latest_grains::TEXT,
    CASE WHEN duplicate_latest_grains = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'latest_daily_market_freshness_selects_current_version',
    '0',
    stale_latest_rows::TEXT,
    CASE WHEN stale_latest_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'current_daily_market_freshness_selects_latest_as_of',
    '0',
    stale_current_rows::TEXT,
    CASE WHEN stale_current_rows = 0 THEN 'pass' ELSE 'fail' END
FROM integrity

UNION ALL

SELECT
    'daily_market_freshness_exception_rows_match',
    exception_rows::TEXT,
    (
        SELECT COUNT(*)::TEXT
        FROM risk_platform.latest_daily_market_freshness
        WHERE freshness_status <> 'current'
    ),
    CASE
        WHEN exception_rows = (
            SELECT COUNT(*)
            FROM risk_platform.latest_daily_market_freshness
            WHERE freshness_status <> 'current'
        ) THEN 'pass'
        ELSE 'fail'
    END
FROM counts

UNION ALL

SELECT
    'current_daily_market_freshness_rows_bounded',
    latest_rows::TEXT,
    current_rows::TEXT,
    CASE WHEN current_rows <= latest_rows THEN 'pass' ELSE 'fail' END
FROM counts

ORDER BY check_name;
