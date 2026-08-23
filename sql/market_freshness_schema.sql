-- PostgreSQL serving contract for exchange-calendar-aware market freshness.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.daily_market_freshness (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    calendar_id TEXT NOT NULL,
    calendar_fingerprint TEXT NOT NULL,
    calendar_timezone TEXT NOT NULL,
    calendar_valid_from DATE NOT NULL,
    calendar_valid_to DATE NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    as_of_date DATE NOT NULL,
    as_of_day_type TEXT NOT NULL CHECK (
        as_of_day_type IN ('session', 'weekend', 'holiday')
    ),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    first_observation_date DATE NOT NULL,
    latest_observation_date DATE NOT NULL,
    expected_latest_session_date DATE NOT NULL,
    expected_session_close_time TIME NOT NULL,
    expected_session_is_early_close BOOLEAN NOT NULL,
    observation_count INTEGER NOT NULL CHECK (observation_count > 0),
    expected_session_count INTEGER NOT NULL CHECK (
        expected_session_count > 0
    ),
    missing_session_count INTEGER NOT NULL CHECK (
        missing_session_count >= 0
    ),
    trailing_missing_session_count INTEGER NOT NULL CHECK (
        trailing_missing_session_count >= 0
    ),
    missing_sessions_json JSONB NOT NULL,
    freshness_status TEXT NOT NULL CHECK (
        freshness_status IN ('current', 'gap_detected', 'stale')
    ),
    input_fingerprint TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (calendar_id <> ''),
    CHECK (calendar_fingerprint <> ''),
    CHECK (calendar_timezone <> ''),
    CHECK (calendar_valid_from < calendar_valid_to),
    CHECK (source <> ''),
    CHECK (symbol <> ''),
    CHECK (calendar_valid_from <= first_observation_date),
    CHECK (first_observation_date <= latest_observation_date),
    CHECK (latest_observation_date <= as_of_date),
    CHECK (expected_latest_session_date <= as_of_date),
    CHECK (as_of_date < calendar_valid_to),
    CHECK (
        expected_session_count =
        observation_count + missing_session_count
    ),
    CHECK (
        trailing_missing_session_count <= missing_session_count
    ),
    CHECK (jsonb_typeof(missing_sessions_json) = 'array'),
    CHECK (
        jsonb_array_length(missing_sessions_json) =
        missing_session_count
    ),
    CHECK (
        (freshness_status = 'current'
            AND missing_session_count = 0
            AND trailing_missing_session_count = 0
            AND latest_observation_date = expected_latest_session_date)
        OR
        (freshness_status = 'gap_detected'
            AND missing_session_count > 0
            AND trailing_missing_session_count = 0
            AND latest_observation_date = expected_latest_session_date)
        OR
        (freshness_status = 'stale'
            AND trailing_missing_session_count > 0
            AND latest_observation_date < expected_latest_session_date)
    ),
    CHECK (input_fingerprint <> '')
);

CREATE INDEX IF NOT EXISTS idx_daily_market_freshness_current_lookup
    ON risk_platform.daily_market_freshness (
        source,
        symbol,
        calendar_id,
        as_of_date DESC,
        model_version,
        ts_ingest DESC,
        calculation_id DESC
    );

CREATE OR REPLACE VIEW risk_platform.latest_daily_market_freshness AS
SELECT
    calculation_id,
    model_version,
    calendar_id,
    calendar_fingerprint,
    calendar_timezone,
    calendar_valid_from,
    calendar_valid_to,
    source,
    symbol,
    as_of_date,
    as_of_day_type,
    ts_event,
    ts_ingest,
    first_observation_date,
    latest_observation_date,
    expected_latest_session_date,
    expected_session_close_time,
    expected_session_is_early_close,
    observation_count,
    expected_session_count,
    missing_session_count,
    trailing_missing_session_count,
    missing_sessions_json,
    freshness_status,
    input_fingerprint
FROM (
    SELECT
        freshness.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                source,
                symbol,
                calendar_id,
                as_of_date,
                model_version
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.daily_market_freshness freshness
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.daily_market_freshness_exceptions AS
SELECT *
FROM risk_platform.latest_daily_market_freshness
WHERE freshness_status <> 'current';

CREATE OR REPLACE VIEW risk_platform.current_daily_market_freshness AS
SELECT DISTINCT ON (
    source,
    symbol,
    calendar_id,
    model_version
)
    calculation_id,
    model_version,
    calendar_id,
    calendar_fingerprint,
    calendar_timezone,
    calendar_valid_from,
    calendar_valid_to,
    source,
    symbol,
    as_of_date,
    as_of_day_type,
    ts_event,
    ts_ingest,
    first_observation_date,
    latest_observation_date,
    expected_latest_session_date,
    expected_session_close_time,
    expected_session_is_early_close,
    observation_count,
    expected_session_count,
    missing_session_count,
    trailing_missing_session_count,
    missing_sessions_json,
    freshness_status,
    input_fingerprint
FROM risk_platform.latest_daily_market_freshness
ORDER BY
    source,
    symbol,
    calendar_id,
    model_version,
    as_of_date DESC,
    ts_ingest DESC,
    calculation_id DESC;
