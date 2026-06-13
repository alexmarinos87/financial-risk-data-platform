-- PostgreSQL warehouse schema for curated pipeline outputs.
-- The parquet data lake remains the durable landing zone; these tables model
-- the warehouse-facing contract for operational consumers.

CREATE SCHEMA IF NOT EXISTS risk_platform;

CREATE TABLE IF NOT EXISTS risk_platform.market_events_raw (
    event_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC(18, 6) NOT NULL CHECK (price > 0),
    volume BIGINT NOT NULL CHECK (volume >= 0),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_market_events_raw_symbol_event_time
    ON risk_platform.market_events_raw (symbol, ts_event DESC);

CREATE INDEX IF NOT EXISTS idx_market_events_raw_ingest_time
    ON risk_platform.market_events_raw (ts_ingest DESC);

CREATE TABLE IF NOT EXISTS risk_platform.returns_1m (
    symbol TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    return_1m DOUBLE PRECISION NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ts_event)
);

CREATE INDEX IF NOT EXISTS idx_returns_1m_window
    ON risk_platform.returns_1m (window_start DESC, symbol);

CREATE TABLE IF NOT EXISTS risk_platform.volatility_5m (
    symbol TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    volatility_5m DOUBLE PRECISION NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ts_event)
);

CREATE INDEX IF NOT EXISTS idx_volatility_5m_window
    ON risk_platform.volatility_5m (window_start DESC, symbol);

CREATE TABLE IF NOT EXISTS risk_platform.data_quality_metrics (
    metric_id BIGSERIAL PRIMARY KEY,
    total_events INTEGER NOT NULL CHECK (total_events >= 0),
    deduped_events INTEGER NOT NULL CHECK (deduped_events >= 0),
    duplicate_events INTEGER NOT NULL CHECK (duplicate_events >= 0),
    late_events INTEGER NOT NULL CHECK (late_events >= 0),
    late_rate DOUBLE PRECISION NOT NULL CHECK (late_rate >= 0),
    duplicate_rate DOUBLE PRECISION NOT NULL CHECK (duplicate_rate >= 0),
    required_fields_checked INTEGER NOT NULL CHECK (required_fields_checked >= 0),
    missing_required_field_count INTEGER NOT NULL CHECK (missing_required_field_count >= 0),
    missing_required_record_count INTEGER NOT NULL CHECK (missing_required_record_count >= 0),
    missing_required_fields_by_name JSONB NOT NULL,
    required_fields_status TEXT NOT NULL CHECK (required_fields_status IN ('ok', 'warn', 'critical')),
    null_fields_checked INTEGER NOT NULL CHECK (null_fields_checked >= 0),
    null_field_count INTEGER NOT NULL CHECK (null_field_count >= 0),
    null_record_count INTEGER NOT NULL CHECK (null_record_count >= 0),
    max_null_rate DOUBLE PRECISION NOT NULL CHECK (max_null_rate >= 0),
    null_fields_by_name JSONB NOT NULL,
    null_rates_by_name JSONB NOT NULL,
    null_rate_status TEXT NOT NULL CHECK (null_rate_status IN ('ok', 'warn', 'critical')),
    value_fields_checked INTEGER NOT NULL CHECK (value_fields_checked >= 0),
    invalid_value_count INTEGER NOT NULL CHECK (invalid_value_count >= 0),
    invalid_value_record_count INTEGER NOT NULL CHECK (invalid_value_record_count >= 0),
    invalid_values_by_name JSONB NOT NULL,
    value_validity_status TEXT NOT NULL CHECK (value_validity_status IN ('ok', 'warn', 'critical')),
    late_status TEXT NOT NULL CHECK (late_status IN ('ok', 'warn', 'critical')),
    duplicate_status TEXT NOT NULL CHECK (duplicate_status IN ('ok', 'warn', 'critical')),
    ts_ingest TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (ts_ingest)
);

CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_ingest_time
    ON risk_platform.data_quality_metrics (ts_ingest DESC);

CREATE INDEX IF NOT EXISTS idx_data_quality_metrics_status
    ON risk_platform.data_quality_metrics (
        required_fields_status,
        null_rate_status,
        value_validity_status,
        late_status,
        duplicate_status
    );

CREATE TABLE IF NOT EXISTS risk_platform.risk_summary (
    symbol TEXT NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    volatility_5m DOUBLE PRECISION,
    value_at_risk_95 DOUBLE PRECISION,
    volatility_status TEXT NOT NULL CHECK (volatility_status IN ('ok', 'warn', 'critical', 'no_data')),
    late_rate DOUBLE PRECISION NOT NULL CHECK (late_rate >= 0),
    duplicate_rate DOUBLE PRECISION NOT NULL CHECK (duplicate_rate >= 0),
    late_status TEXT NOT NULL CHECK (late_status IN ('ok', 'warn', 'critical')),
    duplicate_status TEXT NOT NULL CHECK (duplicate_status IN ('ok', 'warn', 'critical')),
    external_signal_count INTEGER NOT NULL DEFAULT 0 CHECK (external_signal_count >= 0),
    latest_external_signal_name TEXT,
    latest_external_signal_value DOUBLE PRECISION,
    latest_external_signal_source TEXT,
    latest_external_signal_ts_event TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, ts_ingest)
);

CREATE INDEX IF NOT EXISTS idx_risk_summary_latest
    ON risk_platform.risk_summary (ts_ingest DESC, symbol);

CREATE INDEX IF NOT EXISTS idx_risk_summary_status
    ON risk_platform.risk_summary (volatility_status, late_status, duplicate_status);

CREATE TABLE IF NOT EXISTS risk_platform.external_signal_summary (
    name TEXT NOT NULL,
    source TEXT NOT NULL,
    latest_value DOUBLE PRECISION NOT NULL,
    latest_signal_id TEXT NOT NULL,
    latest_ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (name, source, latest_signal_id)
);

CREATE INDEX IF NOT EXISTS idx_external_signal_summary_latest
    ON risk_platform.external_signal_summary (latest_ts_event DESC, name, source);

CREATE TABLE IF NOT EXISTS risk_platform.symbol_dimension_history (
    symbol_dimension_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL,
    asset_class TEXT NOT NULL,
    reporting_currency CHAR(3) NOT NULL,
    sector TEXT,
    effective_from TIMESTAMPTZ NOT NULL,
    effective_to TIMESTAMPTZ,
    is_current BOOLEAN NOT NULL,
    change_reason TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (symbol = UPPER(symbol)),
    CHECK (reporting_currency = UPPER(reporting_currency)),
    CHECK (record_hash <> ''),
    CHECK (effective_to IS NULL OR effective_to > effective_from),
    CHECK (
        (is_current = true AND effective_to IS NULL)
        OR (is_current = false AND effective_to IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_dimension_history_version
    ON risk_platform.symbol_dimension_history (symbol, source, effective_from);

CREATE UNIQUE INDEX IF NOT EXISTS idx_symbol_dimension_history_current
    ON risk_platform.symbol_dimension_history (symbol, source)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS idx_symbol_dimension_history_reporting
    ON risk_platform.symbol_dimension_history (
        asset_class,
        reporting_currency,
        sector,
        effective_from DESC
    );

CREATE OR REPLACE VIEW risk_platform.latest_risk_summary AS
SELECT DISTINCT ON (symbol)
    symbol,
    ts_ingest,
    volatility_5m,
    value_at_risk_95,
    volatility_status,
    late_rate,
    duplicate_rate,
    late_status,
    duplicate_status,
    external_signal_count,
    latest_external_signal_name,
    latest_external_signal_value,
    latest_external_signal_source,
    latest_external_signal_ts_event
FROM risk_platform.risk_summary
ORDER BY symbol, ts_ingest DESC;

CREATE OR REPLACE VIEW risk_platform.latest_data_quality_status AS
SELECT
    ts_ingest,
    total_events,
    deduped_events,
    duplicate_events,
    late_events,
    late_rate,
    duplicate_rate,
    required_fields_status,
    null_rate_status,
    value_validity_status,
    late_status,
    duplicate_status
FROM risk_platform.data_quality_metrics
ORDER BY ts_ingest DESC
LIMIT 1;

CREATE OR REPLACE VIEW risk_platform.current_symbol_dimension AS
SELECT
    symbol_dimension_id,
    symbol,
    source,
    asset_class,
    reporting_currency,
    sector,
    effective_from,
    change_reason,
    record_hash
FROM risk_platform.symbol_dimension_history
WHERE is_current;

CREATE OR REPLACE VIEW risk_platform.finance_risk_semantic_model AS
WITH current_symbol AS (
    SELECT DISTINCT ON (symbol)
        symbol,
        source,
        asset_class,
        reporting_currency,
        sector,
        effective_from
    FROM risk_platform.current_symbol_dimension
    ORDER BY symbol, source
)
SELECT
    risk.symbol,
    dim.asset_class,
    dim.reporting_currency,
    dim.sector,
    dim.effective_from AS dimension_effective_from,
    risk.ts_ingest AS metric_ts,
    risk.volatility_5m,
    risk.value_at_risk_95,
    risk.volatility_status,
    risk.late_rate,
    risk.duplicate_rate,
    risk.late_status,
    risk.duplicate_status,
    risk.external_signal_count,
    quality.required_fields_status,
    quality.null_rate_status,
    quality.value_validity_status
FROM risk_platform.latest_risk_summary risk
LEFT JOIN current_symbol dim ON risk.symbol = dim.symbol
LEFT JOIN risk_platform.latest_data_quality_status quality ON true;
