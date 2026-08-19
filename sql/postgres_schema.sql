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

CREATE TABLE IF NOT EXISTS risk_platform.daily_returns (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    previous_source_event_id TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    return_1d DOUBLE PRECISION NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (source <> ''),
    CHECK (symbol = UPPER(symbol)),
    CHECK (source_event_id <> previous_source_event_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_returns_lookup
    ON risk_platform.daily_returns (
        source,
        symbol,
        ts_event DESC,
        model_version,
        ts_ingest DESC
    );

CREATE TABLE IF NOT EXISTS risk_platform.daily_volatility (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    window_observations INTEGER NOT NULL CHECK (window_observations >= 2),
    annualization_days INTEGER NOT NULL CHECK (annualization_days > 0),
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    volatility_annualized DOUBLE PRECISION NOT NULL CHECK (volatility_annualized >= 0),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (source <> ''),
    CHECK (symbol = UPPER(symbol)),
    CHECK (window_start < window_end),
    CHECK (window_end = ts_event)
);

CREATE INDEX IF NOT EXISTS idx_daily_volatility_lookup
    ON risk_platform.daily_volatility (
        source,
        symbol,
        ts_event DESC,
        model_version,
        window_observations,
        annualization_days,
        ts_ingest DESC
    );

CREATE TABLE IF NOT EXISTS risk_platform.daily_risk_summary (
    calculation_id TEXT PRIMARY KEY,
    model_version TEXT NOT NULL,
    source TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    ts_event TIMESTAMPTZ NOT NULL,
    ts_ingest TIMESTAMPTZ NOT NULL,
    price_close DOUBLE PRECISION NOT NULL CHECK (price_close > 0),
    return_1d DOUBLE PRECISION NOT NULL,
    volatility_annualized DOUBLE PRECISION CHECK (volatility_annualized >= 0),
    volatility_window INTEGER CHECK (volatility_window IS NULL OR volatility_window >= 2),
    annualization_days INTEGER CHECK (annualization_days IS NULL OR annualization_days > 0),
    historical_var_loss DOUBLE PRECISION CHECK (historical_var_loss >= 0),
    var_confidence DOUBLE PRECISION NOT NULL CHECK (var_confidence > 0 AND var_confidence < 1),
    var_window INTEGER NOT NULL CHECK (var_window >= 2),
    var_observations INTEGER NOT NULL CHECK (var_observations >= 0),
    maximum_drawdown DOUBLE PRECISION NOT NULL CHECK (
        maximum_drawdown >= -1 AND maximum_drawdown <= 0
    ),
    price_observations INTEGER NOT NULL CHECK (price_observations >= 2),
    return_observations INTEGER NOT NULL CHECK (return_observations >= 1),
    history_status TEXT NOT NULL CHECK (history_status IN ('partial', 'ready')),
    input_first_event_id TEXT NOT NULL,
    input_last_event_id TEXT NOT NULL,
    var_input_first_event_id TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (calculation_id <> ''),
    CHECK (model_version <> ''),
    CHECK (source <> ''),
    CHECK (symbol = UPPER(symbol)),
    CHECK (return_observations = price_observations - 1),
    CHECK (var_observations <= var_window),
    CHECK (
        model_version <> 'daily-risk-v2'
        OR (volatility_window IS NOT NULL AND annualization_days IS NOT NULL)
    ),
    CHECK (
        model_version <> 'daily-risk-v2'
        OR history_status <> 'ready'
        OR (
            return_observations >= GREATEST(volatility_window, var_window)
            AND var_observations >= var_window
            AND volatility_annualized IS NOT NULL
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_daily_risk_summary_version_lookup
    ON risk_platform.daily_risk_summary (
        source,
        symbol,
        ts_event DESC,
        model_version,
        volatility_window,
        var_window,
        var_confidence,
        annualization_days,
        ts_ingest DESC,
        calculation_id DESC
    );

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

CREATE OR REPLACE VIEW risk_platform.latest_daily_risk_summary AS
SELECT
    calculation_id,
    model_version,
    source,
    symbol,
    source_event_id,
    ts_event,
    ts_ingest,
    price_close,
    return_1d,
    volatility_annualized,
    volatility_window,
    annualization_days,
    historical_var_loss,
    var_confidence,
    var_window,
    var_observations,
    maximum_drawdown,
    price_observations,
    return_observations,
    history_status,
    input_first_event_id,
    input_last_event_id,
    var_input_first_event_id
FROM (
    SELECT
        summary.*,
        ROW_NUMBER() OVER (
            PARTITION BY
                source,
                symbol,
                ts_event,
                model_version,
                volatility_window,
                var_window,
                var_confidence,
                annualization_days
            ORDER BY ts_ingest DESC, calculation_id DESC
        ) AS version_rank
    FROM risk_platform.daily_risk_summary summary
) ranked
WHERE version_rank = 1;

CREATE OR REPLACE VIEW risk_platform.daily_risk_semantic_model AS
SELECT
    risk.calculation_id,
    risk.model_version,
    risk.source,
    risk.symbol,
    dim.asset_class,
    dim.reporting_currency,
    dim.sector,
    dim.effective_from AS dimension_effective_from,
    risk.ts_event AS metric_ts,
    risk.ts_ingest AS calculation_ts,
    risk.price_close,
    risk.return_1d,
    risk.volatility_annualized,
    risk.volatility_window,
    risk.annualization_days,
    risk.historical_var_loss,
    risk.var_confidence,
    risk.var_window,
    risk.var_observations,
    risk.maximum_drawdown,
    risk.price_observations,
    risk.return_observations,
    risk.history_status
FROM risk_platform.latest_daily_risk_summary risk
LEFT JOIN risk_platform.current_symbol_dimension dim
    ON risk.symbol = dim.symbol
    AND risk.source = dim.source;

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
