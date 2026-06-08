-- Local PostgreSQL demo data matching tests/fixtures/demo_events.json.
-- This makes the warehouse-serving layer inspectable without running a full
-- ingestion-to-warehouse load.

CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.market_events_raw_batch (
    event_id TEXT,
    symbol TEXT,
    price NUMERIC(18, 6),
    volume BIGINT,
    ts_event TIMESTAMPTZ,
    ts_ingest TIMESTAMPTZ,
    source TEXT
);

CREATE TABLE IF NOT EXISTS staging.risk_summary_batch (
    symbol TEXT,
    ts_ingest TIMESTAMPTZ,
    volatility_5m DOUBLE PRECISION,
    value_at_risk_95 DOUBLE PRECISION,
    volatility_status TEXT,
    late_rate DOUBLE PRECISION,
    duplicate_rate DOUBLE PRECISION,
    late_status TEXT,
    duplicate_status TEXT,
    external_signal_count INTEGER,
    latest_external_signal_name TEXT,
    latest_external_signal_value DOUBLE PRECISION,
    latest_external_signal_source TEXT,
    latest_external_signal_ts_event TIMESTAMPTZ
);

TRUNCATE TABLE
    staging.market_events_raw_batch,
    staging.risk_summary_batch,
    risk_platform.external_signal_summary,
    risk_platform.risk_summary,
    risk_platform.data_quality_metrics,
    risk_platform.volatility_5m,
    risk_platform.returns_1m,
    risk_platform.market_events_raw
RESTART IDENTITY;

INSERT INTO risk_platform.market_events_raw (
    event_id,
    symbol,
    price,
    volume,
    ts_event,
    ts_ingest,
    source
)
VALUES
    ('evt-1', 'AAPL', 100.0, 10, '2025-01-20T10:01:00Z', '2025-01-20T10:01:05Z', 'stooq'),
    ('evt-2', 'AAPL', 101.0, 11, '2025-01-20T10:02:00Z', '2025-01-20T10:02:04Z', 'stooq'),
    ('evt-3', 'AAPL', 102.0, 12, '2025-01-20T10:03:00Z', '2025-01-20T10:03:04Z', 'stooq'),
    ('evt-4', 'MSFT', 240.0, 9, '2025-01-20T10:01:00Z', '2025-01-20T10:01:03Z', 'stooq'),
    ('evt-5', 'MSFT', 242.0, 10, '2025-01-20T10:02:00Z', '2025-01-20T10:07:00Z', 'stooq'),
    ('evt-6', 'MSFT', 241.0, 8, '2025-01-20T10:03:00Z', '2025-01-20T10:03:05Z', 'stooq')
ON CONFLICT (event_id) DO NOTHING;

INSERT INTO risk_platform.returns_1m (
    symbol,
    ts_event,
    window_start,
    return_1m,
    ts_ingest
)
VALUES
    ('AAPL', '2025-01-20T10:02:00Z', '2025-01-20T10:00:00Z', 0.010000000000000009, '2025-01-20T10:02:04Z'),
    ('AAPL', '2025-01-20T10:03:00Z', '2025-01-20T10:00:00Z', 0.00990099009900991, '2025-01-20T10:03:04Z'),
    ('MSFT', '2025-01-20T10:02:00Z', '2025-01-20T10:00:00Z', 0.008333333333333304, '2025-01-20T10:07:00Z'),
    ('MSFT', '2025-01-20T10:03:00Z', '2025-01-20T10:00:00Z', -0.004132231404958664, '2025-01-20T10:03:05Z')
ON CONFLICT (symbol, ts_event) DO NOTHING;

INSERT INTO risk_platform.volatility_5m (
    symbol,
    ts_event,
    window_start,
    volatility_5m,
    ts_ingest
)
VALUES
    ('AAPL', '2025-01-20T10:03:00Z', '2025-01-20T10:00:00Z', 0.00007001057239470774, '2025-01-20T10:03:04Z'),
    ('MSFT', '2025-01-20T10:03:00Z', '2025-01-20T10:00:00Z', 0.00881448535776616, '2025-01-20T10:03:05Z')
ON CONFLICT (symbol, ts_event) DO NOTHING;

INSERT INTO risk_platform.data_quality_metrics (
    total_events,
    deduped_events,
    duplicate_events,
    late_events,
    late_rate,
    duplicate_rate,
    required_fields_checked,
    missing_required_field_count,
    missing_required_record_count,
    missing_required_fields_by_name,
    required_fields_status,
    null_fields_checked,
    null_field_count,
    null_record_count,
    max_null_rate,
    null_fields_by_name,
    null_rates_by_name,
    null_rate_status,
    value_fields_checked,
    invalid_value_count,
    invalid_value_record_count,
    invalid_values_by_name,
    value_validity_status,
    late_status,
    duplicate_status,
    ts_ingest
)
VALUES (
    7,
    6,
    1,
    1,
    0.16666666666666666,
    0.1428571428571429,
    7,
    0,
    0,
    '{"event_id": 0, "price": 0, "source": 0, "symbol": 0, "ts_event": 0, "ts_ingest": 0, "volume": 0}'::jsonb,
    'ok',
    7,
    0,
    0,
    0.0,
    '{"event_id": 0, "price": 0, "source": 0, "symbol": 0, "ts_event": 0, "ts_ingest": 0, "volume": 0}'::jsonb,
    '{"event_id": 0.0, "price": 0.0, "source": 0.0, "symbol": 0.0, "ts_event": 0.0, "ts_ingest": 0.0, "volume": 0.0}'::jsonb,
    'ok',
    2,
    0,
    0,
    '{"price": 0, "volume": 0}'::jsonb,
    'ok',
    'critical',
    'critical',
    '2025-01-20T10:07:00Z'
);

INSERT INTO risk_platform.risk_summary (
    symbol,
    ts_ingest,
    volatility_5m,
    value_at_risk_95,
    volatility_status,
    late_rate,
    duplicate_rate,
    late_status,
    duplicate_status,
    external_signal_count
)
VALUES
    (
        'AAPL',
        '2025-01-20T10:07:00Z',
        0.00007001057239470774,
        0.009905940594059415,
        'ok',
        0.16666666666666666,
        0.1428571428571429,
        'critical',
        'critical',
        0
    ),
    (
        'MSFT',
        '2025-01-20T10:07:00Z',
        0.00881448535776616,
        -0.003508953168044065,
        'ok',
        0.16666666666666666,
        0.1428571428571429,
        'critical',
        'critical',
        0
    )
ON CONFLICT (symbol, ts_ingest) DO NOTHING;

INSERT INTO staging.market_events_raw_batch (
    event_id,
    symbol,
    price,
    volume,
    ts_event,
    ts_ingest,
    source
)
VALUES
    ('evt-6', 'msft', 241.0, 8, '2025-01-20T10:03:00Z', '2025-01-20T10:03:05Z', 'stooq'),
    ('evt-7', 'goog', 188.5, 15, '2025-01-20T10:04:00Z', '2025-01-20T10:04:03Z', 'stooq');
