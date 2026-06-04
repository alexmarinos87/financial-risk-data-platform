-- Operational PostgreSQL queries for warehouse consumers.
-- These assume the schema in sql/postgres_schema.sql has been applied.

-- 1. Latest pipeline health snapshot.
SELECT
    ts_ingest,
    total_events,
    deduped_events,
    duplicate_events,
    ROUND((duplicate_rate * 100)::numeric, 2) AS duplicate_rate_pct,
    late_events,
    ROUND((late_rate * 100)::numeric, 2) AS late_rate_pct,
    required_fields_status,
    null_rate_status,
    value_validity_status,
    late_status,
    duplicate_status
FROM risk_platform.latest_data_quality_status;

-- 2. Recent non-OK data quality checks for incident review.
SELECT
    ts_ingest,
    total_events,
    missing_required_record_count,
    null_record_count,
    invalid_value_record_count,
    duplicate_events,
    late_events,
    required_fields_status,
    null_rate_status,
    value_validity_status,
    late_status,
    duplicate_status
FROM risk_platform.data_quality_metrics
WHERE required_fields_status <> 'ok'
   OR null_rate_status <> 'ok'
   OR value_validity_status <> 'ok'
   OR late_status <> 'ok'
   OR duplicate_status <> 'ok'
ORDER BY ts_ingest DESC
LIMIT 50;

-- 3. Latest risk summary for ops dashboards.
SELECT
    symbol,
    ts_ingest,
    volatility_5m,
    value_at_risk_95,
    volatility_status,
    late_status,
    duplicate_status,
    external_signal_count
FROM risk_platform.latest_risk_summary
ORDER BY symbol;

-- 4. Symbols with recent elevated risk or quality status.
SELECT
    symbol,
    ts_ingest,
    volatility_status,
    late_status,
    duplicate_status,
    volatility_5m,
    value_at_risk_95
FROM risk_platform.risk_summary
WHERE ts_ingest >= now() - interval '24 hours'
  AND (
      volatility_status <> 'ok'
      OR late_status <> 'ok'
      OR duplicate_status <> 'ok'
  )
ORDER BY ts_ingest DESC, symbol;

-- 5. Warehouse freshness check by curated table.
SELECT
    table_name,
    latest_ts_ingest,
    now() - latest_ts_ingest AS age
FROM (
    SELECT 'returns_1m' AS table_name, MAX(ts_ingest) AS latest_ts_ingest
    FROM risk_platform.returns_1m
    UNION ALL
    SELECT 'volatility_5m', MAX(ts_ingest)
    FROM risk_platform.volatility_5m
    UNION ALL
    SELECT 'risk_summary', MAX(ts_ingest)
    FROM risk_platform.risk_summary
    UNION ALL
    SELECT 'data_quality_metrics', MAX(ts_ingest)
    FROM risk_platform.data_quality_metrics
) freshness
ORDER BY latest_ts_ingest NULLS FIRST;

-- 6. Idempotent raw event load pattern from a staging table.
INSERT INTO risk_platform.market_events_raw (
    event_id,
    symbol,
    price,
    volume,
    ts_event,
    ts_ingest,
    source
)
SELECT
    event_id,
    UPPER(symbol) AS symbol,
    price,
    volume,
    ts_event,
    ts_ingest,
    source
FROM staging.market_events_raw_batch
ON CONFLICT (event_id) DO UPDATE
SET
    symbol = EXCLUDED.symbol,
    price = EXCLUDED.price,
    volume = EXCLUDED.volume,
    ts_event = EXCLUDED.ts_event,
    ts_ingest = EXCLUDED.ts_ingest,
    source = EXCLUDED.source,
    loaded_at = now()
WHERE risk_platform.market_events_raw.ts_ingest <= EXCLUDED.ts_ingest;

-- 7. Idempotent risk summary load pattern from a staging table.
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
    external_signal_count,
    latest_external_signal_name,
    latest_external_signal_value,
    latest_external_signal_source,
    latest_external_signal_ts_event
)
SELECT
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
FROM staging.risk_summary_batch
ON CONFLICT (symbol, ts_ingest) DO UPDATE
SET
    volatility_5m = EXCLUDED.volatility_5m,
    value_at_risk_95 = EXCLUDED.value_at_risk_95,
    volatility_status = EXCLUDED.volatility_status,
    late_rate = EXCLUDED.late_rate,
    duplicate_rate = EXCLUDED.duplicate_rate,
    late_status = EXCLUDED.late_status,
    duplicate_status = EXCLUDED.duplicate_status,
    external_signal_count = EXCLUDED.external_signal_count,
    latest_external_signal_name = EXCLUDED.latest_external_signal_name,
    latest_external_signal_value = EXCLUDED.latest_external_signal_value,
    latest_external_signal_source = EXCLUDED.latest_external_signal_source,
    latest_external_signal_ts_event = EXCLUDED.latest_external_signal_ts_event,
    loaded_at = now();
