-- Reconciliation checks for the local MongoDB -> pipeline -> PostgreSQL demo.
-- These checks assume sql/postgres_schema.sql and sql/postgres_demo_data.sql
-- have been applied, or that pipeline outputs have been loaded with
-- src.warehouse.postgres_loader.

WITH latest_source_audit AS (
    SELECT *
    FROM staging.source_event_audit
    ORDER BY audit_ts DESC
    LIMIT 1
),
latest_quality AS (
    SELECT *
    FROM risk_platform.data_quality_metrics
    ORDER BY ts_ingest DESC
    LIMIT 1
),
warehouse_counts AS (
    SELECT
        (SELECT COUNT(*) FROM risk_platform.market_events_raw) AS raw_events,
        (SELECT COUNT(*) FROM risk_platform.returns_1m) AS returns_1m,
        (SELECT COUNT(*) FROM risk_platform.volatility_5m) AS volatility_5m,
        (SELECT COUNT(*) FROM risk_platform.risk_summary) AS risk_summary,
        (SELECT COUNT(*) FROM risk_platform.data_quality_metrics) AS data_quality_runs,
        (SELECT COUNT(*) FROM risk_platform.latest_risk_summary) AS latest_risk_symbols,
        (SELECT COUNT(*) FROM risk_platform.current_symbol_dimension) AS current_symbol_dimensions,
        (SELECT COUNT(*) FROM risk_platform.finance_risk_semantic_model) AS finance_reporting_rows
)
SELECT
    'source_records_to_quality_total' AS check_name,
    source_records::text AS expected,
    total_events::text AS actual,
    CASE WHEN source_records = total_events THEN 'pass' ELSE 'fail' END AS status
FROM latest_source_audit, latest_quality

UNION ALL

SELECT
    'distinct_event_ids_to_raw_events',
    distinct_event_ids::text,
    raw_events::text,
    CASE WHEN distinct_event_ids = raw_events THEN 'pass' ELSE 'fail' END
FROM latest_source_audit, warehouse_counts

UNION ALL

SELECT
    'duplicate_records_to_quality_duplicates',
    duplicate_records::text,
    duplicate_events::text,
    CASE WHEN duplicate_records = duplicate_events THEN 'pass' ELSE 'fail' END
FROM latest_source_audit, latest_quality

UNION ALL

SELECT
    'late_records_to_quality_late_events',
    expected_late_events::text,
    late_events::text,
    CASE WHEN expected_late_events = late_events THEN 'pass' ELSE 'fail' END
FROM latest_source_audit, latest_quality

UNION ALL

SELECT
    'raw_events_to_quality_deduped_events',
    deduped_events::text,
    raw_events::text,
    CASE WHEN deduped_events = raw_events THEN 'pass' ELSE 'fail' END
FROM latest_quality, warehouse_counts

UNION ALL

SELECT
    'returns_rows_expected',
    '4',
    returns_1m::text,
    CASE WHEN returns_1m = 4 THEN 'pass' ELSE 'fail' END
FROM warehouse_counts

UNION ALL

SELECT
    'volatility_rows_expected',
    '2',
    volatility_5m::text,
    CASE WHEN volatility_5m = 2 THEN 'pass' ELSE 'fail' END
FROM warehouse_counts

UNION ALL

SELECT
    'risk_summary_rows_expected',
    '2',
    risk_summary::text,
    CASE WHEN risk_summary = 2 THEN 'pass' ELSE 'fail' END
FROM warehouse_counts

UNION ALL

SELECT
    'latest_quality_status_expected',
    'late=critical, duplicate=critical',
    CONCAT('late=', late_status, ', duplicate=', duplicate_status),
    CASE
        WHEN late_status = 'critical' AND duplicate_status = 'critical'
        THEN 'pass'
        ELSE 'fail'
    END
FROM latest_quality

UNION ALL

SELECT
    'current_symbol_dimension_rows_expected',
    '2',
    current_symbol_dimensions::text,
    CASE WHEN current_symbol_dimensions = 2 THEN 'pass' ELSE 'fail' END
FROM warehouse_counts

UNION ALL

SELECT
    'finance_reporting_rows_to_latest_risk_symbols',
    latest_risk_symbols::text,
    finance_reporting_rows::text,
    CASE
        WHEN finance_reporting_rows = latest_risk_symbols THEN 'pass'
        ELSE 'fail'
    END
FROM warehouse_counts

ORDER BY check_name;
