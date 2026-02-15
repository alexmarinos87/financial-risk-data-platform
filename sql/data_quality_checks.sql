-- Data quality assertions for the market_event vertical slice.
-- Replace <RUN_ID> and <CONTRACT_VERSION> for ad hoc checks.

WITH silver AS (
  SELECT *
  FROM read_parquet(
    'data_lake/silver/market_events/contract_version=<CONTRACT_VERSION>/**/run_id=<RUN_ID>.parquet',
    hive_partitioning = true
  )
),
gold_dq AS (
  SELECT *
  FROM read_parquet(
    'data_lake/gold/data_quality_metrics/contract_version=<CONTRACT_VERSION>/**/run_id=<RUN_ID>.parquet',
    hive_partitioning = true
  )
)
SELECT 'null_required_fields' AS check_name, COUNT(*) AS failed_rows
FROM silver
WHERE event_id IS NULL
   OR symbol IS NULL
   OR price IS NULL
   OR volume IS NULL
   OR ts_event IS NULL
   OR ts_ingest IS NULL
   OR source IS NULL

UNION ALL

SELECT 'duplicate_event_ids' AS check_name, COUNT(*) AS failed_rows
FROM (
  SELECT event_id
  FROM silver
  GROUP BY event_id
  HAVING COUNT(*) > 1
)

UNION ALL

SELECT 'late_rate_threshold_breach' AS check_name, COUNT(*) AS failed_rows
FROM gold_dq
WHERE late_status <> 'ok'

UNION ALL

SELECT 'duplicate_rate_threshold_breach' AS check_name, COUNT(*) AS failed_rows
FROM gold_dq
WHERE duplicate_status <> 'ok';
