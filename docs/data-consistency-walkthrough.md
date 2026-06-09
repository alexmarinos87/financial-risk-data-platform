# Data Consistency Walkthrough

This walkthrough connects the local source, pipeline, and warehouse pieces into
one consistency story.

## Goal

Show that the same demo data can be traced through each layer:

```text
MongoDB-style source documents
  -> pipeline input fixture
  -> raw parquet output
  -> curated parquet output
  -> PostgreSQL warehouse tables
  -> reconciliation checks
```

The expected demo counts are:

| Layer | Expected |
| --- | ---: |
| Source records | 7 |
| Distinct event IDs | 6 |
| Duplicate records | 1 |
| Late records | 1 |
| Raw warehouse events | 6 |
| `returns_1m` rows | 4 |
| `volatility_5m` rows | 2 |
| `risk_summary` rows | 2 |
| Data quality rows | 1 |

## Start Local Databases

```bash
make local-db-up
```

This starts:

1. PostgreSQL on local port `5433`.
2. MongoDB on local port `27018`.

PostgreSQL is seeded with warehouse-style demo data. MongoDB is seeded with
nested source documents, including one duplicate market event.

## Run The Pipeline

```bash
make clean-generated
make run-demo
```

Expected summary:

```text
Raw events written: 6
Curated records written: 9
Late rate: 16.67% (status: critical)
Duplicate rate: 14.29% (status: critical)
```

## Dry-Run The PostgreSQL Loader

```bash
make load-postgres-dry-run
```

This reads local parquet output and prints the rows that would be loaded into
each PostgreSQL table. It does not connect to PostgreSQL.

## Load Pipeline Output Into PostgreSQL

```bash
make load-postgres-demo
```

The loader reads:

```text
data/raw/market_events/
data/curated/returns_1m/
data/curated/volatility_5m/
data/curated/data_quality_metrics/
data/curated/risk_summary/
data/curated/external_signal_summary/
```

It upserts into:

```text
risk_platform.market_events_raw
risk_platform.returns_1m
risk_platform.volatility_5m
risk_platform.data_quality_metrics
risk_platform.risk_summary
risk_platform.external_signal_summary
```

The local connection string is:

```text
postgresql://risk_user:risk_password@localhost:5433/risk_platform
```

Override it with:

```bash
make load-postgres-demo LOCAL_POSTGRES_DSN='postgresql://user:password@host:5432/database'
```

## Run Consistency Checks

```bash
make check-postgres-consistency
```

The checks compare:

1. Source audit record count to latest data quality `total_events`.
2. Distinct source event IDs to raw warehouse events.
3. Source duplicate records to data quality duplicate count.
4. Source late records to data quality late count.
5. Raw events to data quality deduped count.
6. Curated table row counts to expected demo counts.
7. Latest late and duplicate statuses to expected critical statuses.

Every row should return `status = pass`.

## One-Command Local Consistency Demo

With local PostgreSQL already running:

```bash
make consistency-demo
```

This runs:

```text
clean-generated
run-demo
load-postgres-demo
check-postgres-consistency
```

## Inspect MongoDB Source Shape

```bash
make mongo-shell
```

Find duplicate source business events:

```javascript
db.market_events_source.aggregate([
  { $group: { _id: "$eventId", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } }
])
```

Flatten source events into pipeline-shaped records:

```javascript
db.market_events_source.aggregate([
  {
    $project: {
      _id: 0,
      event_id: "$eventId",
      symbol: "$instrument.symbol",
      price: "$trade.price",
      volume: "$trade.volume",
      ts_event: "$timestamps.event",
      ts_ingest: "$timestamps.ingest",
      source: "$provider"
    }
  }
])
```

## Inspect PostgreSQL Warehouse Shape

```bash
make postgres-shell
```

Latest quality status:

```sql
SELECT *
FROM risk_platform.latest_data_quality_status;
```

Raw to curated counts:

```sql
SELECT 'raw' AS table_name, COUNT(*) FROM risk_platform.market_events_raw
UNION ALL
SELECT 'returns_1m', COUNT(*) FROM risk_platform.returns_1m
UNION ALL
SELECT 'volatility_5m', COUNT(*) FROM risk_platform.volatility_5m
UNION ALL
SELECT 'risk_summary', COUNT(*) FROM risk_platform.risk_summary
UNION ALL
SELECT 'data_quality_metrics', COUNT(*) FROM risk_platform.data_quality_metrics;
```

## Interview Explanation

Use this version:

> I made the source-to-warehouse consistency explicit. The source has 7 records,
> including one duplicate business event and one late event. The pipeline
> deduplicates to 6 raw events, produces 9 curated records, loads those into
> PostgreSQL with idempotent upserts, and the consistency SQL checks that source
> counts, raw counts, curated counts, and data quality metrics agree.

The key point:

```text
MongoDB source shape is not the same thing as PostgreSQL serving shape.
The pipeline owns the contract between them.
```

## AWS Follow-On

The same pattern can be moved to AWS:

```text
Amazon DocumentDB
  -> extract or connector job
  -> validation and flattening
  -> RDS PostgreSQL or Aurora PostgreSQL
  -> consistency checks
```

See `docs/aws-managed-databases.md` for the disabled-by-default Terraform
scaffold.
