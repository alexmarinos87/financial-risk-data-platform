# PostgreSQL And MongoDB Walkthrough

This note makes the source and warehouse roles explicit.

## Mental Model

MongoDB is the upstream operational source shape:

1. Documents are nested and application-oriented.
2. A single document can hold related fields that would become multiple
   warehouse columns.
3. Incremental extraction usually depends on a stable `_id` and an update
   timestamp.
4. The source shape is useful for the application, but not ideal for ops SQL.

PostgreSQL is the warehouse-serving shape:

1. Data is flattened into tables with clear columns.
2. Primary keys and `ON CONFLICT` make loads idempotent.
3. Indexes and views make common ops questions fast and repeatable.
4. SCD Type 2 dimensions preserve reporting history when reference data changes.
5. SQL consumers should not need to understand raw nested source payloads.

The pipeline sits between them:

```text
MongoDB-style source documents
  -> connector or extract job
  -> raw landing
  -> validation and flattening
  -> deduplication
  -> curated PostgreSQL tables
  -> reporting dimensions, ops queries, and dashboards
```

## Optional Local Playground

Start PostgreSQL and MongoDB locally:

```bash
make local-db-up
```

Stop and remove local containers and volumes:

```bash
make local-db-down
```

PostgreSQL is exposed on local port `5433`.
MongoDB is exposed on local port `27018`.

Credentials for the local PostgreSQL container:

```text
database: risk_platform
username: risk_user
password: risk_password
```

These credentials are only for the local Docker playground.

## Inspect MongoDB Source Documents

Open a MongoDB shell:

```bash
make mongo-shell
```

List source collections:

```javascript
show collections
```

Inspect nested source documents:

```javascript
db.background_checks.find({}, { _id: 1, candidateId: 1, check: 1, timestamps: 1 }).pretty()
```

The important thing to notice is the nested shape:

```text
check.id
check.type
check.status
timestamps.createdAt
timestamps.updatedAt
timestamps.completedAt
```

Flatten a nested document into a warehouse-ready shape:

```javascript
db.background_checks.aggregate([
  {
    $project: {
      _id: 0,
      source_document_id: { $toString: "$_id" },
      candidate_id: "$candidateId",
      check_id: "$check.id",
      check_type: "$check.type",
      check_status: "$check.status",
      employer_name: "$employer.name",
      employer_country: "$employer.country",
      ts_created: "$timestamps.createdAt",
      ts_updated: "$timestamps.updatedAt",
      ts_completed: "$timestamps.completedAt",
      source: "$source"
    }
  }
])
```

Inspect the market-event source shape:

```javascript
db.market_events_source.find({}, { _id: 0 }).pretty()
```

Find duplicate business events in MongoDB:

```javascript
db.market_events_source.aggregate([
  { $group: { _id: "$eventId", count: { $sum: 1 } } },
  { $match: { count: { $gt: 1 } } }
])
```

This is the source-side version of the deduplication problem handled by the
pipeline.

Flatten source events into the pipeline input shape:

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

## Inspect PostgreSQL Warehouse Tables

Open a PostgreSQL shell:

```bash
make postgres-shell
```

List schemas and tables:

```sql
\dn
\dt risk_platform.*
\dt staging.*
```

Inspect raw landed events:

```sql
SELECT event_id, symbol, price, volume, ts_event, ts_ingest, source
FROM risk_platform.market_events_raw
ORDER BY ts_event, event_id;
```

Inspect the latest quality status:

```sql
SELECT *
FROM risk_platform.latest_data_quality_status;
```

Inspect latest curated summaries:

```sql
SELECT *
FROM risk_platform.latest_risk_summary
ORDER BY symbol;
```

Inspect the current reporting dimension:

```sql
SELECT
    symbol,
    source,
    asset_class,
    reporting_currency,
    sector,
    effective_from
FROM risk_platform.current_symbol_dimension
ORDER BY symbol, source;
```

Inspect the finance reporting view:

```sql
SELECT
    symbol,
    asset_class,
    reporting_currency,
    sector,
    metric_ts,
    volatility_status,
    late_status,
    duplicate_status
FROM risk_platform.finance_risk_semantic_model
ORDER BY symbol;
```

Review the SCD Type 2 history:

```sql
SELECT
    symbol,
    source,
    sector,
    effective_from,
    effective_to,
    is_current,
    change_reason
FROM risk_platform.symbol_dimension_history
ORDER BY symbol, effective_from;
```

Check warehouse freshness:

```sql
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
```

## See Idempotent Loading In PostgreSQL

The staging table includes an existing event and a new event:

```sql
SELECT *
FROM staging.market_events_raw_batch
ORDER BY event_id;
```

Load from staging into the raw table:

```sql
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
```

Then inspect the result:

```sql
SELECT event_id, symbol, price, volume, ts_ingest
FROM risk_platform.market_events_raw
ORDER BY event_id;
```

What to notice:

1. Existing `evt-6` is not duplicated.
2. New `evt-7` is inserted.
3. Symbols are normalised to uppercase during the load.
4. The primary key is the guardrail that makes the load repeatable.

## How This Maps To The Interview

Use this wording:

> MongoDB is where I expect nested source documents and update cursors. I would
> preserve the source id, flatten into a raw contract, validate timestamps and
> required fields, then load curated PostgreSQL tables with primary keys and
> idempotent `ON CONFLICT` logic.

The important distinction:

```text
MongoDB: source shape, nested, application-friendly, cursor-driven extraction
PostgreSQL: serving shape, relational, query-friendly, constrained tables
Pipeline: contract enforcement, quality checks, deduplication, replay
```

For the full source-to-warehouse consistency loop, see
`docs/data-consistency-walkthrough.md`.
