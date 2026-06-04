# ELT Connector Mapping

This note explains how the project maps to an ELT connector workflow such as
Airbyte, Fivetran, or Stitch.

## Current Project Shape

The local pipeline owns these responsibilities:

1. Load source payloads from JSON fixtures or local producer output.
2. Validate required fields and reject invalid records.
3. Normalise symbols and timestamps.
4. Deduplicate by stable event identifiers.
5. Write immutable raw records.
6. Produce curated outputs for downstream consumption.
7. Emit data quality evidence for each run.
8. Support deterministic backfills from raw partitions.

## Connector-Based Production Shape

In a connector-based deployment, the extraction boundary moves earlier in the
flow:

```text
source system
  -> ELT connector
  -> raw landing tables or raw object storage
  -> validation and normalisation
  -> curated warehouse tables
  -> ops-facing SQL and dashboards
```

The connector would own:

1. Source authentication.
2. Incremental extraction.
3. Source cursor state.
4. Initial raw landing.
5. Basic sync monitoring.

This project would still own:

1. The downstream data contract.
2. Business-level validation.
3. Deduplication and replay safety.
4. Curated transformation logic.
5. Data quality metrics.
6. Backfill and recovery behaviour.
7. Ops-facing warehouse outputs.

## Airbyte-Style Flow

An Airbyte-style setup would use a source connector to land source records into
raw tables or S3. The pipeline would then read the landed data and apply the
same deterministic processing path used locally.

Example mapping:

| Project concept | ELT connector equivalent |
| --- | --- |
| `tests/fixtures/demo_events.json` | Raw records landed by a connector sync |
| `MarketEvent` schema validation | Post-sync contract validation |
| `dedupe_events` | Downstream deduplication by source event key |
| Partitioned parquet writes | Raw or curated object-storage landing zone |
| `risk_platform.*` tables | Warehouse tables for operational consumers |
| Backfill replay | Historical sync plus deterministic transformation replay |

## Warehouse Loading

For PostgreSQL, the recommended pattern is:

1. Load each batch into a temporary or staging table.
2. Validate row counts and key fields.
3. Use `INSERT ... ON CONFLICT` into curated tables.
4. Record load metrics and failures.
5. Keep raw records available for replay.

See:

```text
sql/postgres_schema.sql
sql/ops_queries.sql
```

## Trade-Offs

Using a connector reduces bespoke extraction code but does not remove the need
for downstream engineering. Business rules, schema drift handling, replay
safety, and data quality checks still need to be owned close to the warehouse
contract.

The project deliberately keeps those concerns explicit so they can be tested and
discussed independently from the extraction tool.
