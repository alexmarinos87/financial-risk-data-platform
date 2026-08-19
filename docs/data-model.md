# Data Model

This project is a compact market-risk data platform. Its modelling value is not
the financial formula complexity; it is the source-to-serving contract:

```text
market-like source records
  -> validation and normalisation
  -> deduplicated raw event storage
  -> curated analytical outputs
  -> PostgreSQL serving tables and reporting views
  -> reconciliation evidence
```

The most important modelling habit in this repository is to define the grain of
each dataset before discussing columns or code. The grain is the sentence that
states exactly what one row means.

## Raw Event Schema

```json
{
  "event_id": "uuid",
  "symbol": "AAPL",
  "price": 189.32,
  "volume": 1200,
  "ts_event": "2025-01-20T10:01:00Z",
  "ts_ingest": "2025-01-20T10:01:03Z",
  "source": "stooq"
}
```

## External Signal Schema

```json
{
  "signal_id": "uuid",
  "name": "VIX",
  "value": 18.4,
  "ts_event": "2025-01-20T10:01:00Z",
  "ts_ingest": "2025-01-20T10:01:03Z",
  "source": "cboe"
}
```

## Current Warehouse Grains

| Dataset | Grain | Primary or conflict key | Notes |
| --- | --- | --- | --- |
| `market_events_raw` | One validated, normalised, deduplicated market event per stable source event ID. | `event_id` | This is warehouse raw, not byte-for-byte source landing. Duplicate source payloads are counted in data quality metrics but not retained as separate raw rows. |
| `returns_1m` | One computed return per symbol and event timestamp, using the previous observed price for that symbol. | `(symbol, ts_event)` | The implementation assumes ordered events. Despite the name, this is observation-to-observation return in the current code, not guaranteed clock-minute return unless input cadence is one minute. |
| `volatility_5m` | One rolling volatility value per symbol and event timestamp once enough return observations exist. | `(symbol, ts_event)` | The configured rolling window is a count of return observations; `window_start` records the time bucket used for grouping context. |
| `data_quality_metrics` | One quality summary for a pipeline run timestamp. | `UNIQUE (ts_ingest)` | The table has a surrogate `metric_id`, but idempotent loading uses `ts_ingest`. A production version would usually carry an explicit `pipeline_run_id`. |
| `risk_summary` | One risk status row per symbol and metric timestamp. | `(symbol, ts_ingest)` | This is a serving summary. It intentionally denormalises latest volatility, VaR, quality status and external signal context. |
| `external_signal_summary` | One latest signal observation per signal name/source/latest signal ID included in the run. | `(name, source, latest_signal_id)` | This preserves each distinct latest signal ID. If the product requirement were "one current row per signal", the key should be `(name, source)`. |
| `daily_returns` | One versioned close-to-close return for a source, symbol and event date. | `calculation_id` | The ID binds model version and the two immutable source event IDs. Multiple analytical versions for the same business date can coexist. |
| `daily_volatility` | One versioned annualised volatility observation for a source, symbol, event date and configured window. | `calculation_id` | Window and annualisation parameters are stored explicitly as queryable columns. |
| `daily_risk_summary` | One versioned daily close risk snapshot for a source, symbol, event date and full model parameter set. | `calculation_id` | The base table preserves every calculation. `latest_daily_risk_summary` selects one current row per parameterised grain. |
| `symbol_dimension_history` | One version of symbol reference attributes for a symbol/source over an effective time interval. | `(symbol, source, effective_from)` plus one-current-row index | This is the repository's SCD Type 2 example. It preserves attribute history such as sector changes. |

## Source, Raw, Curated, Serving

The same business object appears differently at each layer:

1. Source records can be nested, duplicated, late or inconsistent.
2. Pipeline records are flattened and validated into a stable contract.
3. Raw parquet records preserve deduplicated event-level facts for replay.
4. Curated parquet records hold derived metrics.
5. PostgreSQL tables expose operational and reporting contracts.

This distinction matters during interviews. A source document shape is not the
same thing as a warehouse serving shape. The pipeline owns the transformation
contract between them.

## Keys And Idempotency

The project uses stable keys to make re-runs safe:

| Table | Idempotency behaviour |
| --- | --- |
| `market_events_raw` | Re-loading the same `event_id` updates the existing warehouse row. |
| `returns_1m` | Re-loading the same `(symbol, ts_event)` updates the computed return. |
| `volatility_5m` | Re-loading the same `(symbol, ts_event)` updates the computed volatility. |
| `data_quality_metrics` | Re-loading the same `ts_ingest` updates the run summary. |
| `risk_summary` | Re-loading the same `(symbol, ts_ingest)` updates the serving summary. |
| `external_signal_summary` | Re-loading the same `(name, source, latest_signal_id)` updates that latest-signal row. |
| `daily_returns` | Re-loading the same `calculation_id` converges on one stored calculation while a changed source history creates a different version key. |
| `daily_volatility` | Re-loading the same `calculation_id` converges; a different window or source history creates another retained row. |
| `daily_risk_summary` | Re-loading the same `calculation_id` converges; the current view ranks versions within an explicit parameterised grain. |

Raw parquet uses `event_id` as the global logical key, matching processing and
the warehouse. Before publication, the local writer inventories existing raw
parquet under a dataset-wide lock. It compares `source`, `symbol`, `price`,
`volume` and UTC `ts_event`, while treating `ts_ingest` as arrival metadata:

1. The same fact with a later `ts_ingest` is a replay. It writes nothing and
   preserves the first accepted timestamp and partition.
2. Changed persisted business data under the same key is a correction conflict.
   The whole write call stops before publishing any new rows.
3. Existing duplicate keys or unreadable/incompatible raw files block further
   publication until an operator repairs or migrates the local dataset.

Inventory requires the seven named columns and compatible physical Parquet
types: strings for IDs/source, double-precision price, integer volume, and
DuckDB microsecond timestamp event/ingest columns. Microsecond-aligned legacy
nanosecond columns are accepted because Python's existing input contract cannot
carry finer precision; compatible timezone-naive timestamps are interpreted as
UTC. Integer, decimal or single-precision physical price columns, coarse or
submicrosecond timestamps, and string-encoded numeric or timestamp columns fail
closed rather than being lossily coerced. UTF-8 field/call/dataset bounds are
checked before publication and before inventory rows enter Python. Local safety
caps also apply prospectively to the Parquet count, filesystem entries,
physical bytes and rows that the next inventory can read. The dataset lock
waits for at most five seconds.

Curated parquet continues to use deterministic content hashes for batch
filenames. All final parquet files are staged and published with atomic
no-overwrite creation on the local filesystem.

## Curated Outputs

1. returns_1m
2. volatility_5m
3. data_quality_metrics
4. risk_summary
5. external_signal_summary
6. daily_returns
7. daily_volatility
8. daily_risk_summary

## Daily Current-Version Contract

The three daily warehouse tables preserve every deterministic calculation ID.
The current view does not collapse parameter changes into one row. Its grain is:

```text
(source, symbol, ts_event, model_version,
 volatility_window, var_window, var_confidence, annualization_days)
```

Within that grain, the greatest `ts_ingest` wins; `calculation_id` is the stable
tie-break. This lets a late historical observation supersede an older serving
version without deleting the earlier analytical evidence.

`daily-risk-v2` makes the volatility window and annualisation basis explicit.
Legacy v1 rows can remain queryable with those newly introduced fields null;
new v2 rows are constrained to populate both.

`daily_risk_semantic_model` joins `current_symbol_dimension` on both `symbol`
and `source`. That source-aware join prevents reference data for one provider
from being applied to another provider's risk calculation.

## Modelling Trade-Offs To Understand

1. `market_events_raw` is deduplicated before write. This makes downstream
   processing simpler, but it means the duplicate raw source row is represented
   as a quality metric rather than a separately stored raw payload.
2. `data_quality_metrics` uses `ts_ingest` as its natural run key. That is fine
   for the demo, but an explicit `pipeline_run_id` would be more robust when two
   runs share the same latest ingest timestamp.
3. The original `risk_summary` semantic view joins the current symbol dimension
   by symbol only. The daily semantic model improves this boundary by joining on
   both source and symbol; migrating the original view remains a separate
   compatibility decision.
4. `symbol_dimension_history` has constraints for valid intervals and one
   current row per symbol/source. It does not currently enforce non-overlapping
   historical intervals in the database, so that should be covered by tests or
   reconciliation SQL if the dimension becomes important.
5. `external_signal_summary` stores latest signal rows with `latest_signal_id`
   in the key. That is useful for retaining a small history of latest
   observations, but it is not a strict current-state table.
6. Raw correction detection covers the persisted `MarketEvent` fields. A
   provider-only change to open, high or low is not visible because the current
   validated raw contract retains close as `price` and does not land the full
   provider response.
7. Raw inventory and locking are deliberately local and linear. A larger or
   distributed implementation would use a table format, manifest or key index
   with transactional coordination rather than scanning parquet under POSIX
   `flock`.
8. Raw first-seen identity is enforced for local parquet publication. The
   PostgreSQL raw loader still has its existing `ON CONFLICT DO UPDATE`
   contract; changing warehouse correction policy requires a separate
   warehouse-owned change and migration decision.
9. Daily warehouse history is append-by-calculation identity, while the current
   view is derived at query time. A larger system might materialise current
   state, record explicit supersession links or use a transactional table format.

## External Signal Summary

`external_signal_summary` stores the latest value for each signal name and source included
in a pipeline run. `risk_summary` rows include the total external signal count and latest
signal context when a signal input is supplied.

## Data Quality Metrics

`data_quality_metrics` includes required-field and null-rate validation evidence for each pipeline run:
It also records value-validity evidence for market fields that must stay in range
(`price > 0` and `volume >= 0`).

```json
{
  "required_fields_checked": 7,
  "missing_required_field_count": 0,
  "missing_required_record_count": 0,
  "missing_required_fields_by_name": "{\"event_id\": 0, \"price\": 0, \"source\": 0, \"symbol\": 0, \"ts_event\": 0, \"ts_ingest\": 0, \"volume\": 0}",
  "required_fields_status": "ok",
  "null_fields_checked": 7,
  "null_field_count": 0,
  "null_record_count": 0,
  "max_null_rate": 0.0,
  "null_fields_by_name": "{\"event_id\": 0, \"price\": 0, \"source\": 0, \"symbol\": 0, \"ts_event\": 0, \"ts_ingest\": 0, \"volume\": 0}",
  "null_rates_by_name": "{\"event_id\": 0.0, \"price\": 0.0, \"source\": 0.0, \"symbol\": 0.0, \"ts_event\": 0.0, \"ts_ingest\": 0.0, \"volume\": 0.0}",
  "null_rate_status": "ok",
  "value_fields_checked": 2,
  "invalid_value_count": 0,
  "invalid_value_record_count": 0,
  "invalid_values_by_name": "{\"price\": 0, \"volume\": 0}",
  "value_validity_status": "ok"
}
```

## Study Loop

Use this repository as an active modelling lab:

1. Define the grain in one sentence.
2. Predict the key and valid row count.
3. Write the SQL or Python that enforces it.
4. Add a test that proves the assumption.
5. Break the assumption with a duplicate, late event, source correction or
   dimension change.
6. Decide whether to fix the model, the transformation, or the documentation.

Good learning questions are concrete:

1. What exactly does one row represent?
2. Which columns uniquely identify that row?
3. What happens when the same source event is replayed?
4. What happens when the source sends a correction?
5. What timestamp drives partitioning, event-time logic and run-time logic?
6. Can a join multiply rows accidentally?
7. Which checks prove source, raw, curated and warehouse counts agree?

## First Implementation Exercise

The first modelling improvement is small and defensible:

```text
Implement symbol reference-data loading for symbol_dimension_history.
```

Implemented scope:

1. Extend `config/symbols.yaml` so each symbol has `source`, `asset_class`,
   `reporting_currency`, `sector` and `effective_from`.
2. Implement `scripts/seed_reference_data.py` so it can generate or apply
   deterministic SCD Type 2 rows.
3. Add tests that prove one current row exists per `(symbol, source)`.
4. Add an overlap check for historical effective intervals.
5. Document why dimensions are separate from event facts.

Generate deterministic SQL without touching a database:

```bash
.venv/bin/python scripts/seed_reference_data.py \
  --config config/symbols.yaml
```

Write the generated SQL to a local demo file:

```bash
.venv/bin/python scripts/seed_reference_data.py \
  --config config/symbols.yaml \
  --output-sql .demo/symbol_dimension_seed.sql
```

Apply it to the local PostgreSQL warehouse:

```bash
.venv/bin/python scripts/seed_reference_data.py \
  --config config/symbols.yaml \
  --apply
```

The learning objective is to understand why reference attributes do not belong
directly inside every event fact when those attributes can change over time.

## Later Provider-Ingestion Exercise

After the reference-data exercise, add a separate ingestion-boundary exercise:

```text
Build a small provider-facing API/webhook/OAuth path.
```

Keep the first version local and provider-neutral:

1. Add an API endpoint that receives provider-style event payloads.
2. Store the raw request body, provider event ID and received timestamp.
3. Verify webhook signatures with a local demo secret.
4. Model OAuth tokens as configuration or local secrets that are never committed.
5. Transform accepted payloads into the existing market-event contract.
6. Add replay and duplicate tests using provider event IDs as idempotency keys.

Only choose a real data provider after checking its current authentication,
rate-limit, webhook and redistribution rules. The portfolio point is not the
specific vendor; it is proving that external ingestion, auth, idempotency and
data contracts are handled deliberately.
