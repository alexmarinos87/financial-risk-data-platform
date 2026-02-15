# Data Model

## Contract

`market_event` is defined in `config/data_contracts.yaml` with:

1. `version` (currently `v1`)
2. `compatibility` strategy (`backward`)
3. required fields
4. logical schema types

## Bronze and Silver Schema

The base event shape:

```json
{
  "event_id": "uuid",
  "symbol": "AAPL",
  "price": 189.32,
  "volume": 1200,
  "ts_event": "2025-01-20T10:01:00Z",
  "ts_ingest": "2025-01-20T10:01:03Z",
  "source": "stooq",
  "contract_version": "v1"
}
```

Silver adds:

1. deduplication by `event_id`
2. normalized `symbol`
3. `window_start` for time-window analytics

## Gold Outputs

1. `risk_summary`
2. `data_quality_metrics`

Both gold datasets are written as partitioned Parquet and include `run_id` and `ts_run`.
