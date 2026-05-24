# Data Model

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

## Curated Outputs

1. returns_1m
2. volatility_5m
3. data_quality_metrics
4. risk_summary

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
