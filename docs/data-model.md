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
