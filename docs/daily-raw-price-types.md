# Reject physical Boolean closing prices

Primary arc42 block: `orchestration`; raw-to-daily analytical validation.

The daily raw reader checks both price and volume for Boolean values before
numeric conversion. `float(True)` produces 1.0, which is finite and positive
and can otherwise survive the analytical price checks. A Boolean upstream
Parquet column is not evidence of a numeric closing price. `False` is rejected
at the same raw-input boundary rather than becoming zero first.

Rejection uses the existing fixed StorageError: `Raw Alpha Vantage daily records
are incompatible`. It occurs before the builder or curated writer is called.
The earlier Boolean-volume check and non-Boolean conversion policies remain:
integers, floats, Decimal values and supported numeric text retain their prior
conversion. This does not change the shared MarketEvent schema, accepted risk
parameters, mathematical methods or model identifiers.

```bash
python -m pytest -q tests/unit/test_daily_raw_price_types.py \
  tests/integration/test_daily_raw_boolean_prices.py
```

Unit tests preserve physical scalar types across a substituted database boundary
and assert no builder/writer invocation on rejection. Integration tests serialize
real Boolean Parquet columns without passing through the canonical event writer,
then exercise the normal daily runner. Invalid input leaves raw bytes unchanged
and creates no curated output. Numeric controls publish and replay without
additional rows or modified file bytes.

The reader can reject only types retained by its query result. If an upstream
writer or a mixed-schema SQL union has already converted a Boolean to a numeric
value, that distinction is lost; this is not complete source-schema enforcement.
No data is repaired or removed, and no provider or cloud operation is added.
