# Validate daily-risk parameters before storage access

Primary arc42 block: `orchestration`. The interface crossing into `analytics`
has one end-to-end contract: an invalid numerical request must not call the
configuration loader, raw-data reader or curated writer.

The runner previously loaded configuration and raw history before the builder
rejected an invalid volatility window, VaR window or confidence. For example,
`--vol-window 1` is a positive integer accepted by argument parsing, but violates
the analytical minimum of two observations. That failure did not require a file
scan and could be masked by an unrelated storage failure.

`validate_daily_risk_parameters` composes the existing analytics validators.
Both the runner and builder call it. No numerical rule is duplicated: the
volatility window remains an integer in [2, 252], the VaR window in [2, 2520],
and confidence remains finite and strictly between zero and one. Boolean and
unsupported values remain rejected with the existing diagnostics. The runner
normalizes confidence before I/O and passes that ordinary float onward, so
custom numeric conversion cannot change the request between reading and output.
The builder still validates independently for callers that bypass the runner.

Invalid numerical requests now take precedence over storage failures. CLI
semantic failures keep exit code 1 and the existing redacted diagnostic; parser
usage handling is unchanged. Defaults, symbol rules, mathematical methods,
model identifiers, valid records and publication ordering are unchanged. Valid
requests still perform the existing configuration, raw-read and writer actions.
This validation does not establish source availability or statistical adequacy.

```bash
python -m pytest -q tests/unit/test_daily_risk_preflight.py
make quality-check
make security-check
make readiness-check
```

Tests retain the real runner, builder and event schema, with explicit reader,
configuration-loader and writer spies. Both injected and default callback paths
must show no attempted I/O for invalid parameters. Actual CLI tests check
rejection before storage access and summary creation. Positive cases compare
all emitted records to the real builder and verify the original I/O order.
These are injected-boundary tests, not real Parquet/warehouse publication proofs.

The local development harness substitutes unused provider/writer import modules
because the full checkout is unavailable. Those substitutes are not committed;
full-repository CI must exercise the tests with normal imports. No dependency,
schema, workflow, activation, provider request or deployment change is involved.

## Calendar-date preflight

The builder and runner share `validate_daily_risk_dates`. Optional bounds must
be built-in `datetime.date` values, not `datetime`, text, booleans or custom date
subclasses. No implicit parsing, timezone conversion or subclass comparison is
performed. Invalid dates now raise the fixed public `ValidationError` before
the builder consumes its iterable or the runner invokes storage callbacks.
Reversed-range diagnostics and inclusive calendar-date selection are preserved.

The direct builder still accepts omitted bounds and `date.min` / `date.max`.
The runner requires an end date earlier than `date.max`: its existing raw
reader constructs an exclusive next-day boundary. That existing reader limit
is now checked before configuration or raw-file access instead of afterward.
The low-level reader date conversion is unchanged.

This tightens direct-Python-API input types and makes invalid-date diagnostics
take precedence over missing data or storage failures. CLI text parsing still
produces ordinary calendar dates; its semantic rejection remains exit 1 with
the existing diagnostic and no summary creation. Valid dates, leap-day
selection, retained calculation history, model identifiers and data are not
changed. Tests use the real builder and runner with I/O spies, not a real
Parquet/warehouse publication.

```bash
python -m pytest -q tests/unit/test_daily_risk_dates.py
```


## Preserve raw volume values until validation

The raw daily reader now passes volume values unchanged to MarketEvent integer
validation instead of calling int() first. Truncating a value such as -0.5 to 0
or 1.5 to 1 destroyed evidence of an invalid observation before the schema could
reject it. Boolean raw volumes are explicitly rejected as in the raw-writer
contract; they are not treated as zero/one observations.

Fractional floats/Decimals and booleans now produce the existing fixed
StorageError (`Raw Alpha Vantage daily records are incompatible`) before any
curated publication. Integral floats/Decimals and ordinary integer volumes still
normalize to integers. No shared schema, mathematical method, event identity,
raw-file bytes or successful publication ordering changes. Existing schema
coercions, such as integer text, remain supported; this is not a new strict
physical-Parquet schema. Other field conversions are unchanged.

Run the two focused suites:

```bash
python -m pytest -q tests/unit/test_daily_raw_volume.py \
  tests/integration/test_daily_raw_volume_validation.py
```

The unit suite supplies query-result rows and checks the actual reader/runner,
public errors, input immutability and no writer calls. It replaces query and
provider-ID boundaries and does not claim actual Parquet decoding. The integration
suite deliberately serializes malformed upstream volumes through the generic
Parquet utility, bypassing the canonical raw-event writer. It exercises normal
reader/runner imports and verifies unchanged raw bytes and absent curated output.
Integral physical representations are positive controls. Run the latter suite in
an environment with the existing DuckDB dependency; it must not be skipped as a
substitute for validation.

Pydantic documents exact-integer conversion for float and Decimal inputs:
https://pydantic.dev/docs/validation/latest/concepts/conversion_table/
