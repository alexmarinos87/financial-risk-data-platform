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


## Bound collection before sorting raw file paths

`_raw_parquet_files` consumes at most `MAX_RAW_FILES + 1` matching paths from
`Path.rglob` before sorting. With the existing 2,048-file policy, match 2,049
proves the request is too large and raises the existing file-limit StorageError.
The previous implementation exhausted and sorted every match before applying
that check. The limit now bounds this function's collected path list, rather
than merely rejecting after the allocation and enumeration have happened.

This is rejection, not truncation: no partial inventory is returned to DuckDB.
Accepted inventories keep their complete sorted order and existing byte and
file-type checks. The limit, public diagnostics, schemas, mathematical methods
and publication policy are unchanged. A failure encountered only after the first
over-limit match is no longer observed; the known limit failure takes precedence.

```bash
python -m pytest -q tests/unit/test_daily_raw_inventory_bound.py
```

Tests measure iterator consumption at a small limit and the real 2,048 limit,
place a failing tail after known excess, exercise the real runner's abort path,
and retain sorted-order, actual directory, empty, byte-limit and unsafe-entry
controls. These are inventory tests; fixture bytes are not decoded as Parquet.

This does not cap the number of nonmatching directories visited, memory used
inside the underlying directory enumerator, or filesystem-operation duration.
It does not add snapshot consistency or change `rglob`'s error/symlink semantics.
Those are separate concerns, not guarantees supplied by a bounded result list.

## Bound returned rows independently of the earlier count

The raw reader retains its early `COUNT(*)` rejection, but the ordered selection
now also binds `LIMIT MAX_RAW_ROWS + 1`. It checks the actual result length before
constructing event objects. The extra row proves that a request exceeds the
existing 100,000-row ceiling; it is never returned as a truncated analytical
history. Both rejection paths use the same fixed `StorageError`.

Counting and selecting are separate observations of external files. An earlier
small count must not authorize a later unlimited fetch when a file is replaced
between those operations. This does not imply that the canonical immutable raw
writer normally replaces files. Valid results remain complete, filtered by the
same source/symbol/end-date conditions and ordered by timestamp and event ID.
Changes that remain within the ceiling are accepted as before; the count is not
a source snapshot or consistency token.

```bash
python -m pytest -q tests/unit/test_daily_raw_row_budget.py \
  tests/integration/test_daily_raw_row_budget_parquet.py
```

Unit tests exercise changed counts, exact limits, rejection before event
construction/publication, connection cleanup and the real configured limit.
The integration scenarios use actual Parquet and DuckDB, replacing a test-owned
input only after its real count completes. They verify a bounded subsequent
fetch, rejection before curated writes and complete results at the ceiling.
The deliberately injected replacement is not a simulated database response.

This limits rows transferred to Python, not DuckDB's internal scan/sort work,
query duration, row width or filesystem mutation. Existing file and byte caps
remain separate controls. No transaction or cross-dataset atomicity is added.
DuckDB documents LIMIT as an output modifier:
https://duckdb.org/docs/stable/sql/query_syntax/limit.html
