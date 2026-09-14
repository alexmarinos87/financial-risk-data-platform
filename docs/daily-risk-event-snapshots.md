# Revalidate and snapshot daily event instances

Primary arc42 block: `analytics`. Follow-up within #232 / goal #225.

An existing `MarketEvent` object is not evidence that its current fields remain
valid. Assignment, `model_copy(update=...)` and `model_construct` can bypass
validation. The shared model does not opt into instance revalidation, so passing
such objects to `MarketEvent.model_validate` could retain invalid or mutable state.

The daily builder now takes a raw field mapping from each model instance and
validates it into a fresh canonical event. Mappings continue through the existing
schema path. The global schema/configuration is unchanged: this boundary enforces
the same field rules and normalizations for both supported input representations.
Invalid schema data raises the existing fixed public `ValidationError` without
including field values in its message or ordinary exception chain.

Revalidation also restores UTC normalization for valid offset timestamps in
unvalidated copies. Normal valid inputs retain their metrics, model version,
identifiers, date selection and ordering. Schema coercions still apply; this is
not a new strict-typing policy. Caller-owned objects are not mutated or frozen.

Each fresh event is retained before asking the iterable for another item. A
producer may therefore reuse one mutable event object without changing earlier
observations already accepted by the builder. The accepted value is the state
at consumption, not a later mutation. This is a per-observation snapshot, not
an atomic snapshot of an entire external source or a thread-safety guarantee.
It adds one schema validation and model allocation per instance input.

```bash
python -m pytest -q tests/unit/test_daily_risk_event_snapshots.py \
  tests/unit/test_daily_risk_finite_outputs.py \
  tests/unit/test_daily_risk_confidence_conversion.py
```

Tests compare model/mapping rejection, exercise mutation/copy/construction,
verify timezone normalization, reusable iterables, later mutation, replay and
input preservation. These are pure-builder tests, not a new real-Parquet or
warehouse-publication proof. Existing runner/publication regressions still run
in the full repository CI; no runner, writer or schema implementation changes.

Pydantic documents the model-instance validation shortcut and construction APIs:
https://docs.pydantic.dev/latest/concepts/models/#validating-data
