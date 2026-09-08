# Event identity during batch deduplication

Primary arc42 building block: `processing`.

`src/processing/deduplicator.py` collapses equal records under an event identity,
not arbitrary records that happen to reuse an ID. The first equal occurrence
and the input order are preserved. The function does not mutate its inputs.

The pipeline normalises symbols and timestamps before deduplication. Equivalent
normalised records still collapse, including timestamps expressed with different
UTC offsets. Every supplied normalised field participates in equality. Reusing
an ID with a changed price, symbol, source, ingestion timestamp, or other field
raises `ValidationError` instead of silently selecting whichever arrived first.
A correction therefore needs a distinct event identity; this helper does not
implement correction precedence or last-write-wins semantics.

A missing, null or unhashable identity is rejected. The existing custom-key
argument and hashable non-string identities remain supported. Errors identify
only the input index, never the complete ID or payload.

This is a batch-local check, not a cross-run identity registry. The durable raw
writer still owns cross-run conflict and replay checks. Rejecting a conflict
before the pipeline acquires partition locks or writes raw/curated output avoids
retaining analytics based on a silently discarded contradictory observation.

Run the focused regression checks with:

```bash
python -m pytest -q tests/unit/test_deduplicator.py \
  tests/integration/test_pipeline_duplicate_identity.py
```

The integration check uses temporary inputs and forbids writer/lock calls. It
also verifies that no Parquet file is produced for a conflicting batch. No live
provider, database, webhook or infrastructure activation is involved.
