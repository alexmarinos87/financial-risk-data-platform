# Backfill windows follow UTC storage partitions

Primary arc42 building block: `orchestration`. Backlog item 5: backfill and
idempotency. This change is independent of the pending lock and event-window PRs.

Convert request timestamps to UTC before flooring the start to its hour.
Previously, local-hour flooring with a fractional UTC offset could select the
wrong partition and omit the final requested hour. For example, a request from
`2026-01-01T05:45:00+05:45` through `2026-01-01T06:45:00+05:45` denotes UTC hours
00 and 01. The old loop instead visited the previous day's hour 23 and hour 00.

The runner still processes complete hourly partitions, inclusively through the
end instant; this is not row-level timestamp filtering. Naive inputs retain the
existing UTC interpretation. Offset-bearing inputs now produce canonical UTC
window summaries, replay filenames and new checkpoints. UTC inputs are unchanged.
UTC conversions outside datetime's range raise ValueError before storage access.

Saved progress is also normalized to the containing UTC hour before adding the
next hourly step. Legacy checkpoints created at a local-hour boundary may have
nonzero UTC minutes. They identify the containing storage partition, not an
alternative offset-aligned partition layout. The checkpoint schema and success /
blocked-overlap / skipped-empty policies remain unchanged. Reading an empty
partition does not rewrite saved progress.

This cannot reconstruct partitions missed by earlier runs. To replay a known
historical range after reviewing it, use the existing `resume=False` Python API
option. No checkpoint migration, automatic replay or stored-data mutation is run
by this change. Existing concurrency and checkpoint-publication limitations
remain; the correction does not add crash recovery or distributed locking.

```bash
python -m pytest -q tests/unit/test_backfill_utc_windows.py \
  tests/integration/test_backfill_utc_parquet.py
```

Unit tests exercise the real runner with recorded partition reads, actual local
checkpoint files and a substituted pipeline callback. Integration tests land
actual raw Parquet, replay both requested hours and check byte-preserving repeat
runs using normal modules. No provider or cloud service is contacted.

Python distinguishes changing calendar fields from converting an instant:
https://docs.python.org/3.11/library/datetime.html#datetime.datetime.astimezone
