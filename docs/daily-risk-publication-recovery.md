# Daily-risk publication and replay evidence

Primary arc42 block: `orchestration`, with an end-to-end contract across the
existing analytical builder and local Parquet writer. The Parquet scenarios add
evidence; the summary section documents isolated staging in `_write_summary`.

```bash
python -m pytest -q tests/integration/test_daily_risk_publication_recovery.py
```

## Reject invalid calculations without publishing

Four cases land actual raw Parquet containing finite positive prices whose
returns or warmed-up volatility overflow. They run with both empty and already
populated curated storage. The real reader, builder, writer configuration and
CLI are used: rejection must preserve every raw/curated file byte and an existing
summary file, with no success output. Earlier valid history remains replayable
by choosing its end date; later invalid raw observations are not deleted.

## Recover after publication but before durability confirmation

Three cases fail directory fsync after the first, fourth or eighth curated file
has been linked. The write raises even though its complete final file exists.
Only this durability-confirmation boundary is injected; raw reads, serialization,
validation, hard links and subsequent replay use the real implementations.

A retry must recognize already-published rows, write only the missing rows and
report correct per-dataset counts. Reading every resulting Parquet row must
match the analytical builder, with exactly one occurrence of each calculation
ID. Timestamp fields are read as exact epoch microseconds and reconstructed as
aware UTC datetimes, matching the reader without optional decoding dependencies.
Published file bytes and raw history remain unchanged; another full replay
writes zero rows. The eighth-file case covers a failed acknowledgement after
all expected rows have become visible.

These tests require DuckDB and the repository's normal test dependencies. They
prove local replay convergence under the injected failure, not atomic publication
across datasets, power-loss durability without successful fsync, concurrent
writer safety, a cloud deployment or a PostgreSQL transaction. Consumers can
still observe partial output before recovery; deterministic replay is not rollback.


## Isolated summary-file publication

The CLI's JSON summary is staged inside a uniquely created private directory
beside the destination and then replaced with one filesystem rename. Two
writers no longer share `<destination>.tmp`. An unrelated regular file or
symlink at that old predictable name is neither written through nor removed.
Concurrent successful calls publish complete JSON; the last replacement wins.
There is no ordering guarantee that the newest business run wins.

Formatting, destination naming, and successful return behaviour are unchanged.
The staging directory is cleaned during normal exception unwinding, including
serialization errors and interruptions. Parent-directory errors now use the
same fixed StorageError as other summary filesystem failures. Replacing a
symlink at the requested destination replaces the entry, not its target.

This only isolates summary staging. Parent directories remain trusted, and
summary publication is not transactional with the curated datasets. No fsync
or power-loss durability guarantee is added. Process termination can leave an
orphan staging directory, and a cleanup failure after replacement may report
failure even though a complete summary is visible. No automatic sweep is added.
Existing temporary files are not migrated or deleted by this change.

Run `python -m pytest -q tests/unit/test_daily_risk_summary_publication.py`.
Tests use real temporary files, a controlled two-writer interleaving, existing
symlinks, and operation-boundary failures. They assert unchanged summary bytes
before publication failure, cleanup of owned staging, preservation of unrelated
files, and retained JSON formatting. No provider or database is contacted.

Python's private temporary-directory and replacement contracts:
https://docs.python.org/3/library/tempfile.html#tempfile.TemporaryDirectory
https://docs.python.org/3/library/os.html#os.replace
