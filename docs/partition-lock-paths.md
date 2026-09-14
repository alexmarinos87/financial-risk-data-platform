# Validate partition names before lock acquisition

Primary arc42 block: `orchestration`. Follow-up within #236 / goal #235.

Partition names are relative, slash-separated paths under the configured lock
root. The acquisition API now checks the entire request before creating any
directory, opening a lock or considering stale replacement. It rejects absolute
paths, Windows drive paths, backslashes, empty/dot/parent segments, control
characters and non-string entries. It rejects unsafe syntax rather than
normalizing it into a different partition identity.

Previously, an absolute partition replaced the base path during path joining,
and `../../outside` traversed out of `.orchestration_locks`. A stale lock at the
escaped location could be removed and replaced. Regression tests demonstrate
this only inside isolated temporary directories and verify that the existing
outside lock is now untouched.

Valid relative names, including the existing year/month/day/hour layout, retain
their payload, sorted/deduplicated results, overlap blocking and release API.
An empty request with valid options remains a no-op. Invalid names now raise a fixed
`ValidationError` before any filesystem access, rather than an OS error or a
partially attempted acquisition. Absolute and normalized-alias names that once
worked incidentally are deliberately no longer supported.

This is lexical validation, not a symlink-safe filesystem sandbox. The base and
its directories must remain trusted; symlinks, concurrent directory replacement,
stale-owner races, fencing and crash recovery are not addressed. The release
API still assumes the caller supplies trusted paths obtained from acquisition.
No existing lock is swept, renamed or migrated, and stale thresholds are unchanged.

```bash
python -m pytest -q tests/unit/test_locks.py \
  tests/unit/test_partition_lock_rollback.py tests/unit/test_partition_lock_release.py \
  tests/unit/test_partition_lock_paths.py
```

The Python path-joining contract is documented at:
https://docs.python.org/3/library/pathlib.html#operators

## Stale-timeout validation

The acquisition API also validates `stale_after_seconds` before any filesystem
operation, even for empty or uncontended requests. `None` still disables
age-based takeover. Otherwise the value must be a positive built-in integer
that fits Python's `timedelta` range. Zero, negatives, booleans, fractional or
text values and overflowing durations raise a fixed `ValidationError`. This
intentionally tightens the direct API; zero is not a disable or force flag.

Previously, a negative timeout could immediately replace a fresh owner's lock.
The regression tests reproduce this with real isolated temporary files, verify
that invalid requests preserve the owner's bytes, and assert no attempted I/O
for invalid options. Positive durations retain the existing age comparison;
no default, lease threshold, retry or stale-recovery algorithm is changed.

This does not make age-based takeover a fenced lease or prove the owner is dead.
Higher-level runners can perform their earlier reads before calling this API.

```bash
python -m pytest -q tests/unit/test_partition_lock_timeout.py
```
