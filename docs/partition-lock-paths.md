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
its directories must remain trusted; parent symlinks, concurrent directory replacement,
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


## Bounded stale-metadata inspection

Before deciding that an existing lock is old enough to replace, the inspector
opens its final path component with nonblocking and no-follow flags, then checks
the descriptor is a regular file. It reads at most 65,537 bytes and only parses
UTF-8 metadata of at most 65,536 bytes. Size is checked before reading and again
before JSON parsing to cover file growth. Descriptors close on every exit path.
Platforms lacking either safe-open flag conservatively disable stale takeover;
normal uncontended acquisition and explicit release remain available.

A FIFO, directory, symbolic link, oversized body, invalid UTF-8/JSON or decoder
depth failure does not establish staleness. The acquisition remains blocked with
OverlapError, and rollback still removes earlier locks acquired by the attempt.
The existing entry is not deleted, repaired or followed to establish an old age.
Normal small regular-file metadata retains the current timestamp/timeout policy,
including legacy naive timestamps interpreted as UTC. Oversized legacy payloads
can no longer be reclaimed automatically; inspect ownership before intervention.

This is not a complete filesystem sandbox: parent directories remain trusted,
and replacement between inspection and unlink is still a race. Nonblocking open
avoids waiting for a FIFO writer, not arbitrary stalls on an unhealthy filesystem.
No fencing, stale-owner liveness proof, crash recovery or metadata migration is
provided. Release semantics and the acquisition payload are unchanged.

Run `python -m pytest -q tests/unit/test_partition_lock_metadata.py` alongside
the existing lock and process-recovery suites. The tests cover exact size edges,
real symlinks/FIFOs, rollback, malformed metadata, descriptor cleanup and missing
safe-open primitives. The FIFO probe runs in a child with a five-second timeout.

Python documents the resource risk of unbounded JSON decoding:
https://docs.python.org/3.12/library/json.html


## Short reads do not establish complete metadata

The stale inspector accumulates bytes until EOF or the existing 65,537-byte
read budget is exhausted. One short read is not proof that the metadata is
complete: its first fragment can be valid JSON even when the remaining file
is malformed or oversized. A later read error also leaves the lock blocked.
UTF-8 decoding and JSON parsing happen only after the bounded read completes.

The byte limit, timestamp/timeout policy, safe-open flags, descriptor cleanup,
and acquisition/release APIs are unchanged. This can recover valid metadata
split across reads, including multibyte UTF-8, without treating a partial JSON
prefix as permission to remove an existing lock. The additional EOF read is an
intentional cost. An unhealthy filesystem can still stall a regular-file read;
this is not a time limit or a consistent snapshot under concurrent modification.

Run `python -m pytest -q tests/unit/test_partition_lock_short_reads.py` alongside
the existing lock suites. The tests constrain actual descriptor reads to short
chunks and exercise a trailing invalid document, post-prefix read error, growth
after stat, exact limits, rollback, and descriptor closure. No production file
is read or removed. The tests simulate legal short reads; they do not claim to
have observed this timing on a particular filesystem in production.

Python's low-level read contract:
https://docs.python.org/3/library/os.html#os.read


## Ambiguous JSON metadata remains blocked

Stale inspection rejects repeated field names within any JSON object, including
identical values and names that become equal after JSON escape decoding. The
normal JSON decoder otherwise keeps only the last value, so an old timestamp
could conceal a conflicting future timestamp and authorize replacement.
`_unique_metadata_object` rejects the ambiguity before it is discarded. The
existing metadata-error path then keeps the lock blocked with `OverlapError`;
the file is not repaired or removed, and earlier acquisitions are rolled back.

Repeated names in separate nested objects are allowed. Valid legacy UTC, offset
and naive timestamps retain their existing interpretation. Normal lock payloads,
size limits, bounded reads, acquisition/release behaviour and timeout policy are
unchanged. A legacy file with duplicate keys now needs ownership investigation,
not automatic reclamation. This is stricter parsing, not proof that a lock owner
is dead or protection from the existing inspect-to-unlink race.

```bash
python -m pytest -q tests/unit/test_partition_lock_duplicate_metadata.py
```

The regressions exercise the public acquisition API with real temporary files,
conflicting/identical/escaped keys, nested ambiguity, rollback and positive
legacy controls. Python documents last-value handling and `object_pairs_hook`:
https://docs.python.org/3.11/library/json.html#repeated-names-within-an-object
