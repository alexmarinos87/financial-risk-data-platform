# Managed Worker Summary Output Safety

Primary arc42 block: `orchestration`.

## Decision

The managed-worker planner delegates summary writes to
`src/orchestration/notification_worker_summary.py`. The CLI and its
`--summary-json` option are unchanged; see
[the worker-plan walkthrough](managed-notification-worker-plan.md).

The earlier writer opened a predictable `<summary>.tmp` path. Checking only the
final destination did not protect an unrelated file targeted by a symlink at
that temporary path. The replacement exclusively creates an unpredictable
sibling with restrictive permissions instead. It never opens or removes the
legacy temporary path.

## Guarantees

JSON is serialized with non-finite values rejected. The encoded document,
including formatting and its final newline, must be at most 1,048,576 bytes.
Serialization and size validation finish before directory creation or writing.

Existing final destinations must be regular files, not directories or symbolic
links, including dangling links. The destination is checked again immediately
before replacement. The temporary file is flushed and fsynced before an atomic
same-directory replacement publishes the complete document. On POSIX, the new
file has owner-only permissions supplied by the temporary-file facility.

A failure before replacement leaves existing destination bytes unchanged.
Temporary-file cleanup is attempted on every exit; a filesystem failure that
also prevents cleanup can require operator removal. Error messages do not
include document content.

## Trust Boundary And Trade-offs

The parent directory must be operator-owned and trusted. This is not protection
against another process that can replace directory entries or parent paths.
The final rename does not follow a destination symlink, but the checks do not
establish a hostile-filesystem sandbox.

Concurrent writers publish complete documents with last-replacement-wins
semantics. This is not compare-and-swap, a lock, an append-only ledger or a
power-loss durability guarantee for the parent directory. The writer does not
validate the business meaning or secrecy of arbitrary caller-supplied JSON;
callers remain responsible for using the credential-free plan contract.

## Regression Evidence

```bash
.venv/bin/python -m pytest -q tests/unit/test_notification_worker_summary.py
make quality-check
make security-check
make readiness-check
```

The tests cover both the shared writer and the existing planner wrapper:
legacy temporary symlink preservation, existing and dangling destination
symlinks, non-regular destinations, invalid JSON, oversized output, the exact
byte limit, successful replacement, replacement failure and directory-creation
failure. No scheduler, external request, database write or cloud operation is
part of this change.
