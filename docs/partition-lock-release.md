# Best-effort release of partition lock sets

Primary arc42 block: `orchestration`. Follow-up to acquisition rollback in #236.

`release_partition_locks` attempts every supplied path, in caller order, even
when an earlier unlink fails. Previously, the first failure stopped the loop
and abandoned later locks that could have been released. Successful release
still returns `None`; missing files and duplicate paths remain idempotent.
The input list is not modified and no unlisted paths are removed.

A single cleanup failure is re-raised as the original exception. Multiple
failures are retained in a Python 3.11 `BaseExceptionGroup`, which becomes an
`ExceptionGroup` when all members derive from `Exception`. A caught interruption
is not silently converted to success or an ordinary error: remaining cleanup is
attempted before the original interruption or containing group propagates.
Callers handling multiple failures must account for the grouped result. There
is no implicit retry. When invoked in a caller's `finally`, an active processing
exception remains the context of the cleanup exception/group.

An ExitStack-based prototype attempted all removals but did not preserve the
required exception context in two local tests. Explicit failure collection keeps
every original error accessible instead of relying on that context behaviour.
Acquisition rollback itself is unchanged by this release-only increment.

This is best-effort exception-unwinding cleanup, not crash recovery or a fenced
lease. Failed unlinks can leave residual locks; inspect ownership before manual
removal. A hung unlink, process kill, power failure, concurrent replacement or
arbitrary asynchronous interruption is not solved. Paths and lock ownership
remain trusted under the existing API contract. Successful release is not proof
that the protected pipeline work succeeded or was rolled back.

Run the original acquisition and new release regressions:

```bash
python -m pytest -q tests/unit/test_locks.py \
  tests/unit/test_partition_lock_rollback.py tests/unit/test_partition_lock_release.py
```

Tests use actual temporary locks and targeted unlink failures. They cover
failure positions, remaining-lock reacquisition, multiple errors, preservation
of an active processing error, operation-boundary interruptions, duplicate and
missing paths, and unrelated-file preservation. They do not contact a database,
provider or cloud service.

Python's exception-group contract:
https://docs.python.org/3.11/library/exceptions.html#BaseExceptionGroup
