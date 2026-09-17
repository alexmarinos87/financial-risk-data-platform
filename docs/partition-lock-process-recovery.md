# Partition-lock process recovery evidence

Primary arc42 block: `orchestration`; backfill/idempotency evidence (backlog 5).

```bash
python -m pytest -q tests/integration/test_partition_lock_process_recovery.py
```

These tests launch a separate Python interpreter against real temporary lock
files. A parent-owned lock blocks the child, including when the child acquires
an earlier sorted partition first. The parent's payload remains unchanged and
no earlier lock is abandoned. After explicit release, the other process can
acquire the complete set without enabling stale takeover.

A regular file obstructing a later partition directory produces a real operating
system failure, without mocking mkdir/open/unlink. Acquisition must roll back
its earlier lock, preserve the obstruction, and allow a fresh-process retry
after the test removes only its own obstruction. The tests also exercise an
ownership handoff from child to parent and back, and sorted/duplicate requests.

Each child has a 15-second timeout and is killed and waited for on timeout.
Contention is deterministic because the parent holds its lock throughout the
child attempt; there are no sleeps or timestamp-based coordination assumptions.

This is evidence for ordinary local-filesystem exception unwinding and overlap,
not a distributed lock, atomic multi-lock acquisition, crash recovery or fencing
claim. Symlink/replacement and stale-owner races remain outside this contract.
The tests change no product code, lock payload, timeout policy or deployment.
