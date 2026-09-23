# Preserve the type of lock acquisition timestamps

Primary arc42 block: `orchestration`; backfill and lock-handling follow-up.

The stale-lock inspector must not turn malformed metadata into evidence that a
lock may be replaced. The `acquired_at` value is passed to the datetime parser
without first converting it to text. A JSON number such as `19700101` previously
became a valid basic-format date after `str()`, allowing age-based takeover.
It now fails parsing, so the existing lock remains blocked and unchanged.

The normal writer already emits timestamp strings. Existing accepted textual
formats retain their policy, including timezone offsets, legacy naive timestamps
interpreted as UTC and basic-format date strings. This is a type-preservation
change, not a stricter timestamp-format migration. Non-string metadata is not
rewritten or repaired. A blocked later partition still rolls back earlier locks
created by the unsuccessful acquisition attempt.

```bash
python -m pytest -q tests/unit/test_partition_lock_timestamp_types.py
```

The tests inspect real temporary files, preserve their bytes, distinguish JSON
numbers from equivalent strings, and exercise the public acquisition rollback.
Disabled takeover still performs no metadata read. Existing metadata-size,
short-read, duplicate-key, descriptor-cleanup and timeout controls are unchanged.

This does not prove the recorded acquisition time is truthful, establish that
an owner has stopped, or solve stale-owner replacement races. Trusted parent
directories and the existing manual-investigation policy remain prerequisites.
