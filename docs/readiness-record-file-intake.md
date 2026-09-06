# Readiness record file-intake hardening

Primary arc42 block: `warehouse`. Goal #187, following the common reader in #186.

## Decision and scope

The readiness recorder's `_read_record` delegates to
`src.common.bounded_json.load_bounded_json_object` with the existing
1,048,576-byte ceiling. Its old path-size check followed by unbounded `read_text`
could exceed the checked size when a file grew. Plain JSON decoding also lost
conflicting earlier duplicate fields before record validation could see them.

This change replaces only the local-file loader. Canonical record validation,
decision reconstruction, request idempotency, SQL, transaction handling,
result formatting and public CLI options are unchanged. Parsing success still
does not authorize a record: the existing semantic validator runs before the
recorder connects to PostgreSQL.

## Compatibility

Ordinary valid readiness files round-trip unchanged, including formatting
whitespace within the byte limit. Duplicate keys at any depth are rejected,
even when the last occurrence would have formed a valid canonical record.
Non-finite numbers, invalid UTF-8, non-object roots and more than 64 nested
containers are rejected. Exact one-megabyte files remain accepted; one byte
more is rejected on actual descriptor reads.

The shared reader now supplies fixed intake diagnostics, so consumers should
not rely on the old exact error text. Existing negative tests retain the same
rejection assertions with updated diagnostic matches. The command's validation
failure exit remains one; no new execution or recording mode is introduced.

The separate existing DSN option and argparse behavior are not changed by this
file-intake patch. Do not pass credentials as unsupported command arguments.
This PR does not claim to solve process-list or shell-history confidentiality.

## Trust and validation

See [the shared intake contract](bounded-json-evidence.md) for platform flags,
trusted-parent-directory assumptions, descriptor cleanup and the absence of a
regular-file read deadline or immutable-file guarantee. Inputs must still come
from immutable reviewed snapshots. A parsed object or hash is not proof of
reviewer authentication, current source state or execution permission.

```bash
python -m pytest -q tests/unit/test_bounded_json.py \
  tests/unit/test_notification_execution_readiness_recorder.py \
  tests/unit/test_readiness_record_file_intake.py
make quality-check
make security-check
make readiness-check
```

New tests use the existing real readiness builder and validator. They demonstrate
a valid last duplicate occurrence being accepted by plain decoding but rejected
at intake, exact byte boundaries, symlinks/FIFOs/directories, unchanged delegation,
no recorder call on malformed input and no database connection on semantically
invalid records. Database-facing calls are injected in positive CLI tests.

No application database is accessed during implementation or unit testing. No
schema, workflow, dependency, configuration enablement, scheduler, transport,
notification or deployment change. Independent review and explicit final-diff
engineer acceptance remain required before merge.
