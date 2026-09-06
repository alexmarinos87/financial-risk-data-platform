# Destination-transition rehearsal file intake

Primary arc42 block: `warehouse`. Goal #198; prerequisite #186 / PR #192.

## Decision and scope

The transition history recorder delegates `_read_record` to the existing common
bounded JSON reader and retains `MAX_RECORD_BYTES = 1_048_576`. Only this loader
changes in product code. Rotation/disable/rollback validation, recorded identities,
SQL, transaction behavior, replay handling, CLI functions and flags are unchanged.

Previously, a file could grow between a size check and the unbounded text read.
Duplicate names could also disappear during ordinary JSON decoding before the
semantic validator saw the record. The new boundary rejects these ambiguous
inputs before invoking persistence, rather than changing how history is stored.

## Executable evidence

```bash
python -m pytest -q \
  tests/unit/test_notification_destination_transition_rehearsal_recorder.py \
  tests/unit/test_transition_record_file_intake.py
make quality-check
make security-check
make readiness-check
```

The new tests construct actual canonical transition records using the existing
no-network rehearsal fixture and record builder. They prove unchanged round-trip
and CLI delegation, root/nested/escaped-equivalent duplicate rejection, exact
byte boundaries, bounded reads after file growth, malformed/non-finite JSON,
excessive nesting, unsafe file types and redacted read errors. Existing negative
tests keep their rejection assertions; only two diagnostic matches change.

Malformed intake must never call persistence. Semantically tampered but valid
JSON must never call `psycopg.connect`. These guards record attempted calls so
a swallowed provider exception cannot produce a false pass. Positive CLI tests
inject persistence rather than connecting to any application database. A separate
test blocks sockets, DNS and PostgreSQL connections while constructing and
reading the synthetic rehearsal record; enabled configuration copies exist only
inside temporary test directories.

## Compatibility and trust

Valid decoded record contents, ordinary formatting and trailing whitespace within
the byte ceiling are preserved. The parser now rejects duplicate fields at all
levels, non-finite tokens and floating-point overflow, malformed UTF-8, non-object
roots and more than 64 nested containers. Intake failure remains exit 1; invalid
CLI usage remains exit 2. The exact shared-reader diagnostic text differs from
older loader messages. No new execution or recording option is introduced.

See [bounded JSON evidence](bounded-json-evidence.md) for platform-specific
no-follow/nonblocking flags and descriptor checks. Parent directories and the
filesystem are trusted; concurrent in-place changes are not prevented, and reads
have no I/O deadline. Use immutable reviewed files. Parsing and hashes are not
source authentication, current readiness or runtime permission. The existing
DSN/argparse interface and shell-history exposure are not redesigned here.

This is an independent sibling of goal #197 / PR #201, not a successor to it or
#194. Accept #192 first, then reconstruct each isolated consumer change on
accepted main and rerun exact-head CI. Independent review and explicit engineer
acceptance remain pending. No schema, workflow, dependency, committed activation,
scheduler, transport or deployment change is included.
