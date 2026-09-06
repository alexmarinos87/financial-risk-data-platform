# Controlled-receiver rehearsal file intake

Primary arc42 block: `warehouse`. Goal #197; prerequisite #186 / PR #192.

## Decision and runtime boundary

The existing receiver history recorder delegates `_read_record` to the shared
bounded JSON reader with its unchanged 1,048,576-byte ceiling. This removes a
path-size check followed by an unbounded text read. The recorder's checklist
and rehearsal validators, SQL, transactions, identities, idempotency and CLI
functions are unchanged.

The sequence is local file intake, then semantic record validation, then the
existing explicit PostgreSQL recording operation. The file parser does not
run the receiver or send a notification. Ordinary JSON decoding can discard
earlier duplicate names; the strict reader rejects them before they disappear.
Tests demonstrate both root and nested conflicts whose last occurrences form
an otherwise valid canonical record, including an escaped equivalent field.

## Compatibility and limits

Completed and rejected canonical records retain their exact decoded contents.
Valid JSON formatting and trailing whitespace remain allowed within the byte
ceiling. Duplicate fields, non-finite numbers/float overflow, malformed UTF-8,
non-object roots and more than 64 nested containers now fail at intake. The
shared reader supplies fixed diagnostics; exact old message text is not an API.
The CLI flags, rejection exit 1 and usage exit 2 remain unchanged.

See [bounded JSON evidence](bounded-json-evidence.md) for descriptor ownership,
platform-dependent flags and short-read handling. Parent directories and the
filesystem remain trusted. Concurrent in-place modification is not prevented;
this is not an immutable snapshot or I/O deadline. Parsing and hashes do not
authenticate a reviewer or grant runtime permission. Use immutable reviewed
inputs. Existing DSN options, shell-history exposure and argument parsing are
not redesigned in this loader-only patch.

## Validation and review

```bash
python -m pytest -q tests/unit/test_receiver_record_file_intake.py
make quality-check
make security-check
make readiness-check
```

The tests reuse the existing real rehearsal builders/validators, exercise the
actual local loader, and guard CLI persistence calls on malformed, missing,
nonregular or growing inputs. They count actual bytes when a file grows after
inspection. Semantic tampering must not reach a database connection; the test
records attempted calls rather than relying on an exception that could be
caught by the recorder. Positive CLI tests inject persistence and do not claim
live database recording. Existing disposable PostgreSQL CI remains a separate
regression gate, not proof of local-file behavior by itself.

This candidate is independent of readiness-recorder integration #194 and the
sibling transition-intake goal #198. Accept the common reader first, then
reconstruct each isolated consumer delta on accepted main and rerun exact-head
checks. Independent correctness/production review and explicit final-diff
engineer acceptance are still required. No schema, workflow, dependency,
configuration activation, scheduling or deployment changes are included.

Reference for decoder behavior: Python 3.11 JSON documentation,
https://docs.python.org/3.11/library/json.html#repeated-names-within-an-object
