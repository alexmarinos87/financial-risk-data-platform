# Offline notification evidence validation

Primary arc42 block: `orchestration`. Goal #208, following #207 / PR #212 and
its common-reader prerequisite #192. Existing recorder and worker-health
candidate branches are not dependencies of this command.

## Purpose and use

The existing recorders accept a file and proceed to PostgreSQL recording. This
separate command checks an already retained document without that write path:

```bash
python -m src.orchestration.validate_notification_evidence \
  --kind readiness --input /path/to/readiness-record.json

python -m src.orchestration.validate_notification_evidence \
  --kind receiver --input /path/to/receiver-rehearsal.json

python -m src.orchestration.validate_notification_evidence \
  --kind transition --input /path/to/transition-rehearsal.json
```

Use an existing local document produced for the corresponding record contract;
the command does not create a rehearsal, fetch a record or enable configuration.
There is no DSN argument/environment lookup, recording switch, network mode or
output-file option. Help and argument errors do not load domain contracts.
Argument abbreviations are disabled, and errors do not echo supplied values.

The command reads through the bounded Unicode-safe file reader, selects one
explicitly allow-listed semantic validator, and uses that contract's existing
canonical byte function. It does not invent another record schema or delegate
to a recorder. Existing record identifiers, validation rules and persistence
logic are unchanged.

## Digest and report interpretation

`--expected-sha256` optionally accepts a known lowercase 64-character SHA-256 of
the **canonical record document**. It is not the hash of the original file's
formatting, and is not a signature or proof of approval. Whitespace and JSON
field order can differ while the canonical digest stays the same. Compare the
reported `document_sha256` to the same canonical digest retained by the recorder,
not an arbitrary raw-file checksum.

A successful report contains the record kind, validated record ID, canonical
byte count/digest and whether the supplied digest was checked. It does not
include the input path, full document, endpoint values or provider diagnostics.
The stdout JSON report is at most 4,096 UTF-8 bytes including its newline.
The input and canonical-document ceiling is 1,048,576 bytes; the canonicalizer's
existing allocation behavior is unchanged, not a new total-memory guarantee.

**Status `valid` means only that the retained record is internally consistent.**
A record of a blocked readiness decision or rejected rehearsal can be valid.
The command does not observe current database heads, evaluate current review
expiry, authenticate sources or approve a deployment. Every report explicitly
keeps `source_authenticated`, `current_state_verified` and
`runtime_permission_granted` false. It performs no database or external request.

Exit 0 means successful retained-record validation and any requested digest
match. Exit 1 means rejected evidence, unavailable validation or output failure.
Input/semantic errors return a fixed `invalid_evidence` report without source
fragments; unexpected validation failures return `validation_failed` without a
traceback. Invalid arguments exit 2 with a fixed message; help exits 0. A
successful validation does not establish that the record has been persisted.

## Validation and trust limits

```bash
python -m pytest -q tests/unit/test_validate_notification_evidence.py \
  tests/unit/test_notification_evidence_real_contracts.py
make quality-check
make security-check
make readiness-check
```

Focused tests isolate reporting and argument handling with injected contracts.
Separate wiring tests use the real existing builders, validators and canonical
serializers for all three record kinds. They reject altered identities and
wrong-family inputs; prove blocked/rejected records do not grant permission;
and block socket, DNS and PostgreSQL connection attempts. Clean-interpreter
checks also reject recorder and database-driver imports, avoiding false evidence
from modules already imported by the test runner. Positive CLI tests never
record a document. Tests only create local temporary input/configuration files.

Trusted parent directories and immutable-file provenance remain caller
responsibilities. The reader does not prevent concurrent in-place changes or
supply an I/O deadline. This is not a current-state preflight, source signature
check or alternative to independent code review. Accept prerequisite diffs
first, reconstruct this isolated addition on accepted main and rerun exact-head
CI before final engineer acceptance. No workflow, schema, dependency, committed
activation, scheduling, transport or deployment changes are included.
