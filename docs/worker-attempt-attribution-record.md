# Canonical worker attempt attribution records

Primary arc42 block: `warehouse`. Goal #226; follows #213 and #218.

A receipt ID or callback return does not independently identify a stored source.
`build_worker_attempt_attribution` retains one exact validated authority, its
execution context, the normalized source attempt and a reconstructed observation
receipt. The existing context validator and observer receipt builder remain the
semantic authorities; this layer does not invent another execution policy.

Equivalent attempt timestamps normalize to UTC before hashing. The validator
rebuilds all relationships, including source digest, context and derived receipt,
and compares canonical bytes. Changing a receipt or source then merely rehashing
the outer record fails. A coherent different source is a different record, not
permission to overwrite the first. Returned JSON is detached from caller inputs.

The attempt is limited to 8 KiB and complete record to 1 MiB, including its ID.
These are post-encoding bounds, not process-memory guarantees. NUL and unpaired
surrogate text are rejected rather than repaired before UTF-8 PostgreSQL JSONB
storage. Existing source fields include endpoint_host and payload_sha256, but no
endpoint URL, credential value or payload body. Do not print the complete record
as a routine operator report. Identifiers are not a secret scanner.

The next persistence layer must reopen the retained authority, preserve exactly
one context per authority/slot/kind scope, and commit a new source attempt with
its attribution together. Existing unattributed rows cannot be silently assigned
ownership. Replay must verify the retained source rather than send again.

A valid record is still caller-supplied evidence. It does not prove transport,
authenticate an operator, verify current configuration/authority, cover every
invocation, claim a runtime slot or establish zero failures. Both
failure_history_complete and runtime_permission_granted stay false. There is no
I/O, schema application, observer mutation, default producer change or activation.

Run `python -m pytest -q tests/unit/test_worker_attempt_attribution_contract.py`
and the normal repository quality/security/readiness checks. Tests cover both
kinds, valid failures, UTC normalization, detached input, rehashed contradictory
links, database text, exact byte bounds and agreement with the existing observer.
Independent review and explicit final-diff acceptance remain separate from tests.
