# Reopening worker readiness source records

Primary arc42 block: `warehouse`. First layer of the readiness-source adapter.

`verify_worker_readiness_record` reopens the complete persisted readiness record,
uses the existing semantic validator, verifies its retained SHA-256, and requires
its exact selected record ID, destination and execution kind. It does not accept
a digest or a projected status as a substitute for the source document.

The observation clock is explicit. Both decision evaluation and record creation
must be no later than observation. Age is recomputed rather than copied from a
view or a long-running transaction timestamp. The exact configured maximum is
accepted; older evidence is stale. Maximum age is an integer from 1 to 300,
excluding booleans. Input serialization is bounded to 1 MiB and returned records
are detached JSON snapshots.

The result distinguishes `retained_status` from current authority. An intact
historical allow record may have been superseded. Therefore
`current_evidence_verified` and `runtime_permission_granted` always remain false.
The next layer must reconcile the selected current review and governed worker
configuration; this verifier does not authenticate a producer, select the latest
record, inspect current failure history, or refresh notification readiness.

A missing record is not passed to this verifier as an empty successful document.
The future inventory adapter must report it as missing. Malformed, differently
bound, oversized, future-dated and changed records raise `ValidationError`.
Rehashing a contradictory decision cannot bypass the existing policy validator.

Run `python -m pytest -q tests/unit/test_worker_readiness_source.py`, followed by
repository quality, security and readiness checks. Tests compose the actual
readiness record builder and gate fixtures rather than replacing the underlying
policy validator.

No existing source contract, workflow, schema, dependency or activation switch is
changed. This is in-memory verification only: no database, network, delivery,
scheduler, deployment or `terraform apply`. IDs and hashes are integrity evidence,
not authenticated identities or signatures.
