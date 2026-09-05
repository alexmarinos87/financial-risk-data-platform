# Reviewed worker authority recording

Primary arc42 block: `warehouse`.

## Decision

`src/warehouse/reviewed_notification_worker_authority_history.py` composes the
reviewed configuration checks from PR #152 with the append-only recorder from
PR #153. It introduces neither a new transition model nor a second ledger.

`prepare_reviewed_worker_authority` validates the document size, exact supplied
predecessor, strict plan semantics, configuration reconstruction and destination
review lifetime without connecting to PostgreSQL. The result is a detached
canonical transition.

`record_reviewed_worker_authority` completes that preparation before calling the
existing recorder. Inside its transaction, the recorder acquires the per-worker
lock and reconciles the actual retained head again. A supplied predecessor does
not replace that database check. A competing new root or stale successor still
fails; exact historical replay still returns the original sequence without
promoting it to current authority. Storage failures, including unconfirmed
commit acknowledgements, are not converted into success.

## Operator command

Validate reviewed snapshots without any database connection:

```bash
.venv/bin/python -m src.warehouse.reviewed_notification_worker_authority_history \
  --transition .demo/worker-authority.json \
  --worker-config path/to/reviewed/workers.yaml \
  --delivery-config path/to/reviewed/delivery.yaml \
  --destination-config path/to/reviewed/destinations.yaml
```

For a successor, supply `--previous path/to/exact-predecessor.json`. Recording
additionally requires `--record` and an operator-provided
`WAREHOUSE_POSTGRES_DSN` environment variable. The command does not take a DSN
argument that could be copied into shell history. It does not resolve a webhook
endpoint. The default remains validation-only even when the environment contains
a database DSN.

## Trust and compatibility

Configuration files must be immutable reviewed snapshots in trusted directories.
This composition checks their contents, not provenance or reviewer identity.
Historical replay requires the same reviewed configuration evidence and supplied
historical predecessor; the storage primitive still verifies exact retained
request identity. There is no live-readiness check or permission to deliver.

The lower-level `notification_worker_authority_history` API is deliberately
unchanged and remains a trusted persistence primitive. This new entry point is
an opt-in configuration-checked admission path, not a database privilege change
that makes every other writer impossible. Restricted writers, authenticated
operators and production migration remain separate responsibilities.

No schema, dependency, workflow, infrastructure or committed activation setting
changes. Full database behavior remains covered by the existing PostgreSQL
contract suite; the added tests use injected recorders and the existing cursor
primitive, not a live application database.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_reviewed_worker_authority_recording.py
make quality-check
make security-check
make readiness-check
```

Review configuration rejection before persistence, exact delegation, historical
replay, locked head conflicts, validation-only CLI behavior and failure
propagation. CI evidence is not independent human approval or operational
activation.
