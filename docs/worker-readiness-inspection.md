# Inspect worker readiness without execution

Primary arc42 block: `warehouse`. Goal: #204, under roadmap #76.

The command wraps the existing read-only source reader. It does not evaluate
failure histories, claim schedule slots, run preflight execution, suspend or
activate workers, send notifications, or write database records.

## Default: no database access

```bash
python -m src.warehouse.inspect_worker_readiness --worker-id example-worker
```

This returns `status: not_requested`, with both `database_read_attempted` and
`database_read_completed` false. It does not look up a DSN environment variable
or import the database reader. Help is also database-free. Argument abbreviations
are disabled, and argument-error output does not echo supplied values.

## Explicit read

Supply an authorized database DSN through the environment using your normal
secret-management mechanism. The command accepts an environment-variable name,
not a DSN value on the command line:

```bash
python -m src.warehouse.inspect_worker_readiness \
  --worker-id example-worker \
  --read-database \
  --dsn-env WAREHOUSE_POSTGRES_DSN
```

Only this mode calls `read_current_worker_readiness`. Its dedicated idle
read-only autocommit session, query timeout, current authority selection, bounded
source retrieval, canonical document verification and current-review comparison
remain in force. The supplied target is the operator's responsibility; this
command does not make an arbitrary DSN safe or prove a database is disposable.

## Output and exit codes

The report contains the observation time, selected authority reference, snapshot
reference, per-kind retained record IDs/digests/status, missing execution kinds
and allow-listed blocking reasons. It does not print full source documents,
endpoint values, connection strings or raw provider errors. The projection checks
identity and status coherence but is not another authority policy or independent
source attestation; source validation belongs to the reader.

Exit 0 means either no read was requested or the selected readiness sources were
ready. Inspect `status` to distinguish those cases. Exit 2 means a completed read
found blocked/missing authority, or the argument parser rejected the command.
Exit 1 means configuration, reading or output validation failed. A failure report
records whether the read was attempted, and does not claim that no database I/O
occurred. `database_read_completed` is only true after the reader and report
validation both complete successfully.

Every report keeps `runtime_permission_granted` and `failure_history_verified`
false. In particular, `ready_sources` and exit 0 do not authorize delivery or
establish healthy per-worker failure history. An unknown worker cannot establish
which execution kinds should exist, so its report uses `authority_missing`
rather than inventing a successful empty inventory.

## Evidence and boundaries

Focused tests cover opt-in access, no default credential lookup, safe argument
errors, environment indirection, each outcome and exit code, malformed/contradictory
results, detached output and sanitized failures. A separate wiring test calls the
real reader and semantic validators with only the database connection replaced:
valid source records report both kinds, while a changed stored digest fails.
The predecessor's PostgreSQL contract proves the actual SQL and session path.

On a complete checkout, run:

```bash
python -m pytest -q \
  tests/unit/test_worker_readiness_inspection.py \
  tests/unit/test_worker_readiness_inspection_wiring.py
```

Then run repository quality/security/readiness checks. The command adds no schema,
workflow, dependency, default DSN, activation switch or scheduler integration.
No deployment or Terraform apply is performed. Explicit final-diff acceptance
and accepted-main integration remain separate from passing tests.
