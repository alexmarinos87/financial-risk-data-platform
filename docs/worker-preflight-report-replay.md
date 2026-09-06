# Offline replay of complete worker preflight reports

Primary arc42 block: `orchestration`. Goal #171, following usage-error redaction.

The diagnostic can consume its complete captured stdout document with `--report`,
without extracting a nested snapshot by hand. This third mutually exclusive source
mode never consults the database, even when the original report came from a live
read and a database DSN is available in the current environment.

```bash
python -m src.orchestration.check_notification_worker_preflight \
  --report path/to/captured-report.json \
  --worker-id risk-operations-managed \
  --selected-transition-id "$REVIEWED_TRANSITION_ID" \
  --scheduled-for "$REVIEWED_SCHEDULE_SLOT" \
  --worker-config path/to/reviewed/workers.yaml \
  --delivery-config path/to/reviewed/delivery.yaml \
  --destination-config path/to/reviewed/destinations.yaml
```

## Revalidation, not renewed authority

The wrapper must contain exactly source mode, database-read flag, false runtime
permission and result. Source mode and strict boolean flags must agree. The
existing reviewed-preflight validator reconstructs the entire inner result,
including its snapshot and configuration binding. Explicit worker, transition
and normalized slot must match the captured selection; the command cannot silently
replace them with values chosen by the report.

Successful replay emits `source_mode = retained_report` and
`database_read_performed = false`, preserving the original captured result and its
database observation time. Replaying a replay is deterministic. Exit codes remain
0/3/4 for eligible/wait/blocked at that captured instant, never permission to run.
Changed reviewed configuration, tampered evidence and invalid wrapper metadata
fail closed. Captured source-mode metadata is an unauthenticated claim; consistent
metadata and hashes cannot prove that a past database read actually occurred.

The existing bounded regular-file reader is shared by snapshot and report modes:
no symlink final components, duplicate JSON fields or oversized input. Trusted
parents and immutable reviewed configurations remain caller responsibilities.
The command still emits only stdout and does not create or overwrite report files.

```bash
python -m pytest -q tests/unit/test_notification_worker_report_replay.py
make quality-check
make security-check
make readiness-check
```

No database refresh, health evaluation, ledger write, schedule claim, live request,
notification, configuration switch, deployment or Terraform apply. Reuse the
existing contracts; do not interpret captured eligibility as current authority.
Explicit engineer acceptance of this candidate and its predecessors remains pending.
