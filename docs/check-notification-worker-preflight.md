# Read-only worker preflight operator command

Primary arc42 block: `orchestration`. Goal #166 follows #164/#165 under #76.

## What this demonstrates

```text
explicit worker / transition / schedule slot
  + retained snapshot OR explicitly requested current database read
  + reviewed immutable configuration files
  -> configuration-bound authority preflight at the captured database instant
  -> bounded JSON diagnostics; never execution permission
```

The module is `src.orchestration.check_notification_worker_preflight`.
There is no execute, record, schedule, deployment or output-file flag. Output is
stdout only, so the command introduces no file-overwrite surface. Database reads
require `--read-current`; an environment DSN cannot turn offline mode into a read.
Argument abbreviation is disabled.

## Offline validation

Provide a retained snapshot produced by the first layer, the intended worker,
selected transition and exact slot, plus the reviewed configuration paths:

```bash
.venv/bin/python -m src.orchestration.check_notification_worker_preflight \
  --snapshot path/to/authority-snapshot.json \
  --worker-id risk-operations-managed \
  --selected-transition-id "$REVIEWED_TRANSITION_ID" \
  --scheduled-for "$REVIEWED_SCHEDULE_SLOT" \
  --worker-config path/to/reviewed/workers.yaml \
  --delivery-config path/to/reviewed/delivery.yaml \
  --destination-config path/to/reviewed/destinations.yaml
```

The input must be bounded regular-file JSON with no duplicate object fields or
symbolic-link final component. Trusted parent directories and immutable files
remain prerequisites. The descriptor is checked before reading, including FIFO
rejection on platforms with nonblocking file opens. The whole stdout report is
bounded to 1 MB including its newline.

Offline output explicitly says `source_mode = retained_file` and
`database_read_performed = false`. It revalidates captured evidence only. It
cannot prove the snapshot is still current, even when the captured result passes.

## Explicit current read

Use the same identity and reviewed-file arguments, replacing `--snapshot ...`
with `--read-current`. The adapter obtains its DSN only from the operator's
`WAREHOUSE_POSTGRES_DSN` environment variable. Do not put credentials into command
arguments or retained evidence. A DSN argument is not supported.

This invokes the first layer's new read-only transaction; it does not select an
old grant supplied by a caller. Wrong-worker snapshots are rejected. Unknown
workers return blocked evidence rather than an invented grant. Output identifies
`live_database_read`, but is still evaluated at its captured database instant:
a later stop or configuration change requires another observation.

The command does not acquire the shared delivery lock, evaluate health, claim a
slot, mutate authority or activate any delivery path. Source identifiers and
hashes do not authenticate operators or configuration approval.

## Exit status

| Code | Meaning |
| --- | --- |
| 0 | Captured authority/slot is eligible for separate health review, not execution. |
| 1 | Validation or I/O failed; no successful report is emitted. |
| 2 | Invalid command usage. |
| 3 | Captured valid authority is waiting for its slot. |
| 4 | Captured preflight is blocked. |

Validation and provider failures emit one fixed diagnostic without DSNs,
filenames, configuration values or raw provider exceptions. This trades verbose
operator debugging for a stable secret-free boundary. Argparse usage errors are
not a channel for credentials: never pass secrets as unsupported arguments.

## Validation and acceptance

```bash
.venv/bin/python -m pytest -q tests/unit/test_check_notification_worker_preflight.py
make quality-check
make security-check
make readiness-check
make postgres-contract-check
```

Tests inject readers and use actual source contracts with temporary enabled
configuration copies. They cover offline/no-database behavior despite a DSN,
explicit delegation, exit codes, missing/wrong workers, malformed/oversized/
duplicate JSON, symlinks/FIFOs, rejected flags and redacted failures. The
predecessor's disposable PostgreSQL fixture covers the real snapshot SELECT.

No committed configuration switches, schemas, workflows, dependencies or
infrastructure change. No application database was contacted during development.
No external notification, scheduler activation, deployment or Terraform apply.
The candidate remains pending explicit engineer acceptance. Accept predecessors
in order, reconstruct each delta on accepted main and rerun exact-head CI.
