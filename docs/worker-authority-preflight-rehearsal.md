# No-network worker authority preflight rehearsal

Primary arc42 block: `orchestration`.

## Run the walkthrough

From the repository root, after the normal development setup:

```bash
.venv/bin/python -m src.orchestration.rehearse_worker_authority_preflight \
  --planned-at 2026-09-06T00:00:00Z \
  --summary-json .demo/worker-authority-preflight-rehearsal.json
```

Omit `--summary-json` to print the report without creating an output file. The
planning instant must be timezone-aware and have whole-second precision.
Equivalent timezone representations produce the same canonical UTC report;
there is no implicit wall clock. The default worker is `risk-operations-managed`.

There is no `--execute` or `--record` option. This command needs no database,
provider key, webhook endpoint, cloud credential or running scheduler.

## What the report proves

The rehearsal reads the three committed worker/delivery/destination files as
bounded regular files. It enables only in-memory dataclass copies and uses the
actual worker planner, lifecycle-authority builder and preflight evaluator.
No permissive substitute plan schema is used and no configuration file is
rewritten to enable the worker.

Ten deterministic scenarios are reconciled against fixed expected outcomes:

| Scenario | Expected outcome |
| --- | --- |
| Due slot with agreeing authority/configuration | `eligible_for_health_review` |
| Otherwise valid early invocation | `wait` |
| Old selected grant with a newer current stop | `blocked` |
| Stale observation | `blocked` |
| Future observation | `blocked` |
| Current configuration disabled | `blocked` |
| Missing current authority | `blocked` |
| Authority at its exclusive expiry | `blocked` |
| Expired review observation | `blocked` |
| Different selected schedule slot | `blocked` |

The report contains each scenario's evidence, expected outcome, required blocker
and complete preflight result. Each result is independently reconstructed by the
preflight validator. A wrong outcome or missing expected reason fails the whole
rehearsal rather than printing a misleading passing report.

For the documented command, expect `scenario_count = 10`, `passed_count = 10`,
`failed_count = 0`, with one eligible, one waiting and eight blocked cases.
The complete report has a deterministic content identity.

## Synthetic evidence, not operational approval

`observations_synthetic = true` is mandatory in the report. The reviewer and
operator identifiers are explicitly synthetic. The expired-review case is an
intentional inconsistent-input challenge, not an assertion that a real approved
configuration or current database record was read.

Both the report and every preflight retain `readiness_evaluated = false` and
`runtime_permission_granted = false`. Passing the due-slot scenario proves only
the preflight contract, not healthy delivery, an acquired lock, a claimed slot,
authenticated approval or a deployed worker.

See [the preflight contract](notification-worker-authority-preflight.md) for the
source-provenance boundary. Live consistent observation acquisition and
composition with the separately reviewed health/suspension stack remain future
work. No competing per-kind health evaluator is included in this rehearsal.

## Output and failure safety

Optional report output uses the existing exclusive-temporary-file, atomic,
1 MB bounded summary writer. The CLI rejects destinations inside the repository
`config` tree and rejects symbolic-link destinations through the shared writer.
The writer's trusted-parent-directory and concurrent-writer limitations still
apply; this is not a hostile-filesystem sandbox or a durable append-only ledger.

The rehearsal performs no database read, connection, network resolution,
notification request, lock acquisition, scheduler mutation, deployment or
`terraform apply`. Configuration bytes stay unchanged. Tests replace DNS,
socket connection and PostgreSQL connection functions with failures to detect
accidental calls and compare committed configuration bytes before and after.

## Validation

```bash
.venv/bin/python -m pytest -q tests/unit/test_worker_authority_preflight_rehearsal.py
make quality-check
make security-check
make readiness-check
```

Regression cases cover deterministic replay, timezone equivalence, changed
planning identity, all scenario outcomes, no-network/no-database behavior,
bounded output, symlink preservation, configuration-output rejection, absent
execution flags, invalid dates, unknown workers and inconsistent evaluator
results. CI and self-review remain engineering evidence, not independent human
approval or permission to activate infrastructure.
