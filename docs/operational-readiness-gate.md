# Operational Readiness Gate

## Outcome

This increment turns the latest retained operational service-level report into one
read-only, deterministic readiness decision:

```text
current gate policy
  + current operational threshold policy
  + current local schedule fingerprint
  + latest expected market session
  + effective portfolio mandate fingerprint
  + latest exact-contract retained report
  -> allow or block
  -> deterministic JSON evidence and process exit code
```

The gate is incapable of executing the portfolio schedule, requesting provider
data, retrying or delivering notifications, activating a cloud schedule, changing
positions or deploying infrastructure.

## Gate policy

`config/operational_readiness_gates.yaml` defines:

- the operational service-level policy to evaluate;
- the maximum retained-report age; and
- whether a current `warning` report may pass.

The demonstration gate is strict: warning and critical reports block, and the
report may be at most one hour old. Its deterministic fingerprint binds all gate
settings, so policy changes produce distinguishable decision evidence.

Critical status always blocks. `allow_warning` can permit warning evidence but
cannot permit critical evidence.

## Exact current contract

Before querying PostgreSQL, the runner resolves:

- the current operational service-level policy and fingerprint;
- the linked local schedule and fingerprint;
- the schedule calendar;
- the latest expected market session at the explicit evaluation instant; and
- the effective portfolio mandate and fingerprint for that session.

It then reads at most two rows from
`risk_platform.latest_operational_service_level_reports`, filtering by the exact
policy, schedule, calendar, portfolio, risk-limit policy and mandate contract.
Zero rows becomes a deterministic `report_missing` block. More than one exact row
fails closed because the serving grain is inconsistent.

## Decision rules

The gate blocks for any of these reasons:

| Reason | Meaning |
| --- | --- |
| `report_missing` | No retained report matches the exact current contract |
| `report_timestamp_future` | The report timestamp is after the explicit gate instant |
| `report_age_exceeds_limit` | The report is older than the configured maximum age |
| `report_session_mismatch` | The report covers a different latest expected market session |
| `report_status_warning` | The report is warning and the gate does not allow warnings |
| `report_status_critical` | The report is critical |

Malformed identities, digests, timestamps, statuses or configuration bindings are
invalid evidence and fail closed rather than becoming a normal policy decision.

The decision is `allow` only when no reason remains.

## Operator command

Use an explicit UTC timestamp:

```bash
.venv/bin/python -m src.warehouse.operational_readiness_gate \
  --gate-id us-tech-local \
  --evaluated-at 2026-04-01T12:00:00Z \
  --summary-json .demo/operational-readiness-gate.json
```

Exit codes are:

- `0` — allowed;
- `2` — valid evidence produced a blocked decision; and
- `1` — configuration, retained evidence or PostgreSQL access was invalid.

The JSON evidence includes the gate and report identities, exact current
fingerprints, expected session, report age/future offset, report status, decision
and ordered reasons. It explicitly records that no schedule, provider, delivery
or cloud activation side effect occurred.

## Current boundary

This is a reviewable read-only gate. It is not wired into the local scheduler,
notification delivery or deployment workflows. A later integration slice may
consume the exit code only after deciding which operation should be protected and
how an operator may override or acknowledge a block.
