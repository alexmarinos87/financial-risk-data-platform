# Rolling Operational Service-Level Objectives

## Outcome

This report-only path turns retained operational service-level reports into one
deterministic rolling objective-attainment decision:

```text
current operational report per expected market session
  -> exact current policy, schedule, calendar and mandate contract
  -> bounded market-session window
  -> four objective-attainment ratios
  -> explicit insufficient, met or missed status
  -> credential-free JSON evidence
```

It does not rerun the scheduler, query a market-data provider, retry a
notification, deliver an alert, activate a cloud schedule or perform automated
remediation.

## Versioned Objective Policy

`config/operational_service_level_objectives.yaml` defines a policy with:

- the retained operational service-level policy it evaluates;
- a bounded objective window measured in expected market sessions;
- the minimum retained observations required before attainment can be judged;
- one success threshold and target ratio for each supported objective.

The first model version is `operational-slo-attainment-v1`. Objective policy
fingerprints use the `operational-slo-objective-policy-` prefix and bind all
window, minimum-history, source-metric, threshold and target settings. Changing an objective therefore creates a new evidence identity rather
than relabelling an earlier result.

The four objectives are:

| Objective | Source metric | A session succeeds when |
| --- | --- | --- |
| Schedule completion | `schedule_lag_sessions` | lag is at or below the configured threshold |
| Constituent freshness | `market_freshness_exception_count` | exception count is at or below the configured threshold |
| Retry-exhaustion incidence | `notification_retry_exhausted_count` | exhausted-event count is at or below the configured threshold |
| Dead-letter duration | `notification_oldest_dead_letter_age_seconds` | oldest age is at or below the configured threshold |

## Session Window And Corrections

The runner resolves the current operational policy, local schedule, market
calendar and effective portfolio mandate before reading retained reports. It then
builds the last configured expected market sessions ending at
`--through-session`.

For each expected session, the current retained report is selected by:

```text
as_of DESC
calculation_id DESC
```

Identical duplicate calculation IDs are ignored. Reusing one calculation ID for
conflicting report content fails closed.

Only reports matching the exact current contract are eligible:

```text
operational policy fingerprint
schedule fingerprint
calendar ID
portfolio ID
risk-limit policy ID
mandate fingerprint
```

A report from an older policy, schedule or mandate cannot enter the objective
window silently.

## Missing Reports And Insufficient History

The denominator is the configured expected market-session window, not merely the
number of reports that happen to exist. A missing report therefore counts against
attainment once the minimum-history requirement has been reached.

Before `minimum_observations` current reports exist, every objective and the
overall result use:

```text
history_status = insufficient
overall_status = insufficient
```

This is distinct from an objective miss. Once the minimum is reached, each
objective is `met` or `missed` using:

```text
attainment_ratio = successful expected sessions / expected sessions in window
```

The evidence separately records observed failures and missing-report sessions so
a consumer can distinguish bad operational values from absent reporting.

## Operator Command

After operational service-level reports have been recorded in PostgreSQL:

```bash
.venv/bin/python -m src.orchestration.run_operational_service_level_objectives \
  --objective-policy-id us-tech-local-20-session \
  --through-session 2026-03-31 \
  --summary-json .demo/operational-slo-attainment.json
```

The requested date must be an expected session in the configured calendar. The
PostgreSQL reader is restricted to the exact current contract and selected
window, and it caps one request at 10,000 retained rows.

## Evidence Contract

The report records:

- deterministic calculation, model and objective-policy identities;
- exact operational policy, schedule, calendar, portfolio, risk-limit policy and
  mandate identities;
- expected window boundaries and missing report sessions;
- current input report calculation IDs and document SHA-256 values;
- available and expected observation counts;
- objective thresholds, targets, success/failure counts and attainment ratios;
- `insufficient`, `met` or `missed` status; and
- explicit false side-effect flags.

The calculation ID binds the complete expected-session window, current report
identities and digests, objective settings and resulting objective rows.
Corrections or policy changes therefore produce distinguishable evidence.

## Current Boundary

This increment writes optional local JSON only. It does not yet persist objective
attainment in PostgreSQL or expose current attainment views. Append-only history
and queryable serving are the next separate dependency layer. It also does not
add a dashboard, paging, automatic gate integration, schedule activation or
external notification delivery.
