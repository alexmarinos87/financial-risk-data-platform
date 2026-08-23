# Rolling Operational Service-Level Objectives

## Outcome

This path turns retained operational service-level reports into deterministic
rolling objective-attainment evidence and preserves every accepted calculation in
PostgreSQL:

```text
current operational report per expected market session
  -> exact current policy, schedule, calendar and mandate contract
  -> bounded market-session window
  -> four objective-attainment ratios
  -> explicit insufficient, met or missed status
  -> optional credential-free JSON evidence
  -> append-only PostgreSQL history
  -> current objective and exception views
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
window, minimum-history, source-metric, threshold and target settings. Changing
an objective therefore creates a new evidence identity rather than relabelling
an earlier result.

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

An identical duplicate calculation ID is ignored. Reusing one calculation ID for
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

The evidence separately records observed failures and missing report sessions so
a consumer can distinguish bad operational values from absent reporting.

## Report-Only Operator Command

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

## Append-Only Recording

Record an accepted report explicitly:

```bash
.venv/bin/python -m \
  src.warehouse.operational_service_level_objective_recorder \
  --report .demo/operational-slo-attainment.json
```

The recorder:

- validates the exact report and objective shapes;
- rechecks observation counts, attainment ratios and statuses;
- requires every report-only side-effect flag to remain false;
- serialises a canonical JSON document and records its SHA-256 digest;
- inserts on `calculation_id` without overwriting history;
- converges on replay when the stored digest matches; and
- rejects reuse of one calculation ID for different content.

Database triggers block update and delete operations on retained objective
reports.

## PostgreSQL Serving Contract

The base history table is:

```text
risk_platform.operational_service_level_objective_reports
```

It stores scalar identity and window fields, ordered source report IDs and
digests, the complete objective document, the canonical report document and its
SHA-256 digest.

The historical objective rows are exposed through:

```text
risk_platform.operational_service_level_objective_metric_history
```

Current report versions are selected within the exact objective-policy,
operational-policy, schedule, calendar, portfolio, risk-limit policy, mandate,
model and through-session grain by:

```text
calculated_at DESC
calculation_id DESC
```

The current views are:

```text
risk_platform.latest_operational_service_level_objective_reports
risk_platform.current_operational_service_level_objective_status
risk_platform.current_operational_service_level_objective_exceptions
```

The status view exposes one row per current objective. The exceptions view
contains current `missed` and `insufficient` rows; it does not discard the base
history.

## Reconciliation

`sql/operational_service_level_objectives_consistency_checks.sql` verifies:

- every source report ID and SHA-256 pair references retained source evidence;
- source policy, schedule, calendar, portfolio, mandate and date-window metadata
  match the objective report;
- objective source metrics and units are canonical;
- success, failure, missing and attainment counts reconcile;
- objective and overall statuses follow the declared rules;
- calculation IDs and current grains remain unique;
- current selection has not chosen a stale correction; and
- historical, current and exception view row counts match their declared grains.

The live PostgreSQL CI contract also proves deterministic replay, conflicting
identity rejection, correction ranking and append-only update/delete blocking.

## Evidence Contract

Each report records:

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

This increment provides calculation, optional local JSON, append-only PostgreSQL
history, current serving views and reconciliation. It does not add a dashboard,
paging, automatic readiness-gate integration, schedule activation, external
notification delivery or automated remediation. A stable dashboard/query
contract is the next dependency layer; controlled gate integration follows only
after that evidence is reviewable.
