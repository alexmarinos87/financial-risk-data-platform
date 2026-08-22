# Portfolio Risk-Limit Breach Lifecycle

## Outcome

This increment turns current portfolio risk-limit evaluations into a deterministic
operational lifecycle without adding a second mutable state store:

```text
latest risk-limit metric evaluations
  -> ordered metric transitions
  -> actionable transition view
  -> contiguous breach episodes
  -> open-episode view
  -> reconciliation evidence
```

The source of truth remains
`risk_platform.latest_portfolio_risk_limit_evaluations`. Corrections to retained
risk-limit evidence flow through the existing current-version rule and cause the
transition and episode views to recompute.

This is current operational state derived from retained evidence. It is not an
acknowledgement workflow, notification system, case-management tool or trading
control.

## Transition Contract

The view:

```text
risk_platform.portfolio_risk_limit_metric_transitions
```

orders each metric series within the full policy and analytical grain:

```text
policy_id
policy_fingerprint
portfolio_id
definition_fingerprint
risk-limit model version
attribution model version
weighting method
covariance method
correlation method
covariance window
annualisation basis
metric name
```

Rows are ordered by:

```text
ts_event ASC
ts_ingest ASC
calculation_id ASC
```

Because the input is already the latest current evaluation for each event date
and metric, this ordering describes the current corrected series rather than the
raw history of superseded calculations.

Transition types are:

| Previous | Current | Transition |
| --- | --- | --- |
| none | ok | `initial_ok` |
| none or ok | warning/critical | `opened` |
| warning | critical | `escalated` |
| critical | warning | `deescalated` |
| warning/critical | ok | `resolved` |
| all other combinations | unchanged severity | `unchanged` |

The view also exposes the previous evaluation identity, previous subject key,
integer severity rank and whether the subject changed. Subject changes matter for
the component-concentration metric because a corrected or later attribution can
make a different constituent the largest contributor.

The focused operational subset is:

```text
risk_platform.portfolio_risk_limit_actionable_transitions
```

It contains `opened`, `escalated`, `deescalated` and `resolved` rows. No external
message is sent from this view.

## Episode Contract

The view:

```text
risk_platform.portfolio_risk_limit_breach_episodes
```

creates one episode for each contiguous run of warning or critical observations
within one metric grain.

Continuity means consecutive **available evaluation observations**, not
calendar-day adjacency. Weekends, holidays and other dates without an attribution
snapshot do not break an episode. An `ok` evaluation closes the episode; the next
warning or critical evaluation opens a new episode.

Each episode exposes:

- deterministic `episode_key`;
- policy, definition, model, method and window grain;
- episode sequence within that grain;
- first breach date and last breach date;
- first subsequent `ok` date when resolved;
- `open` or `resolved` state;
- warning, critical and total breach-observation counts;
- subject-change count;
- opening, latest-breach and peak evaluation identities;
- opening, latest and peak subjects and observed values;
- latest and peak breach excess; and
- latest evidence ingest time.

Peak selection is deterministic:

```text
severity rank DESC
breach excess DESC
observed value DESC
event time ASC
ingest time ASC
calculation ID ASC
```

Critical observations therefore outrank warnings. Within the same severity, the
largest threshold excess wins. Remaining fields provide stable tie-breaking.

Current unresolved episodes are available through:

```text
risk_platform.portfolio_risk_limit_open_episodes
```

## Correction Behaviour

Risk-limit evaluations are versioned by calculation ID. The current evaluation
view selects a corrected row within its declared grain by:

```text
ts_ingest DESC
calculation_id DESC
```

The lifecycle views operate only on that current series. A correction can
therefore:

- move an episode start;
- remove or introduce an escalation;
- change the peak observation;
- change the largest constituent subject;
- resolve an episode earlier or later; or
- remove an episode entirely from current operational state.

Superseded risk-limit evaluation rows remain in the history table. The lifecycle
views do not delete or rewrite that evidence.

## Local Operator Flow

After generating and loading risk-limit evaluations, apply the lifecycle views:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_breach_lifecycle_schema.sql
```

Inspect transitions and open episodes:

```sql
SELECT
    policy_id,
    portfolio_id,
    metric_name,
    ts_event,
    previous_status,
    status,
    transition_type,
    subject_key,
    subject_changed
FROM risk_platform.portfolio_risk_limit_metric_transitions
ORDER BY policy_id, metric_name, ts_event;

SELECT
    policy_id,
    portfolio_id,
    metric_name,
    episode_start,
    last_breach_date,
    latest_breach_status,
    peak_status,
    peak_subject_key,
    peak_breach_excess
FROM risk_platform.portfolio_risk_limit_open_episodes
ORDER BY policy_id, metric_name, episode_start;
```

Run focused reconciliation:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_breach_lifecycle_consistency_checks.sql
```

A newly created local PostgreSQL volume applies the lifecycle schema after the
risk-limit schema through the read-only Docker initialization mounts.

## Reconciliation

The focused checks verify that:

- every latest evaluation has one transition row;
- the actionable view contains exactly the actionable transition types;
- every `opened` transition creates one episode;
- every `resolved` transition closes one episode;
- the open-episode view matches unresolved episodes;
- severity ranks and transition/status combinations are valid;
- warning and critical counts reconcile to episode observation counts;
- opening, latest, peak and resolution identities reference transition rows;
- open and resolved nullability contracts are correct;
- episode keys are unique;
- episodes do not overlap within one metric grain; and
- an open episode has no later current evaluation.

## Boundary

This increment deliberately does not add:

- durable acknowledgement or ownership;
- comments, exceptions or waivers;
- notification delivery;
- email, Slack, webhook or paging integration;
- automatic escalation timers;
- position changes or trade blocking;
- scheduling, deployment or `terraform apply`.

The next bounded increment can use the actionable transition view to create an
idempotent notification outbox while still keeping delivery disabled.
