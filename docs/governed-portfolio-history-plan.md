# Governed Portfolio History Plan

## Outcome

This command turns one requested historical range into exact, deterministic segments where one portfolio mandate and one risk-limit policy version are simultaneously valid:

```text
requested inclusive date range
  -> all configured portfolio mandates
  -> all configured risk-limit policy versions
  -> mandate-policy interval intersections
  -> exact gap and overlap validation
  -> ordered plan-only execution segments
```

The pure planner is `src/analytics/governed_portfolio_segments.py`. The operator command is `src/orchestration/plan_governed_portfolio_history.py`.

## Operator command

```bash
.venv/bin/python -m src.orchestration.plan_governed_portfolio_history \
  --portfolio-id us-tech-equal \
  --policy-id us-tech-standard \
  --portfolio-config config/portfolios.yaml \
  --risk-limit-config config/portfolio_risk_limits.yaml \
  --start-date 2026-01-01 \
  --end-date 2026-12-31 \
  --covariance-window 20 \
  --summary-json .demo/governed-portfolio-history-plan.json
```

The command is plan-only. It does not acquire a lock, read analytical Parquet, execute a calculation stage, write curated data, load PostgreSQL, send a notification, acknowledge a breach, deploy infrastructure, or run `terraform apply`.

## Temporal semantics

Mandate and policy periods use inclusive `effective_from` and exclusive nullable `effective_to` bounds. The requested `start_date` and `end_date` are inclusive.

A segment is the inclusive intersection of:

```text
requested range
portfolio mandate period
risk-limit policy period
```

Every calendar date in the requested range must be covered exactly once. A mandate gap, policy gap, overlap, missing period, or incompatible policy parameter fails the complete plan. The planner never silently skips an uncovered date and never chooses a newest policy as a fallback.

## Compatibility rules

Every intersecting policy version must match:

- the requested portfolio;
- the requested policy ID;
- the requested covariance window; and
- the attribution model's 252-day annualisation basis.

All mandate versions must match the requested portfolio. IDs must be unique and schedules must be non-overlapping. Configuration parsers provide the primary contract; the planner repeats defensive validation for injected or programmatic callers.

## Deterministic identity

Each segment receives a deterministic ID binding:

```text
segment start and end
mandate fingerprint
policy fingerprint
governed-portfolio-segments-v1
```

The plan ID binds the requested range and the ordered segment IDs. Reordering the source YAML entries does not change the plan. Changing a mandate, policy, date boundary, threshold fingerprint, or requested range produces a different identity.

## Output evidence

The JSON summary records:

- deterministic plan ID;
- requested range and calendar-day count;
- segment count;
- mandate and policy-version counts;
- covariance and annualisation parameters;
- ordered segment IDs and inclusive dates;
- mandate metadata for each segment;
- policy-version metadata for each segment;
- the governed stage sequence; and
- planned segment and stage-invocation counts.

The planner caps one request at 100 segments. An unexpectedly fragmented configuration must be reviewed or split into smaller requests.

## Execution boundary

The existing governed portfolio cycle still requires one range to fit within one mandate and one policy version. This planner provides the exact ranges an operator or future explicitly controlled executor would pass to that cycle.

Automatic multi-segment execution is intentionally separate. Before adding it, the repository needs explicit checkpointing, failure/resume semantics, per-segment review evidence, and a clear decision on whether to stop or continue after a segment failure.
