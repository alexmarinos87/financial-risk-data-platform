# Managed Notification Worker Plan

Primary arc42 block: `orchestration`.

## Goal

Describe one bounded managed notification worker without activating a scheduler
or executing notification delivery:

```text
reviewed worker configuration
  + delivery and retry policy bounds
  + reviewed destination contract
  + deterministic planning instant
  -> disabled | blocked | would_schedule
  -> exact future schedule slot
  -> no scheduler or delivery side effect
```

The configuration model is:

```text
portfolio-risk-notification-worker-config-v1
```

The plan model is:

```text
portfolio-risk-notification-worker-plan-v1
```

## Committed default

The committed worker is intentionally disabled:

```yaml
workers:
  risk-operations-managed:
    enabled: false
```

The committed webhook delivery, retry execution and notification destination
are also disabled. A default plan therefore reports every applicable blocking
reason rather than implying operational activation.

## Bounded worker contract

Each worker binds:

- one reviewed destination ID;
- a sorted, unique subset of `initial` and `retry` work;
- a fixed UTC interval and bounded deterministic jitter;
- initial and retry batch limits no greater than their delivery policies;
- one shared concurrency slot;
- a bounded execution timeout;
- current readiness status `allowed` with evidence no older than five minutes;
- mandatory suspension on readiness failure;
- mandatory suspension on unresolved persistence ambiguity;
- mandatory suspension when destination review expires; and
- a bounded repeated-failure threshold and cooldown.

`max_concurrency` is exactly one because initial and retry execution share the
repository-wide PostgreSQL notification-delivery advisory lock. The plan
retains the reviewed lock model, scope and key fingerprint but does not acquire
the lock.

## Deterministic scheduling

The next slot is the first fixed-interval boundary strictly after `planned_at`.
A deterministic jitter value is derived from the worker fingerprint and slot
boundary. The same configuration and planning instant therefore produce the
same:

```text
worker fingerprint
schedule boundary
jitter
scheduled_for
plan ID
```

There is no random runtime state and no hidden scheduler clock.

## Plan states

The planner emits exactly one state:

```text
worker disabled                         -> disabled
enabled worker with governed blockers  -> blocked
enabled worker with no blockers        -> would_schedule
```

Blocking reasons have fixed order:

```text
worker_disabled
delivery_disabled
retry_execution_disabled
destination_not_active
endpoint_environment_mismatch
```

A disabled or blocked plan has `activation_action = none`. A valid enabled plan
has `activation_action = would_create`; this is descriptive evidence only and
does not create a cloud or local schedule.

## Execution and suspension evidence

The plan lists one bounded work item for each configured execution kind. The
entrypoints are descriptive module identities; the planner never imports or
invokes their execution functions.

Every future worker cycle must require current retained readiness and refresh it
under the shared delivery lock before the first request. The plan also declares
these suspension conditions:

```text
expired_review
persistence_ambiguity
readiness_failure
repeated_delivery_failure
```

P4e1 does not implement the activation, suspension or resume authority. Those
operations belong to a separate append-only governance increment.

## Operator command

```bash
.venv/bin/python \
  -m src.orchestration.plan_notification_worker \
  --worker-id risk-operations-managed \
  --planned-at 2026-09-05T20:00:00Z \
  --summary-json .demo/notification-worker-plan.json
```

There is no `--execute` option.

## Safety boundary

The planner reads only regular, non-symbolic-link configuration files bounded
to 1 MB. It does not read PostgreSQL evidence, resolve an endpoint value, open a
socket, perform DNS, send a webhook, write a delivery attempt, mutate the
outbox, acknowledge a breach, activate local or cloud scheduling, deploy
infrastructure or run `terraform apply`.

The credential-free plan contains only environment-variable names and reviewed
fingerprints. It does not contain an endpoint value, complete URL, credential,
payload body, response body, environment value or PostgreSQL DSN.
