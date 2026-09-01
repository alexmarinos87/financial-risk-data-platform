# Read-Only Notification Execution Readiness Gate

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

P4d5a adds a deterministic, read-only decision before notification execution:

```text
reviewed delivery configuration
  + reviewed destination fingerprint
  + current activation and receiver evidence
  + current rotate / disable / rollback evidence
  + unresolved persistence ambiguity
  -> one exact allow or block decision
```

The model is
`portfolio-risk-notification-execution-readiness-gate-v1`.

The gate evaluates `initial` and `retry` execution independently. It does not
send a notification, resolve an endpoint value, write a delivery attempt, mutate
the outbox, acknowledge a risk event, or deploy infrastructure.

## Evidence

The gate binds the current reviewed evidence for one destination:

- webhook delivery configuration and fingerprint;
- retry-execution policy and fingerprint;
- destination ID, fingerprint, endpoint environment-variable identity, and
  activation status;
- activation checklist, authority, receiver rehearsal, and current review
  status;
- latest destination transition rehearsal and current transition status; and
- unresolved retry persistence uncertainty, including unbound ambiguity.

PostgreSQL access is read-only. The reader selects from:

```text
risk_platform.current_notification_activation_rehearsal_review
risk_platform.current_notification_destination_transition_review
risk_platform.current_notification_retry_destination_ambiguities
```

The gate rejects duplicate current grains and bounded ambiguity overflow rather
than choosing evidence nondeterministically.

## Blocking reasons

Blocking reasons use fixed policy order:

```text
delivery_disabled
retry_execution_disabled
destination_not_active
configuration_mismatch
activation_review_missing
activation_not_ready
activation_identity_mismatch
transition_review_missing
transition_rehearsal_missing
transition_rehearsal_superseded
transition_not_ready
transition_identity_mismatch
persistence_ambiguity
```

The order is part of the decision identity. A changed configuration, destination
fingerprint, checklist, authority, transition rehearsal, ambiguity event,
execution kind, evaluation time, or reason set produces a different
`decision_id`.

An `allow` decision therefore means all of the following are true at the
evaluation time:

```text
delivery configuration enabled
destination review active
delivery endpoint environment identity matches the destination
activation and controlled-receiver review ready
transition rehearsal ready and matched to current activation
no unresolved persistence ambiguity
retry execution enabled when execution_kind = retry
```

## Fail-closed behaviour

The gate blocks rather than guessing when evidence is missing, stale,
superseded, mismatched, or ambiguous. An ambiguity without a destination binding
also blocks because the system cannot safely prove that it belongs elsewhere.

The gate is evidence, not execution authority on its own. P4d5b will retain the
decision append-only. P4d5c will refresh and enforce an exact current `allow`
decision while holding the shared notification delivery lock.

## Safety boundary

Delivery and manual retry execution remain disabled by default. Normal
validation:

- performs no DNS lookup or socket operation;
- performs no provider or webhook request;
- writes no delivery attempt;
- mutates no outbox or acknowledgement;
- stores no endpoint value, credential, payload body, response body, or DSN; and
- does not run `terraform apply`.
