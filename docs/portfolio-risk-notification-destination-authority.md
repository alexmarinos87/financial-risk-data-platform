# Notification Destination Execution Authority

## Outcome

This increment defines one reusable, secret-free authority contract for notification
execution:

```text
reviewed destination metadata
  + exact destination fingerprint
  + clock-derived evaluation time
  + delivery endpoint-environment identity
  + selected event types
  -> active or rejected destination authority
  -> no endpoint resolution and no delivery
```

Primary arc42 block: `orchestration`.

The contract is implemented in:

```text
src/orchestration/portfolio_risk_notification_destination_authority.py
```

## Authority boundary

`portfolio-risk-notification-destination-authority-v1` resolves one destination by
its stable ID and requires the delivery configuration to name the same endpoint
environment variable as the destination contract.

For executable use, the destination must be `active` at the supplied clock-derived
evaluation time. `disabled`, `not_yet_reviewed`, and `review_expired` destinations
fail closed.

The contract also compares every selected notification event type with the
destination event allow-list before an execution path may make its first request.
Duplicate event types are collapsed into a sorted canonical set for deterministic
evidence.

## Deterministic evidence

The authority ID binds:

- authority model version;
- destination ID;
- destination fingerprint;
- endpoint-environment identity;
- exact evaluation time; and
- canonical selected event types.

The destination fingerprint already binds ownership, recipient scope, data
classification, event allow-list, activation review identity, review time, and
expiry. A reviewed change therefore creates a different authority identity.

Evidence records the endpoint environment-variable name but no endpoint value,
full URL, credential, payload, response body, or database DSN.

## Plan and execution use

Callers may evaluate an inactive destination with `require_active=False` to produce
read-only plan evidence. Any path that can perform delivery must use the default
`require_active=True` behavior immediately before side effects.

Binding the initial-delivery and retry-execution adapters to this shared authority is
a separate dependent PR. This keeps the reusable authority contract independently
reviewable before it changes an existing side-effect boundary.

## Safety

This increment:

- performs no network request;
- resolves no endpoint value;
- writes no delivery attempt;
- mutates no outbox or acknowledgement;
- activates no cloud schedule;
- deploys no infrastructure; and
- does not run `terraform apply`.

Ordinary CI performs no external request. Committed destination activation,
webhook delivery, and retry execution remain disabled.
