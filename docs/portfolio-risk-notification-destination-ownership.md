# Portfolio Risk Notification Destination Ownership

## Outcome

This increment defines a reviewed, secret-free ownership contract for external
portfolio-risk notification destinations:

```text
committed destination metadata
  + accountable owner and contact
  + bounded recipient scope
  + explicit lifecycle-event allow-list
  + change-controlled activation review
  -> deterministic destination fingerprint
  -> credential-free activation evidence
  -> no endpoint resolution or delivery
```

Primary arc42 block: `governance`, with a read-only orchestration validation
interface.

The contract is implemented by:

```text
config/notification_destinations.yaml
src/orchestration/portfolio_risk_notification_destination_contract.py
```

## Secret-free endpoint reference

The repository stores only the name of the environment variable that an operator
would populate locally:

```yaml
endpoint_env: RISK_NOTIFICATION_WEBHOOK_URL
```

The contract rejects actual URLs, embedded credentials and other endpoint values in
committed destination metadata. Validation summaries expose the environment-variable
name but never its value.

## Ownership and recipient scope

Every destination identifies:

- a stable destination ID;
- an accountable owner team;
- an operational contact role;
- a bounded recipient scope;
- one declared purpose;
- the `internal` data classification; and
- a sorted, explicit lifecycle-event allow-list.

The initial demonstration destination is owned by `risk-operations`, uses the
`risk-operations-oncall` role contact and is limited to opened, escalated and
resolved breach events.

## Disabled-by-default activation

Committed activation remains disabled and carries no approval evidence:

```yaml
activation:
  enabled: false
  change_request_id: null
  reviewed_by: []
  reviewed_at: null
  review_expires_at: null
```

An enabled destination must bind a change-request ID, at least one independent
reviewer, a timezone-aware review time and a review expiry no more than 366 days
after review. The destination owner contact cannot act as the sole activation
reviewer.

At a deterministic evaluation time, activation is classified as:

```text
disabled
not_yet_reviewed
active
review_expired
```

An expired review is visible evidence and is not equivalent to an active
destination.

## Operator validation

```bash
.venv/bin/python \
  -m src.orchestration.portfolio_risk_notification_destination_contract \
  --destination-id risk-operations-webhook \
  --evaluated-at 2026-04-01T12:00:00Z \
  --summary-json .demo/notification-destination.json
```

The validator has no `--execute` option. It does not resolve the endpoint
environment variable, open a network connection, write a delivery attempt, mutate
the outbox or acknowledge a breach.

## Activation package

Before a real destination is enabled, a review package should contain:

1. The destination ID and deterministic fingerprint.
2. Owner team, contact role and recipient scope.
3. The exact lifecycle-event allow-list.
4. The change-request ID and independent reviewer identities.
5. Review and expiry timestamps.
6. Evidence from a controlled local receiver or fake transport.
7. Rollback instructions that set activation and delivery back to disabled.
8. Confirmation that the endpoint value remains outside the repository.

This contract records review authority; it does not itself authorize the existing
webhook sender. A later P4d increment must bind initial delivery and manual retry
execution to the exact active destination fingerprint.

## Boundary

This increment performs no external request, delivery attempt, acknowledgement,
outbox mutation, provider request, cloud scheduling, infrastructure deployment or
`terraform apply`. It does not create a recipient, provision a webhook or store an
endpoint URL.
