# Notification Activation Checklist

Primary arc42 block: `orchestration`.

## Goal

Provide one deterministic, secret-free review package before notification delivery
can move from disabled local governance evidence towards a controlled receiver
exercise:

```text
reviewed destination authority
  + accountable reviewers
  + exact activation controls
  + bounded review lifetime
  -> canonical activation checklist
  -> ready or visibly incomplete
```

The contract is implemented in
`src/orchestration/notification_activation_checklist.py`.

## Required controls

The checklist requires an exact boolean decision for each of these controls:

- recipient ownership has been signed off;
- the destination review is active;
- endpoint environment-variable identity is confirmed;
- the event allow-list is confirmed;
- receiver-side idempotency is confirmed;
- rollback has been tested;
- ambiguous-outcome handling is documented; and
- credentials remain outside the repository.

A checklist may be retained while incomplete, but `activation_ready` is true only
when every required control is true. Missing, unknown or non-boolean controls fail
closed.

## Deterministic evidence

The checklist identity binds:

- destination ID and fingerprint;
- destination authority ID;
- canonical reviewer identities;
- review and expiry timestamps; and
- every control decision.

Changing any review decision creates a different checklist ID. Duplicate reviewers,
empty reviewer sets, naive timestamps and non-positive review windows are rejected.

## Safety boundary

The checklist records no endpoint URL, endpoint value, credential, payload body,
response body, database DSN or environment value. Building or validating it performs
no network request, notification delivery, attempt write, acknowledgement, cloud
schedule activation, infrastructure deployment or `terraform apply`.

Committed notification destination activation, webhook delivery and governed retry
execution remain disabled. A later PR can consume this review package in a controlled,
no-network receiver rehearsal without weakening the existing delivery gates.
