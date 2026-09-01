# Notification Destination Transition Plan

Primary arc42 block: `orchestration`.

## Goal

Create deterministic, delivery-free evidence for a notification destination
rotation, disablement, or rollback:

```text
current reviewed destination
  + target reviewed or disabled destination
  + declared transition operation
  -> exact metadata comparison
  -> deterministic transition plan
  -> target authority requirement
  -> no endpoint resolution or delivery
```

The model is
`portfolio-risk-notification-destination-transition-plan-v1`.

## Operations

### Rotate

A rotation requires:

- active current and target destination reviews at `planned_at`;
- unchanged owner, purpose, recipient scope, classification, channel, and event
  allow-list;
- a changed endpoint environment-variable identity; and
- different deterministic destination fingerprints.

The plan explicitly states that current authority cannot authorise the target.
A target authority must match the target fingerprint, endpoint environment
identity, destination ID, and exact planning timestamp.

### Disable

Disablement requires an active current destination and a disabled target using
the same endpoint environment-variable identity. The target contains no review
evidence and must not receive execution authority.

### Rollback

Rollback requires a disabled current destination, a newly reviewed active
target using the prior endpoint environment-variable identity, and an exact
`prior_plan_id`. A previous authority is not reusable: the fresh rollback
review changes the target fingerprint, so a new target authority is required.

## Scope control

The transition contract rejects changes to:

- destination ownership;
- purpose or recipient scope;
- data classification;
- channel; and
- lifecycle event allow-list.

Those changes require a separate destination-governance increment rather than
being hidden inside an endpoint rotation.

## Deterministic identity

The plan ID binds:

```text
operation
planned_at
destination ID
current and target fingerprints
current and target endpoint environment-variable identities
current and target activation evidence
prior plan identity when rolling back
authority requirement
```

Any changed review, endpoint identity, operation, time, or predecessor plan
produces a different plan ID.

## Safety boundary

The plan stores environment-variable names but never endpoint values or URLs.
It has no `--execute` option and performs no DNS lookup, socket operation,
external request, delivery-attempt write, outbox mutation, acknowledgement,
cloud scheduling, infrastructure deployment, or `terraform apply`.

Committed notification destination activation and delivery remain disabled.
