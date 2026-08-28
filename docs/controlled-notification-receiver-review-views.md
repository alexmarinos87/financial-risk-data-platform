# Notification Activation and Controlled Receiver Review Views

Primary arc42 block: `warehouse`.

## Goal

Turn retained activation checklists and controlled-receiver rehearsals into one
current, reviewer-oriented state per notification destination:

```text
append-only activation checklists
  + append-only controlled receiver rehearsals
  -> deterministic latest evidence per destination
  -> explicit review status
  -> ready and review-required queues
```

These are query contracts, not authority to enable notification delivery.

## Current-version selection

The latest checklist for a destination is selected by:

```text
reviewed_at DESC
checklist_id DESC
```

The latest rehearsal is selected independently by:

```text
recorded_at DESC
record_id DESC
```

Historical rows remain immutable. A newer checklist can therefore supersede a
successful rehearsal without deleting either item. The destination remains under
review until the newer checklist has its own completed rehearsal.

## Review states

The current review view exposes exactly one of:

```text
checklist_incomplete
checklist_not_yet_active
checklist_expired
rehearsal_missing
rehearsal_evidence_conflict
rehearsal_superseded
rehearsal_rejected
rehearsal_failed
ready
review_required
```

Precedence is fail closed. Incomplete or time-invalid checklist evidence outranks
rehearsal evidence. A rehearsal whose stored destination or authority identity no
longer reconciles is classified as an evidence conflict. A valid rehearsal against
an older checklist is classified as superseded rather than ready.

`incomplete_controls_json` lists every required checklist control that is not true.
The review view also retains checklist and rehearsal identities, timing, host and
event allow-lists, request counts, failure codes and receiver model metadata.

## Serving views

```text
risk_platform.latest_notification_activation_checklists
risk_platform.latest_controlled_notification_receiver_rehearsals
risk_platform.current_notification_activation_rehearsal_review
risk_platform.current_notification_activation_review_failures
risk_platform.current_notification_activation_ready
```

`current_notification_activation_ready` contains only current active checklists
with a completed, internally consistent rehearsal against the exact same checklist,
destination fingerprint and authority identity.

## PostgreSQL evidence

The transaction fixture covers eight reviewer states:

- ready;
- incomplete checklist;
- not-yet-active checklist;
- expired checklist;
- missing rehearsal;
- rejected rehearsal;
- failed rehearsal; and
- a newer checklist superseding an older completed rehearsal.

Reconciliation checks prove unique current grains, correct latest selection,
incomplete-control expansion, status precedence, and exact ready/failure
partitions.

## Safety boundary

The views are read only and expose no endpoint URL or path, endpoint value,
credential, environment value, payload body, response body or PostgreSQL DSN.
Ordinary validation performs no DNS lookup, socket open, external request,
delivery-attempt write, outbox mutation, acknowledgement, cloud schedule
activation, deployment or `terraform apply`.
