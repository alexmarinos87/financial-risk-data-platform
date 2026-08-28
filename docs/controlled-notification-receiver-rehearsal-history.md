# Append-Only Controlled Receiver Rehearsal History

Primary arc42 blocks: `orchestration` and `warehouse`.

## Goal

Retain the outcome of each controlled receiver rehearsal without turning the
in-memory receiver into an external delivery path:

```text
activation-ready checklist
  + controlled receiver configuration
  + accepted receipt evidence
  + bounded terminal outcome
  -> canonical rehearsal record
  -> append-only PostgreSQL history
  -> exact request replay convergence
```

The record model is
`portfolio-risk-controlled-receiver-rehearsal-record-v1`.

## Retained evidence

Every record binds:

- an operator request ID;
- the complete canonical activation checklist;
- destination, authority and checklist identities;
- the controlled receiver model version;
- sorted host and event-type allow-lists;
- the simulated successful response status;
- ordered accepted receipts;
- attempted, accepted, unique-key and duplicate counts;
- start, finish and record timestamps; and
- a terminal outcome and bounded failure code.

The activation checklist is retained separately in
`risk_platform.notification_activation_checklists`. Rehearsals reference it by
`checklist_id`, allowing a later review view to show a checklist even when no
successful rehearsal exists.

## Terminal outcomes

```text
completed
rejected_before_request
failed_during_rehearsal
```

A completed record requires at least one accepted request and an exact receiver
summary. A rejected-before-request record has no accepted request or receiver
summary. A failed-during-rehearsal record retains the partial summary and proves
that one selected request was rejected after zero or more accepted requests.

Arbitrary exception text is not retained. Failure evidence is limited to:

```text
validation_error
storage_error
unexpected_error
```

## Replay and conflict behaviour

The operator `request_id` is the append-only idempotency boundary:

```text
same request ID + same canonical evidence
  -> return the retained record

same request ID + different evidence
  -> reject
```

The checklist and rehearsal tables both reject UPDATE and DELETE operations
through PostgreSQL triggers. Exact checklist retries converge by deterministic
`checklist_id` and SHA-256.

## PostgreSQL contract

The PostgreSQL 16 contract records one example of each terminal state, repeats
the completed request, rejects conflicting request reuse, verifies mutation
rejection and runs the full reconciliation suite.

The reconciliation checks cover checklist references, count arithmetic, terminal
status rules, review-window alignment and every no-side-effect declaration.

## Safety boundary

Rehearsal history records host names and payload SHA-256 values only. It records
no endpoint URL or path, endpoint value, credential, environment value, payload
body, response body or PostgreSQL DSN.

Ordinary validation performs no DNS lookup, socket open, external request,
delivery-attempt write, outbox mutation, acknowledgement, cloud scheduling,
infrastructure deployment or `terraform apply`. Committed destination activation,
webhook delivery and governed retry execution remain disabled.
