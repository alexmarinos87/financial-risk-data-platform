# Controlled Notification Receiver Rehearsal

Primary arc42 block: `orchestration`.

## Goal

Exercise the existing webhook transport boundary against a deterministic in-memory
receiver before any external endpoint is activated:

```text
activation-ready checklist
  + approved HTTPS host
  + reviewed event allow-list
  + canonical webhook request
  + stable Idempotency-Key
  -> controlled receiver validation
  -> same-content duplicate handling
  -> credential-free receipt evidence
```

The implementation is
`src/orchestration/controlled_notification_receiver.py`. Instances are callable with
the same endpoint, payload, headers and timeout arguments used by the existing
notification delivery transport.

## Receiver controls

Construction requires a canonical activation checklist whose `activation_ready`
state is true. The receiver also requires explicit approved hosts and event types,
a bounded request count, a bounded payload size and a successful simulated HTTP
status.

Every request must satisfy all of the following before it is accepted:

- the endpoint is HTTPS and contains no username, password or fragment;
- the endpoint host is in the explicit controlled-host allow-list;
- headers are exactly `Content-Type`, `Idempotency-Key` and `User-Agent`;
- the content type is `application/json`;
- the timeout is positive and no greater than 30 seconds;
- the payload is canonical UTF-8 JSON and within 64 KiB;
- the payload contains a safe event ID and reviewed event type; and
- the `Idempotency-Key` equals the payload event ID.

Unknown headers fail closed. This deliberately prevents an ordinary rehearsal from
silently carrying an authorization token, cookie or other unreviewed request data.

## Receiver-side idempotency

The receiver retains only the SHA-256 associated with each accepted
`Idempotency-Key`:

```text
same key + same payload SHA-256      -> accepted duplicate
same key + different payload SHA-256 -> rejected conflict
```

This demonstrates the receiver control required for the distributed failure window
where a remote request may have succeeded before local delivery-attempt persistence
was confirmed.

## Receipt evidence

Each accepted request records:

- request ordinal and controlled receive timestamp;
- endpoint host, never path, query or full URL;
- event ID and event type;
- idempotency key;
- payload SHA-256;
- simulated HTTP status; and
- whether it was a same-content duplicate.

The summary derives a deterministic rehearsal ID from the activation checklist,
receiver contract and receipts. It explicitly states that no payload body, response
body, endpoint path, external request, socket, DNS lookup, delivery-attempt write,
outbox mutation, acknowledgement, infrastructure deployment or `terraform apply`
occurred.

## Activation boundary

This is executable local evidence, not authority to contact an external receiver.
Committed destination activation, webhook delivery and governed retries remain
disabled. A later increment may persist rehearsal results and expose reviewer-facing
readiness views while keeping ordinary CI no-network.
