# Retry Execution Destination Authority

Primary arc42 block: `orchestration`, with an append-only `warehouse` evidence
binding.

## Goal

A governed notification retry must use one current, reviewed destination rather
than only a delivery endpoint value:

```text
exact retained retry plan
  + current retry evidence
  + shared PostgreSQL delivery lock
  + active destination ownership contract
  -> destination authority refreshed under the lock
  -> approved event-type check
  -> one request per authorised event
  -> terminal retry history
  -> append-only destination binding
```

## Enforcement order

The retry executor obtains the existing delivery lock, rebuilds the current
retry plan, and proves that it still matches the retained plan. It then resolves
the destination authority using the same clock-derived execution instant and
checks the exact endpoint environment-variable identity and event types. Only
after those checks may the first transport call occur.

The execution identity binds the destination ID, fingerprint and authority ID.
An expired review, disabled destination, endpoint-environment change or event
outside the allow-list therefore fails before delivery.

## Durable evidence

The existing terminal retry record remains backward compatible. A separate
append-only row in
`risk_platform.portfolio_risk_notification_retry_destination_bindings` links
that terminal `record_id` to:

- the authority ID;
- destination ID and fingerprint;
- endpoint environment-variable name, never its value;
- authority evaluation time and reviewed event types; and
- the canonical, credential-free authority document.

Exact retries converge on the existing binding. Reusing a terminal record with
different destination evidence fails closed. UPDATE and DELETE are rejected by
PostgreSQL triggers.

A failure before destination authority exists has no binding. A completed
execution, or a failure after authority was observed, retains the binding.

## Safety boundary

Committed destination activation, webhook delivery and retry execution remain
disabled. Ordinary CI uses fake transports and performs no external webhook
request. The evidence contains no endpoint URL, payload body, response body,
PostgreSQL DSN, credential or environment value. It does not acknowledge a
breach, mutate the outbox, activate cloud scheduling, deploy infrastructure or
run `terraform apply`.
