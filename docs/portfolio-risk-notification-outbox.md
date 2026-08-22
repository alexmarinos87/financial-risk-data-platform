# Portfolio Risk Notification Outbox

## Outcome

This increment converts current actionable portfolio risk-limit transitions into
durable, replay-safe notification **candidates**:

```text
current actionable risk-limit transitions
  -> deterministic notification event identity
  -> credential-free JSON payload
  -> pending or suppressed disposition
  -> content-addressed Parquet
  -> immutable PostgreSQL outbox history
  -> current, pending, suppressed and summary views
```

It does not deliver email, Slack, webhooks, pages or any other external message.
The outbox is evidence for a later delivery adapter, not a claim that notification
has occurred.

## Event Contract

The model version is:

```text
portfolio-risk-notification-outbox-v1
```

One event is produced for each current lifecycle transition:

| Transition | Event type | Disposition |
| --- | --- | --- |
| `opened` | `breach_opened` | `pending` |
| `escalated` | `breach_escalated` | `pending` |
| `deescalated` | `breach_deescalated` | `suppressed` |
| `resolved` | `breach_resolved` | `pending` |

De-escalations are retained but deliberately suppressed with:

```text
deescalation_not_routed
```

This keeps the lifecycle observable without creating an implied production
routing policy. A later increment can change routing through a new model version
rather than rewriting this evidence.

The deterministic event ID binds:

```text
outbox model version
policy fingerprint
metric name
source evaluation calculation ID
previous evaluation calculation ID
transition type
```

An identical rerun therefore produces the same event ID and no duplicate
Parquet record. Corrections to the current risk-limit series produce different
source calculations and therefore distinguishable candidates.

## Payload

`payload_json` contains only reviewed operational evidence:

- event and transition identity;
- policy ID and fingerprint;
- portfolio ID, definition fingerprint and base currency;
- metric, subject, thresholds, status and breach excess;
- source evaluation identities; and
- source event and ingest timestamps.

The payload does not include:

- PostgreSQL DSNs;
- provider API keys;
- email addresses;
- webhook URLs;
- access tokens;
- raw provider responses; or
- local filesystem paths.

## Local Generation

Risk-limit evaluations must already be loaded into PostgreSQL and the breach
lifecycle schema must be applied. Then generate candidates for a bounded range:

```bash
.venv/bin/python -m src.orchestration.run_portfolio_risk_notification_outbox \
  --policy-id us-tech-standard \
  --start-date 2026-01-01 \
  --end-date 2026-03-31 \
  --summary-json .demo/portfolio-risk-notification-outbox-summary.json
```

The command reads only:

```text
risk_platform.portfolio_risk_limit_actionable_transitions
```

It caps one request at 10,000 events. The summary never prints the DSN and
records:

```json
{
  "delivery": {
    "performed": false,
    "external_destinations": 0,
    "reason": "delivery_not_implemented"
  }
}
```

Candidates are written to:

```text
data/curated/portfolio_risk_notification_outbox
```

Each event is published independently through the content-addressed writer.
Partial local publication is replay-safe.

## Warehouse Loading

A dry run inventories Parquet without opening a PostgreSQL connection:

```bash
.venv/bin/python -m src.warehouse.portfolio_risk_notification_outbox_loader \
  --dry-run
```

Apply the schema and load the candidates:

```bash
docker compose exec -T postgres psql -U risk_user -d risk_platform \
  < sql/portfolio_risk_notification_outbox_schema.sql

.venv/bin/python -m src.warehouse.portfolio_risk_notification_outbox_loader \
  --dsn postgresql://risk_user:risk_password@localhost:5433/risk_platform
```

The history table is:

```text
risk_platform.portfolio_risk_notification_outbox
```

The loader uses `ON CONFLICT (event_id) DO NOTHING`. Outbox evidence is immutable;
a replay converges without updating a previously retained event.

Current and reporting views are:

```text
risk_platform.current_portfolio_risk_notification_outbox
risk_platform.portfolio_risk_notification_pending
risk_platform.portfolio_risk_notification_suppressed
risk_platform.portfolio_risk_notification_outbox_summary
```

The current view joins retained outbox rows to the current actionable-transition
view. When a corrected evaluation changes the lifecycle, superseded candidates
remain in history but disappear from current operational views.

## Bounds

The PostgreSQL transition reader requires one policy and a completed end date.
The Parquet warehouse reader retains the repository's local safety bounds:

- at most 4,096 files;
- at most 1 GB of physical input;
- at most 250,000 rows;
- symbolic-link rejection; and
- unsafe-file rejection.

## Reconciliation

`sql/portfolio_risk_notification_outbox_consistency_checks.sql` verifies:

- source and previous evaluation references;
- current transition metadata;
- unique event IDs;
- pending and suppressed disposition rules;
- payload identity;
- one current candidate per actionable transition;
- stale candidates excluded from current views; and
- pending, suppressed and summary row counts.

## Boundary

This increment deliberately does not implement:

- external delivery;
- delivery attempts or retries;
- channel destinations;
- acknowledgement or ownership;
- escalation timers;
- personal data or recipient directories;
- scheduling;
- deployment; or
- `terraform apply`.

The next delivery-oriented slice would require an explicitly configured adapter,
idempotency key, attempt history and retry/dead-letter policy. Until then,
`pending` means eligible candidate—not sent.
