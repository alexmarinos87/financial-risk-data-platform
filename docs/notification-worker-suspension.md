# Managed-worker suspension decisions

Primary arc42 block: `orchestration`. P4e2c, first independently reviewable layer.

`evaluate_worker_suspension` combines one canonical retained authority transition
with a bounded health observation at an explicit UTC instant. It returns
`no_suspension_required`, `suspend`, or `inactive`. Every result retains
`runtime_permission_granted = false`; none operates a scheduler or performs delivery.

## Decision rules

All selected execution kinds require fresh allowed readiness, matching destination
and delivery fingerprints, and a fresh complete failure-history summary. A
successful initial path cannot mask a failing retry path: consecutive failures
are checked independently per kind. The reviewed threshold is inclusive. Zero
failures must be explicitly observed; an absent history is persistence ambiguity,
not evidence of success. Observations and rows use the plan's maximum readiness
age, with the exact maximum accepted and older evidence rejected as unsafe.

Missing observations, expired or missing destination review, worker configuration
drift, stale or blocked readiness, persistence ambiguity, and repeated failures
produce canonical reasons from the existing authority contract. Malformed,
future-dated, oversized, duplicated, or differently bound evidence raises
`ValidationError`; callers must stop rather than interpret an exception as approval.

Authority expiry is exclusive. Expired, disabled, not-yet-effective and suspended
authority remains inactive. The decision may expose a resume-not-before timestamp,
but passing that timestamp never resumes a worker. A new reviewed lifecycle
transition is still required. No competing state machine is introduced.

## Observation and trust boundary

The observation is an explicit adapter contract, not a claim that this module
queried PostgreSQL. It contains an observation identifier, exact authority ID,
observation time, current worker fingerprint, destination review expiry, up to two
readiness references and up to two per-kind failure summaries. Readiness references
retain source record IDs and document SHA-256; failure summaries retain history
identity and digest, consecutive failure count, and unresolved ambiguity.

The trusted caller must independently select current retained authority, verify
source documents and their provenance, and derive complete chronological failure
summaries. Hash strings and IDs do not authenticate source records. This layer
validates structure, binding, freshness and decision semantics; it does not load or
verify the underlying readiness/history documents or authenticate a reviewer.
No raw endpoint, DSN, credential, payload, response or provider diagnostic is
accepted as an observation field. Identifiers are bounded, not a general-purpose
secret scanner; callers must not put secrets into identifier fields.

## Validation and next boundaries

The validator reconstructs the whole decision from its retained observation and
the supplied exact authority. A recomputed content hash cannot hide changed reasons,
outcome, cooldown or side-effect flags. Focused tests cover both execution kinds,
missing sources, threshold and freshness boundaries, expiry, malformed evidence,
changed scope, initial-only workers and permanent no-runtime-permission behavior.

Run `python -m pytest -q tests/unit/test_notification_worker_suspension.py` plus the
repository quality and security checks. The next isolated layers bind a suspension
decision to the existing stop transition and then persist both atomically. No
configuration defaults, database schema, scheduler, transport, workflow, cloud
resource or infrastructure activation changes in this layer. No `terraform apply`.
