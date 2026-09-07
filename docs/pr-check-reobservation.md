# Reobserve checks before reporting a PR stack

Primary arc42 block: `engineering-controls`. Goal #210 follows #209 / PR #214.

## Reproduced problem and change

Rechecking branch SHAs does not detect check results changing at the same commit.
On #203, an injected scanner changed from success to failure after its first read;
head, base and main stayed identical and the collector still reported technical
success. No fresh check/status inventory was requested before that report.

`scripts/pr_stack_review.py` now uses one `_observe_checks` composition for both
observations. Each calls the existing bounded pagination readers and the exact-head
reconciler, including #214's combined-status aggregate check. After initially
collecting every candidate, it obtains a second complete inventory for each exact
head and requires equality of the detached projected results. Changed IDs, names,
App identities, states, conclusions, added/removed entries or incomplete responses
invalidate collection, even when a replacement would still have a passing outcome.
Only row ordering and unused provider bodies/URLs/descriptions are ignored.

All PR identity and main rereads finish after the last inventory reobservation.
Detected changes raise a fixed error; the CLI exits 1 without a JSON success report.
There is no retry-until-green behavior or fallback to the earlier observation.
Stable failure/missing-check results still emit their normal technical blockers,
and a blocked predecessor still blocks its descendants. Engineer acceptance,
independent review and merge permission remain separate and pending/false.

## Report and request cost

Existing fields remain; the report adds:

```json
{
  "check_inventory_observations": 2,
  "check_inventory_consistency": "two_equal_observations_non_atomic"
}
```

For P selected PRs, single-page inventories use `2 + 7P` GETs (177 at the existing
25-PR ceiling). With the existing ten-page ceiling for both inventory types in
both observations, the upper bound is `2 + 43P` GETs (1,077). Existing byte, page,
row, endpoint, socket-timeout and selection bounds are unchanged. Select a small
stack; authentication may be needed within GitHub rate limits. Rate-limit or
transport failures reject collection, never silently omit a page. The socket
timeout is not a whole-command deadline. No workflow or automatic invocation is
added; every execution still requires explicit `--read-github`.

## Validation and limits

```bash
python -m pytest -q tests/unit/test_pr_check_evidence.py \
  tests/unit/test_pr_stack_review.py \
  tests/unit/test_pr_combined_status_reconciliation.py \
  tests/unit/test_pr_check_reobservation.py
make quality-check
make security-check
make readiness-check
```

Tests exercise the actual collector/CLI with injected state transitions, mutable
responses, two-page inventories and the maximum selection. They do not access
GitHub or real credentials. Separate connected reads are not evidence of live
execution of the new urllib transport.

Two matching observations are not an atomic snapshot, a lock or continuous
monitoring. A transient change and reversal between observations, or a change
immediately after the final observation, can remain undetected. The check projection
does not attest runner provenance or the merge tree tested by CI. Branch rules,
independent review and final-diff engineer acceptance remain unverified.
No API writes, scanner dismissal, check override, merge, database, notification,
activation-default change, deployment or Terraform apply is introduced.
