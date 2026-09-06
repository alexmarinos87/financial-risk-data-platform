# Exact-head pull-request check evidence

Primary arc42 block: `engineering-controls`. Goal #195 under #76.

## Decision

A green Actions workflow is not the full check inventory. PR #191 demonstrated
this: four workflow jobs passed while a separate GitGuardian check failed.
`scripts/pr_check_evidence.py` summarizes both check runs and commit statuses
without assigning engineer acceptance, dismissing a finding, or merging a PR.
This complements the local Git/overnight package in `scripts/morning_review.py`;
it does not replace it or change any branch rule.

Call `summarize_checks` with the exact full head SHA, every page from the check
runs endpoint using `filter=latest`, and every page from the combined commit
status endpoint for that same SHA. Each page's total must reconcile with the
collected distinct IDs. Check heads and status-page heads must match exactly.
An incomplete or changing page inventory is an error, never a partial success.
The bound is ten pages / 1,000 rows per source; unsupported names or API states
fail closed. No network or filesystem I/O occurs in this module.

## Repository-specific diagnostic policy

The policy expects Python readiness, PostgreSQL contract, Security guardrails
and Infrastructure validation from GitHub Actions App 15368, plus GitGuardian
Security Checks from App 46505. These identities were observed on this repository;
they are not a universal GitHub policy or a substitute for reading branch rules.
Same-named checks from another app do not satisfy the expectation. Every
additional observed failure still counts, including legacy commit statuses.

Only `success` counts as passed. Missing, queued, neutral and skipped evidence
remain incomplete. This is deliberately more conservative than GitHub's possible
branch-rule semantics. Repeated latest app/name or context identities remain
ambiguous: the tool does not guess which result to discard. Provider-reported
failures are not waived by prose describing a suspected false positive.

Output contains selected IDs, names and result fields only, not API output
bodies, descriptions or arbitrary provider URLs. Results are detached and ordered.
`engineer_acceptance` is always `pending`, `branch_rules_verified` is false and
`merge_authorized` is false, even when all reported checks passed.

## Validation and limits

```bash
python -m pytest -q tests/unit/test_pr_check_evidence.py
make quality-check
make security-check
```

Tests reproduce a workflow-green/scanner-red inventory, missing/spoofed app
checks, additional failing statuses, pagination gaps, stale SHA evidence,
ambiguous reruns, strict types and provider-content exclusion.

The input remains captured evidence, not authenticated offline provenance.
This does not prove a workflow tested the latest base/merge tree, that GitHub has
already created every future check, or that a reviewer accepted the diff. The
successor collects current PR/compare evidence with read-only API calls. GitHub
also limits reference-level check listing to recent suites; this tool does not
claim an unlimited historical audit. Existing scanner incidents stay untouched.

Primary API contracts:
- <https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference>
- <https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference>
