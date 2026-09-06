# Read-only pull-request stack review

Primary arc42 block: `engineering-controls`. Goal #196 follows #195 / PR #200.

## Decision and operator path

The notification diagnostic chain has several unmerged predecessors. A passing
child PR must not hide a parent's failed scanner check or imply that the parent
was accepted. This command collects evidence for review; it has no merge,
approve, dismiss, write-status, branch-update, database or deployment operation.

From a checkout containing this candidate:

```bash
python -m scripts.pr_stack_review --read-github \
  --repository alexmarinos87/financial-risk-data-platform \
  --pr 167 --pr 168 --pr 169 --pr 176 --pr 177 --pr 181 --pr 191 --pr 193
```

Public repositories can be read without a token. Private repositories require
an existing `GH_TOKEN` environment value with read access to pull requests,
contents, checks and commit statuses. There is no token argument. Do not paste
credentials into command history. No write scope is needed by this command.
It does not use cloud credentials, invoke Git, inspect worktree files or modify
the existing local `morning-review` package.

The command emits one JSON report to stdout only. Exit 0 means the reported
technical checks and base comparisons are clear, **not approval to merge**.
Exit 3 means technical blockers were captured. Exit 1 means collection failed
and no successful report was emitted; exit 2 is invalid usage. Usage and runtime
errors use fixed diagnostics without raw arguments, tokens or provider bodies.

## Collection contract

For each explicitly selected PR, read exact head/base identities, the complete
latest check-run and combined-status inventories, and the comparison of those
exact commits. The preceding reconciler checks App identity, SHA binding,
pagination completeness, missing evidence and every reported failure.

After every candidate has been collected, re-read all PR identities and `main`.
A changed head, base, draft/open state or main reference invalidates collection.
The comparison records how many base commits the candidate lacks. Dependency
order follows matching base-branch/head-branch identities within the selected
same-repository candidates. Missing predecessors, conflicting head identities,
cycles, head/base mismatches and inherited technical failures are explicit or
fail collection; no relationship is invented from PR body prose.

The report separates check outcomes, technical blockers, draft status, pending
predecessor merge and pending engineer acceptance. All candidates retain
`merge_authorized = false`. Independent review, branch-rule enforcement and the
actual merge tree tested by CI are not verified by this command. Use CI checkout
logs, the final diff and independent review alongside it before acceptance.
Logical dependencies not expressed by base branches need separate review.

## Transport and bounds

The standard-library transport permits only an internal allowlist of GET paths
at `https://api.github.com`. It rejects redirects and does not follow any URLs
inside responses. TLS certificate verification stays enabled. Environment proxy
routing is disabled; environments requiring a corporate proxy need a separately
reviewed adapter rather than silently redirecting authenticated requests.

Select at most 25 unique positive PR numbers. Each API read is limited to 1 MiB,
with a 15-second socket timeout. Pagination is capped at ten pages / 1,000 rows
per source; limits, missing data and unsupported fork PRs fail closed. The
socket timeout is not a whole-command deadline. The final stdout report is also
bounded. API bodies, descriptions and arbitrary provider URLs are not retained;
review output includes only selected PR and check identities and diagnostics.

## Validation and important limits

```bash
python -m pytest -q tests/unit/test_pr_check_evidence.py tests/unit/test_pr_stack_review.py
make quality-check
make security-check
make readiness-check
```

Tests exercise the actual collector and entry point using injected API responses,
including scanner failure propagation, exact refs moving mid-collection,
pagination, missing/cyclic dependencies, behind-base comparisons, strict
selection, safe transport construction and redacted errors. Transport tests
inspect actual Request methods/headers and bounded reads; they do not contact
GitHub or use real credentials. Full CI runs the unchanged repository suite.

Ref-stable collection is **not an atomic GitHub snapshot**. Check results can
change between requests or new checks may be created immediately afterward.
Re-run at the final review point. The fixed expected-check policy belongs to this
repository; it is not an inferred universal or branch-protection policy. Neither
this tool nor a passing successor dismisses #191's existing GitGuardian finding.

This independent two-PR engineering-control stack starts at accepted main. It
imports none of the unaccepted notification/source/suspension implementations.
Accept #200 before this candidate, reconstruct the isolated successor delta on
accepted main and rerun exact-head CI. Independent review and explicit engineer
acceptance remain separate; no changes to CI settings or existing PR stacks are
made by running the command.

Primary API references:
- <https://docs.github.com/en/rest/pulls/pulls#get-a-pull-request>
- <https://docs.github.com/en/rest/commits/commits#compare-two-commits>
- <https://docs.github.com/en/rest/checks/runs#list-check-runs-for-a-git-reference>
- <https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference>
