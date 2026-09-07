# Reconcile combined commit-status evidence

Primary arc42 block: `engineering-controls`. Goal #209 follows #200/#203.

## Decision and reproduced failure

The exact-head check reconciler validated every returned context but ignored the
combined status response's `state`. A synthetic complete response with aggregate
`failure` and its only context `success` produced `outcome=passed` on #203.
This is contradictory evidence, not a successful validation or a reason to pick
the more optimistic source.

`scripts/pr_check_evidence.py` now recomputes the combined state from the complete
context inventory and requires every page to agree. Failure/error takes priority,
then pending, then success. The aggregate is global: a pagination slice containing
only successful contexts can correctly report failure if a different page has the
failed context. Validation happens after complete-row and context-state checks.
Missing, malformed or contradictory aggregate fields raise fixed `EvidenceError`
diagnostics. The actual collector propagates rejection and its CLI emits no
successful JSON report.

An empty legacy-status inventory legitimately has combined state `pending`.
This alone does not block complete successful check-run evidence: commit statuses
and check runs are separate APIs. The existing required-App/check policy and
additional-failure checks remain unchanged. The output adds the reconciled
`combined_status_state` without granting acceptance or merge permission.

## Evidence and compatibility

Old synthetic page factories now supply the real API's aggregate field. No prior
assertion is removed. New tests cover all aggregate precedence cases, contradictory
and malformed summaries, changing pages, global pagination semantics, real
collector/CLI rejection, detached output and a projected live empty-status response
captured through the connected GitHub reader for #203. That test is captured-data
replay, not a network test or an assertion that #203's checks remain current.

```bash
python -m pytest -q tests/unit/test_pr_check_evidence.py \
  tests/unit/test_pr_stack_review.py \
  tests/unit/test_pr_combined_status_reconciliation.py
make quality-check
make security-check
make readiness-check
```

Previously saved incomplete synthetic responses without `state` are deliberately
rejected. Product/API responses include that field. This change does not add a
network request, scanner disposition, branch rule, workflow, dependency, database
operation or deployment. A coherent capture is neither source authentication nor
independent review; final-diff engineer acceptance remains pending.

Primary reference: GitHub's combined commit-status response semantics:
<https://docs.github.com/en/rest/commits/statuses#get-the-combined-status-for-a-specific-reference>.
