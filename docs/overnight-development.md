# Overnight Candidate Development Runbook

This is a separate mode from `make overnight-sandbox`. The sandbox remains a
validation-only command. Once activated, candidate development may edit one
isolated worktree and create incremental green commits. A separately isolated
publisher may push those commits to one new branch and open one draft pull
request only after every activation prerequisite below is met.

Machine policy: `.github/overnight-publication-policy.json`

Schema version 1 publication status: **disabled only**

The machine policy, read as an exact Git blob from a recorded commit on
protected `main`, is the activation source of truth. This prose and a saved
scheduled prompt have no activation switch. The dependency-free security check
rejects malformed, ambiguous, configured, or enabled schema-version-1 policy;
it does not act as the future privileged activation verifier.

The current repository audit found no protection or ruleset on `main` and only
a broad personal GitHub credential. CI now defines bounded triggers,
concurrency, read-only permissions, and immutable action revisions, but those
repository-file controls do not create an approved unattended publisher.

Enabling publication requires a later human-reviewed policy schema and trusted
verifier that supply an unexpired authorization and approved identities for
implemented publisher, policy-verifier, and candidate-isolation adapters. Every
prerequisite below must also be independently verified. The scheduled task must
continue reading and hashing the policy from the pinned base on every run.

## Outcome And Authority

One scheduled run may produce at most:

1. One approved backlog item.
2. One primary arc42 building block.
3. One isolated worktree and new branch.
4. One to three coherent, locally green commits.
5. One atomic candidate-branch push per green checkpoint when the task manifest
   allows it.
6. One draft pull request against the recorded base.

It never grants permission to push to `main`, force-push, rewrite history, mark
a PR ready, approve or merge a PR, deploy, use cloud credentials, or accept the
change. The morning decision remains `PENDING` for the human engineer.

Local scheduled tasks require the Codex desktop app, an available local project,
an isolated worktree, and the app and computer to remain running. Test the prompt
interactively before making it recurrent. See the
[official scheduled tasks guide](https://learn.chatgpt.com/docs/automations).

## Architecture Boundary

```text
Human-approved task manifest
          |
          v
Scheduler and single-run lease
          |
          v
Credential-free isolated worktree
  -> bounded writer
  -> read-only correctness reviewer
  -> read-only production challenger
  -> validation and evidence gate
          |
          v
Least-privilege publisher
  -> create-only branch push
  -> draft PR adapter
          |
          v
Human review and acceptance
```

The change runner and candidate tests must not receive the publisher credential.
The publisher must not execute candidate code. Publication requires an
out-of-process boundary or equivalent isolation that injects a short-lived token
only after validation. The current local broad credential does not satisfy this
design.

## Activation Prerequisites

`docs/security-protocols.md` remains the canonical security policy. The checks
below add unattended-publication requirements and do not replace or weaken it.
Keep publication disabled until all of these are implemented and independently
verified:

- [ ] Protect `main` with required pull requests and required, uniquely named CI
      checks.
- [ ] Require human review and CODEOWNER review for protected paths, dismiss
      stale approvals, require approval after the last push, and require
      conversation resolution.
- [ ] Block force pushes, branch deletion, administrator bypass, and bot bypass.
- [ ] Independently verify the repository CI concurrency keyed by pull request
      or branch so a new checkpoint cancels superseded validation for the same
      candidate. This is not the overnight writer lease.
- [ ] Use a dedicated short-lived GitHub App installation token with only
      metadata read, contents read/write, pull-request write, and checks read.
- [ ] Deny the publisher workflow, Actions-write, deployment, environment,
      secret, administration, package, OIDC, merge, and ruleset-bypass authority.
- [ ] Keep the publisher token out of the writer, reviewer, test, hook, and
      Makefile environments.
- [ ] Run candidate edits and validation through a named isolation adapter that
      proves a scrubbed environment, disabled hooks and credential helpers,
      denied publisher/cloud credentials, denied candidate network, a bounded
      filesystem, and an enforced runtime deadline.
- [ ] Prove create-only branch publication, draft-only PR creation, token denial,
      base movement, branch collision, and two-run race behaviour.
- [ ] Pin every privileged adapter and its closed dependency set to protected-
      base blobs or signed artifacts with approved SHA-256 identities.
- [ ] Run this workflow interactively and inspect several no-op/local-only runs
      before enabling external writes.

Until every box is checked on protected `main`, the scheduled task must stop
before creating a branch, editing, committing, pushing, or opening a PR. Local
candidate work remains available only in a human-controlled interactive session.
GitHub documents the relevant controls in its
[protected branch guidance](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches).

## Approved Task Manifest

The scheduler may select only a backlog item that links to one complete manifest
at `docs/overnight-manifests/<manifest-id>.json`. The lowercase manifest ID must
match `[a-z0-9][a-z0-9-]{7,63}` and the filename exactly. The entire Git blob
returned by `git show <base-sha>:<manifest-path>` is the manifest byte sequence;
hash those bytes without newline, encoding, key-order, or whitespace
normalisation.

```json
{
  "schema_version": 2,
  "manifest_id": "frdp-analytics-daily-risk-20260815",
  "status": "approved for overnight development",
  "authorization_issued_at": "2026-08-15T19:00:00Z",
  "authorization_expires": "2026-08-15T22:00:00Z",
  "repository": "alexmarinos87/financial-risk-data-platform",
  "protected_base_branch": "main",
  "arc42_primary_block": "analytics",
  "context_goal": "One stakeholder-visible outcome",
  "allowed_paths": ["src/analytics/daily_risk.py", "tests/unit/test_daily_risk.py"],
  "interfaces_crossed": [],
  "runtime_scenario": "Control and data flow affected",
  "quality_scenario": "Stimulus and measurable response",
  "recovery_scenario": "Failure leaves no published candidate",
  "acceptance_criteria": ["One observable result"],
  "validation_targets": ["security-check", "quality-check", "readiness-check", "git-diff-check"],
  "maximum_changed_lines": 500,
  "maximum_changed_files": 2,
  "maximum_commits": 3,
  "maximum_pushes": 3,
  "maximum_runtime_minutes": 120,
  "retry_policy": "human-renewed-manifest-only",
  "risk": "low",
  "draft_pr_publication": "eligible-after-global-activation"
}
```

The human engineer must commit the backlog link and manifest to protected
`main`. A Manifest ID is immutable and single use: changing bytes under the same
ID is an error, not a new authorization. A retry requires a new manifest ID and
file. Missing, expired, contradictory, high-risk, or incomplete authorization is
a successful no-op. The scheduler must not invent a replacement task.

`scripts/overnight_manifest_verifier.py` accepts only strict JSON bytes, one of
the low-risk `analytics`, `processing`, or `benchmarking` blocks, exact paths,
fixed validation target identifiers, hard budgets, and a UTC authorization
window of at most 24 hours. This parser does not read a worktree, Git ref, or
network. Its `verify_protected_base_manifest` adapter requires a full commit SHA
equal to the local `origin/main` ref before and after verification, derives the
path from the Manifest ID, and loads one size-bounded `100644` Git blob. It
independently recomputes the repository-format Git object ID, hashes the exact
bytes, and emits redacted evidence with
`publication_authorized: false`.

The adapter is a Linux/WSL boundary that uses `/usr/bin/git`, a symlink-free
repository-root path, a minimal child environment, bounded output, and disabled
Git protocols. It rejects partial/promisor repositories before object reads.
It relies on the trusted fetch and local object store for commit/tree parsing;
the selected blob gets the additional independent object-ID check. It never
fetches and cannot prove that the local ref is fresh, that GitHub protects
`main`, or that a human approved the commit. The trusted orchestrator owns those
checks. A valid contract is not publication authority.

Invoke the repository adapter with `.venv/bin/python
scripts/overnight_manifest_verifier.py
--repository <root> --base-sha <full-sha> --manifest-id <id>`. Exit `0` emits
valid-but-non-authorizing evidence. Exit `2` is a controlled denial that the
scheduler maps to a successful no-mutation outcome. Exit `1` is an operational
verifier failure that requires investigation; neither nonzero outcome permits a
branch, edit, commit, or external write.

Schema version 2 does not accept free-form commands or cross-block interfaces.
A later trusted runner maps its four validation identifiers to fixed argument
vectors without a shell. A future schema may admit bounded vertical slices only
after the interface and committed-diff verifier exists.

## Arc42 Module Selection

Use the building block view and modularity rules in `docs/architecture.md`.
When several items are eligible, prefer the item with:

1. One clear stakeholder outcome and primary building block.
2. One understandable runtime scenario and rollback boundary.
3. A measurable quality scenario with focused tests.
4. No path overlap with an open branch or pull request.
5. The lowest security, data, operational, performance, and cost risk.

Reject a candidate that crosses unrelated blocks, exceeds its budget, or touches
secrets, IAM/OIDC, authentication, network access, deployment, infrastructure,
database schemas/load logic, locks/backfills, data-quality thresholds,
dependencies, binaries, submodules, or symlinks. Those areas require a separate
human-directed task and PR.

## Isolation And Base Freshness

Each run must:

1. Verify and use the scheduler-supplied isolated worktree. Refuse the primary
   checkout and do not create a nested worktree.
2. Require a clean tracked and untracked state.
3. In a trusted orchestration step, fetch `origin`, resolve `origin/main`, and
   record its commit SHA. Do not execute candidate code in this step.
4. Read `AGENTS.md`, `.github/overnight-publication-policy.json`, architecture,
   security policy, and the selected manifest from that exact commit with
   `git show <base-sha>:<path>`. Hash the machine policy and selected manifest;
   reject local, uncommitted, or stale copies.
5. Confirm no matching local/remote branch, terminal run record, or PR exists.
6. Verify the protected-base policy names implemented policy-verifier,
   candidate-isolation, and publisher adapter paths plus approved SHA-256
   digests. With the current not-configured values, stop before a branch or
   mutation; prompt text is not isolation.
7. Acquire the repository-global lease and manifest claim described below.
8. In the trusted process, extract those exact protected-base blobs and their
   closed dependencies into a read-only execution directory outside the
   candidate-writable tree. Verify every digest before execution; never execute
   a same-named local replacement.
9. Only then enter the configured candidate process with inherited Git hooks,
   credential helpers, SSH agents, cloud credentials, publisher credentials,
   and candidate network access disabled.
10. Create a generated branch at the recorded SHA and validate its name with
   `git check-ref-format`, for example:

   ```text
   agent/<block>-<short-outcome>-<YYYYMMDD>-<base8>
   ```

11. Never reuse raw prompt text as a ref and never create tags.
12. Recheck the remote base and lease before every external write. If either
   changed, stop; do not autonomously rebase or merge.

Only one scheduled run may operate on this repository at a time. Independent
review agents may work in parallel read-only, but there is one writer and one
candidate branch.

### Repository Lease And Manifest Run Record

For one workstation, use a registry beneath the absolute Git common directory
returned by `git rev-parse --path-format=absolute --git-common-dir`. Do not put
the registry in a per-worktree `.sandbox` directory.

1. Generate a run UUID and random fencing token. Atomically acquire the fixed
   repository lease `<git-common-dir>/overnight-candidates/active` with one
   `mkdir`, then atomically write redacted owner metadata: run UUID, process
   identity, base SHA, selected Manifest ID and hash, start time, deadline, and
   fencing token. If `active` already exists, perform a no-op even when it names
   a different manifest.
2. Derive the manifest record key as the lowercase hexadecimal SHA-256 of the
   UTF-8 Manifest ID only. Store the separately calculated full-blob manifest
   hash in every claim and terminal record.
3. While holding `active` but before acquiring a manifest claim, recheck the
   claim and terminal paths. If exactly one well-formed record exists, verify
   its Manifest ID and blob hash and leave it untouched. A matching ID and hash
   is a duplicate no-op; a different hash is unauthorized ID reuse. In either
   case, recheck the owned `active` run UUID and fencing token and atomically
   move only that lease to a unique no-op or denied history path. This is the
   sole finalization path that does not require a manifest claim owned by the
   current run. If both records exist, either is malformed, or the fenced lease
   move fails, retain `active` for human recovery.
4. When neither record exists, atomically acquire
   `<git-common-dir>/overnight-candidates/claims/<record-key>` with one `mkdir`
   and atomically bind the Manifest ID and hash plus the current run UUID and
   fencing token into it. If acquisition reports an existing record, repeat the
   pre-claim state inspection in step 3; on any other acquisition error, retain
   `active` for human recovery.
5. Before every phase, mutation, and external write, verify that both `active`
   and the single manifest claim name the expected Manifest ID and hash, current
   run UUID, and fencing token, and verify that no terminal record exists. That
   exact owned claim is the normal running state; renew both owner timestamps
   and continue. Any terminal record, foreign or mismatched claim, malformed
   record, or simultaneous claim and terminal is an invariant failure: retain
   `active` for human recovery and do not act. The deadline is the smaller of
   the manifest runtime and its authorization expiry.
6. Never remove a stale or malformed repository lease, claim, or terminal
   automatically. The human engineer inspects and explicitly resolves it before
   issuing a new manifest ID to retry.
7. Bind the repository-lease identity and fencing token into validation evidence
   and every publisher request. The trusted policy verifier and publisher must
   re-read both lease records and reject an owner, token, deadline, or manifest
   mismatch immediately before acting.
8. Append redacted phase records inside the manifest claim after commits and
   external writes. A push is not terminal while draft-PR creation is still
   pending.
9. For a normal terminal outcome (successful local completion, an ordinary
   implementation or validation failure, or a publication outcome), first prove
   that `active` and the claim still match the current Manifest ID and hash, run
   UUID, and fencing token and that no terminal record exists. Then atomically
   move the manifest claim to
   `<git-common-dir>/overnight-candidates/terminal/<record-key>` and write the
   final state. Then, only after rechecking the owner and fencing token,
   atomically move `active` to a unique history path for that run. If either
   finalization fails, leave the repository lease in place and require human
   recovery rather than allowing a second writer. Acquisition, metadata,
   ownership, malformed-record, and other invariant failures described above do
   not use this normal finalization path and retain `active`.

This registry coordinates scheduler worktrees sharing one Git common directory.
A multi-host scheduler requires an external transactional lease and is outside
this runbook.

## Implementation, Review, And Green Checkpoints

For the selected item:

1. Record the arc42 context, block, interfaces, runtime flow, state, side
   effects, quality scenario, recovery, permissions, and cost constraints.
2. Implement only the allowed paths and acceptance criteria.
3. Run an independent read-only correctness and maintainability review.
4. Run a separate read-only production-failure challenge covering invalid
   input, partial failure, retries, idempotency, concurrency, volume,
   permissions, rollout, rollback, monitoring, and cost.
5. Separate proven defects from questions and optional hardening. Return
   accepted fixes to the writer and repeat affected review passes.
6. Run every manifest validation target through the protected fixed-argument
   mapping. Schema version 2 requires exactly:

   ```bash
   make security-check
   make quality-check
   make readiness-check
   git diff --check
   ```

7. Confirm the diff stays within its allowlist and line, file, commit, and push
   budgets. Confirm generated outputs and credentials are not staged.
8. Stage explicit allowed paths and run `git diff --cached --check`.
9. Commit only a coherent checkpoint whose relevant checks pass. Do not create
    WIP or knowingly broken commits.

Count the candidate budget from the staged change against the recorded base:

```bash
git diff --cached --numstat -z <base-sha>
```

Parse the NUL-delimited records and sum additions and deletions. Use numstat only
for paths, binary markers, and lines; the committed tree-snapshot gate rejects
symlinks and gitlinks. This is a pre-commit line-budget check until the separate
committed-line adapter exists.

After each commit, call `verify_committed_candidate_history` with the exact base,
Manifest ID, and full candidate SHA. It derives the linear parent chain from
independently rehashed commit bytes and compares every edge using in-memory,
raw-byte path maps from independently rehashed canonical tree objects. Empty or
malformed tree topology, unlisted paths, non-regular/non-text blobs, and file or
commit budget overflow fail closed without logging file content. The observer
caps each blob at 1 MiB, the unique changed-blob set at 4 MiB, and recursively
expanded tree graphs by object, byte, entry, depth, and path limits. Its evidence
explicitly
leaves line budget, content fingerprint, worktree cleanliness, validation, push
count, object-store isolation, and publication authority unverified. The trusted
controller must establish common-object-store isolation before relying on this
observation in a later gate; this observer does not establish that isolation. A
candidate cannot proceed merely because this scope gate passes.

A checkpoint is not green until validation is repeated against the exact commit
tree that would be pushed:

1. Record the commit and tree SHAs, then create a fresh detached validation
   checkout at that commit through the candidate-isolation adapter.
2. Require an empty Git status before the first command: no tracked
   modifications, untracked paths, or conflicts. Do not copy the development
   worktree or its ignored outputs.
3. Run the complete manifest and arc42 validation with credentials, hooks,
   network, and writable paths restricted by the adapter.
4. Before and after every command, require the same commit/tree SHAs and no
   tracked mutation. Record exact command, tool versions, timestamps, return
   code, and redacted log hash.
5. Destroy no user work. Retain the redacted evidence and have the trusted policy
   verifier bind its hash to the commit/tree, manifest, base, adapter identities,
   and diff fingerprint.

Only this post-commit evidence may authorize a checkpoint push. Any later
commit, amend, rebase, file mutation, or validation-environment change invalidates
the binding and requires a new isolated validation pass.

When incremental publication is activated and the manifest authorizes more than
one push, repeat scope inspection, content fingerprinting, relevant validation,
base freshness, and the create/update lease before every checkpoint push. Never
force-push or amend a pushed commit. Prefer one final push when intermediate
remote recovery is not required.

After the final green commit, run `make morning-review` against the recorded
base. The evidence package remains local and the decision remains `PENDING`.

## Publication Gate

Publication is permitted only when the activation checklist, manifest, lease,
base, scope, reviews, validation, content fingerprint, and credential boundary
all pass in the same run.

The publisher adapter and its adversarial tests are not implemented in this
repository, so publication remains disabled. A future adapter must accept a
signed, schema-validated request containing the manifest/policy hashes, lease
and fencing token, expected base/remote SHAs, branch, commit/tree SHAs, diff
fingerprint, validation evidence hashes, exact approved adapter paths/digests,
and draft PR metadata. It must return a redacted signed result, verify its own
approved identity before accepting a token, and never run repository code.

When that adapter is configured, the first green checkpoint lifecycle is:

1. Obtain a new short-lived publisher token outside the candidate process.
2. Recheck the lease and protected base SHA.
3. Use an atomic compare-and-swap ref operation whose expected old object ID is
   the all-zero absent-ref value. Reject a concurrent creation and verify the
   resulting remote SHA.
4. Open one draft PR against the manifest's `PR base branch`; verify base, head,
   draft state, and CI creation or required human approval.
5. Drop or expire the token and record the published SHA.

For each manifest-authorized later checkpoint in the same run:

1. Repeat review, scope, budget, validation, fingerprint, lease, and base checks.
2. Obtain a new short-lived token and use an atomic compare-and-swap update whose
   expected old object ID is the last published SHA.
3. Require the new commit to be a descendant of that SHA. Reject any mismatch,
   remote movement, or non-fast-forward update.
4. Verify the new remote SHA and CI state, drop the token, and update the run
   record.

After the final checkpoint, the publisher performs no further mutation. It
never force-pushes, rebases, amends a published commit, or uses a broader token.

The PR body must include:

- `Decision: PENDING — human acceptance required`.
- Manifest item and authorization expiry.
- Base, commit, and tree SHAs plus a content-complete diff fingerprint.
- Arc42 context, block, interfaces, runtime flow, state, and side effects.
- Quality and production-failure scenarios.
- Security, permissions, operational, performance, and cost implications.
- Exact changed paths, validation commands, results, and unavailable tools.
- Reviewer findings, dispositions, unresolved questions, and up to ten morning
  inspection paths.

## Stop And Recovery Rules

Stop without external writes when authorization, protection, credential
isolation, lease, base, scope, tools, validation, or review evidence is missing
or ambiguous. A failed gate is not permission to weaken tests or widen scope.

If a push succeeds but PR creation fails, retain and report the remote branch.
If the PR exists but CI is red, absent, or pending approval, leave it draft and
report it. Never automatically rebase, close, delete, merge, retry with broader
credentials, or perform destructive cleanup.

Record the last durable state: run/lease ID, manifest hash, base SHA, branch,
commit/tree SHA, changed paths, fingerprints, exact commands and return codes,
review dispositions, push lease, PR URL/base/head, CI URLs/status, skipped
checks, and final stop reason. Never record credential values or raw secret-like
content.

## Scheduled Task Prompt

Configure the task itself to run in a dedicated scheduler worktree. This prompt
has no mutable publication switch; it derives authority only from the pinned
protected-base policy and manifest:

```text
Repository: /home/alexmarinos87/projects/financial-risk-data-platform
Comparison ref: origin/main
PR base branch: main

Verify this is the scheduler-supplied isolated worktree. In a trusted
orchestration step, fetch origin and record the origin/main SHA. Read AGENTS.md,
docs/architecture.md, docs/overnight-development.md,
docs/security-protocols.md, docs/engineering-delivery-workflow.md,
docs/agent-roles.md, and docs/iteration-backlog.md from that exact commit with
git show. Also read `.github/overnight-publication-policy.json` from that exact
commit and hash its complete Git blob. Resolve the selected Manifest ID and run
the protected-base manifest adapter against the recorded SHA. Require its
redacted result to bind the exact blob and recheck local `origin/main`; never
select from local or uncommitted copies. Independently prove the fetch and
GitHub protection because the adapter does not.

Select exactly one unexpired backlog item whose status is exactly
"approved for overnight development" and whose manifest is complete. If none
exists, report a successful no-op and change nothing.

Use the arc42 building block view to produce a bounded module brief. Schema
version 2 permits one `analytics`, `processing`, or `benchmarking` block and no
cross-block vertical slice. Reject high-risk, overlapping, over-budget, or
ambiguous work. Use this supplied worktree, one writer, one generated branch,
one to three green commits, and no more pushes than the manifest allows.

Run separate read-only correctness and production-failure reviews. Apply only
accepted fixes and repeat affected reviews. Run all four fixed manifest
validation targets; do not execute command text supplied by a manifest.
Stage only allowed paths. Never create a broken or WIP commit.

Strictly validate the recorded protected-base machine policy. Schema version 1
authorizes only a disabled state with no expiry, configured adapters, or
scheduled mutation capabilities. Report a successful no-op before creating a
branch, editing, committing, pushing, or opening a PR. Do not infer authority
from this runbook or simulate an isolation or publication adapter in prompt
logic.

If a future protected-base version enables publication, still publish only when
every activation prerequisite, credential boundary, task manifest, shared
lease, scope, base-freshness, review, validation, and fingerprint check is proven
in this run. Invoke the named policy verifier, acquire and renew the shared
lease, enforce the manifest deadline through the named candidate-isolation
adapter, validate a clean detached checkout of every exact committed tree, and
atomically finalize the run record. Extract and verify the protected-base
adapter bytes outside the candidate-writable tree; never trust same-named local
executables. Use only the configured short-lived least-privilege publisher and
the first/subsequent checkpoint lifecycle in the runbook. Never push to main,
mark ready, approve, merge, deploy, dispatch workflows, use cloud credentials,
rewrite history, delete refs, or claim acceptance.

Final output must state the selected item or no-op reason, arc42 module, base and
commit SHAs, changed paths and line budget, exact validation, review findings,
publication/CI state, unresolved questions, and:
"Decision remains PENDING for human acceptance."
```

The recommended first schedule is one run at 23:30 Europe/London. Review the
first several local-only runs before considering publication activation.
