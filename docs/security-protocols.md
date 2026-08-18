# Security Protocols

This repo is designed to be safe for local portfolio work and interview demos.
Cloud deployment remains manual and approval-driven.

## Enforced Controls

1. `make security-check` scans for obvious committed secrets, generated output,
   unsafe deploy triggers, missing ownership rules, unsafe local database port
   bindings, risky Kubernetes defaults, and managed database creation flags.
2. CI runs for pull requests and pushes to `main`, cancels superseded
   runs for the same pull request or branch, keeps repository permissions
   read-only, and pins its external actions to reviewed commit SHAs. It
   separates security guardrails, Python readiness, and infrastructure
   validation into distinct jobs. Python readiness includes linting, type
   checking, tests, demo execution, and warehouse dry-run loading.
3. `.gitignore` and `.dockerignore` exclude local environments, generated data,
   Terraform state, kubeconfigs, AWS credential folders, key material, and local
   sandbox logs. Terraform provider lock files remain tracked for reproducible
   validation.
4. Docker Compose publishes PostgreSQL and MongoDB only on `127.0.0.1`.
5. The deploy workflow is manual-only and requires typed confirmation.
6. The deploy workflow requires `ALLOW_CLOUD_DEPLOY=true` in the protected
   GitHub Environment.
7. Production deploys must run from `main`.
8. Kubernetes deploys render the selected overlay with the immutable image tag,
   then perform server-side dry-run and diff before applying the same manifest.
9. Base Kubernetes manifests default to no service account token automount and
   default-deny ingress/egress.
10. `.github/CODEOWNERS` marks sensitive paths for ownership review.
11. `make infrastructure-check` renders Kubernetes overlays and validates
    Terraform without applying cloud changes.
12. The CI infrastructure job installs Kubernetes and Terraform CLIs, then runs
    `make infrastructure-check`; it does not configure cloud credentials,
    deploy, or run `terraform apply`.

## Human Approval Required

Require explicit approval before:

1. Running `terraform apply`.
2. Triggering the GitHub deploy workflow.
3. Applying Kubernetes manifests to a real cluster.
4. Changing IAM, OIDC, secrets, database credentials, or network access.
5. Running destructive database operations.
6. Publishing or merging an unattended overnight change.

Approval to prepare or publish a scheduled candidate must be encoded in the
complete, unexpired manifest described by `docs/overnight-development.md`.
Publication approval never includes readiness, acceptance, merge, deployment,
workflow dispatch, or cloud credentials.

## Recommended GitHub Settings

Configure branch protection or repository rules for `main`:

1. Require pull requests before merge.
2. Require passing status checks.
3. Require review from CODEOWNERS for protected paths.
4. Block force pushes.
5. Require conversation resolution before merge.
6. Protect the `prod` GitHub Environment with required reviewers.

Before any unattended branch publication, also require stale-review dismissal,
approval after the last push, no administrator or bot bypass, CI concurrency,
and a short-lived repository-scoped publisher credential. The writer and tests
must never receive that credential. Until those controls are independently
verified, scheduled publishing remains disabled.

## Overnight Sandbox

Use `docs/overnight-sandbox.md` for unattended validation. The sandbox can run
checks and write logs, but it must not push, merge, deploy, use cloud
credentials, or create cloud resources.

`docs/overnight-development.md` describes a separate candidate mode. It does
not weaken the sandbox or activate publication by itself.

`.github/overnight-publication-policy.json` is the machine-readable publication
policy. Schema version 1 is deliberately disabled-only: `make security-check`
rejects attempted activation, configured adapters, mutation capabilities,
ambiguous JSON, and CI contract regressions. That candidate-tree check is not a
trusted activation verifier. A future scheduler must load and verify approved
policy and verifier bytes from the pinned protected-base commit before any
branch creation or worktree mutation.

## Known Boundaries

The current local security check is intentionally lightweight. It is useful for
this repo, but it is not a substitute for organization-wide secret scanning,
SAST, dependency scanning, image scanning, cloud policy checks, or production
security review. Immutable-action enforcement in this iteration covers
`.github/workflows/ci.yml`; the privileged manual deployment workflow remains a
separate deployment-control review.
