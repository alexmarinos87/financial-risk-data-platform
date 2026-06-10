# Overnight Sandbox Runbook

Use this runbook for a safe unattended validation loop. The sandbox is designed
to gather evidence overnight, not to merge code or deploy infrastructure.

## What It Does

`make overnight-sandbox` runs repeated local validation cycles for up to eight
hours. Each cycle runs:

1. `git status --short --branch`
2. `make security-check`
3. `make readiness-check`

Logs are written under `.sandbox/overnight/`, which is ignored by git.

## What It Will Not Do

The sandbox does not:

1. Push commits.
2. Open or merge pull requests.
3. Trigger deployment workflows.
4. Run `terraform apply`.
5. Connect to AWS.
6. Use cloud credential environment variables by default.
7. Delete anything outside generated local output paths handled by
   `make clean-generated`.

The sandbox refuses to start when common cloud credential environment variables
are present, unless `--allow-cloud-env` is explicitly supplied.

## Before Starting

Run:

```bash
git status --short --branch
make security-check
make sandbox-once
```

If Docker is available and you want source-to-warehouse evidence as well:

```bash
make local-db-up
make consistency-demo
make local-db-down
```

## Start An Overnight Run

Use a terminal session you can return to in the morning:

```bash
make overnight-sandbox
```

To keep it running after closing the terminal:

```bash
mkdir -p .sandbox
nohup make overnight-sandbox > .sandbox/overnight.out 2>&1 &
```

Check progress:

```bash
tail -f .sandbox/overnight.out
find .sandbox/overnight -name summary.json -print
```

## Custom Runtime

Run one cycle:

```bash
make sandbox-once
```

Run for two hours with a ten-minute pause between cycles:

```bash
.venv/bin/python scripts/overnight_sandbox.py --hours 2 --sleep-seconds 600
```

## Morning Review

Review:

1. Latest `.sandbox/overnight/*/summary.json`.
2. Any failed command logs in the same run directory.
3. `git status --short --branch`.
4. `.demo/pipeline-summary.json` if the final cycle passed.

Then decide what should become a branch or pull request. Do not merge or deploy
based only on overnight output.

## Security Protocols

1. Keep cloud credentials out of the sandbox environment.
2. Keep AWS resources disabled by default.
3. Keep `.sandbox/`, `.demo/`, `data/`, and local caches ignored.
4. Use `make security-check` before publishing work.
5. Review any change touching IAM, secrets, Terraform, deployment workflows,
   database schemas, or load logic.
6. Require explicit approval before `terraform apply`, Kubernetes deploys,
   GitHub deploy workflows, or destructive database operations.

See `docs/security-protocols.md` for the full repository security checklist.
