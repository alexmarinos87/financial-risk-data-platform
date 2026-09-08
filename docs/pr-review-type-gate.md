# Direct type checking for PR review scripts

Primary arc42 block: `engineering-controls`. Goal #223 follows #217.

## Decision

The existing `type-check` recipe selected only the `src` package. The check
reconciler and stack collector live under `scripts`, so a successful result for
`src` did not establish that either review script had been type-checked.

The recipe now keeps the original command and adds an explicit module selection:

```bash
python -m mypy --package src
python -m mypy --module scripts.pr_check_evidence --module scripts.pr_stack_review
```

Both `quality-check` and `readiness-check` already depend on this target. No new
workflow, duplicate CI job, dependency version, missing-import exemption or
runtime behavior is introduced. The commands use the existing locked mypy and
repository configuration. Either command failing fails the target.

## Evidence

`tests/unit/test_pr_review_type_gate.py` invokes the real Make target with an
injected Python shim, checks the exact two command arguments, and challenges
failure in each stage. Dry-run checks confirm both existing aggregate targets
retain lint, typing, tests and dependency validation. These are wiring and error
propagation tests, **not an execution of mypy**. The final CI log must separately
show actual mypy execution and success for the two explicitly selected scripts.

```bash
python -m pytest -q tests/unit/test_pr_review_type_gate.py
make type-check
make quality-check
make readiness-check
```

## Limits and acceptance

Static checks do not replace the runtime validation of untrusted API mappings.
The existing `Any` boundaries are not removed by selecting these modules; this
is not a claim of strict typing or complete proof of correctness. Other scripts
are not newly selected. HTTP transport integration, independent review and the
engineer's final-diff acceptance remain separate evidence and decisions.

This change does not run the GitHub collector, submit reviews, merge candidates,
dismiss scanner findings, change branch rules, activate notifications, contact
an application database or deploy infrastructure. It remains pending acceptance
alongside its predecessors.

Reference: mypy's repeatable `--module` option selects explicit modules:
<https://mypy.readthedocs.io/en/stable/command_line.html#cmdoption-mypy-m>.
