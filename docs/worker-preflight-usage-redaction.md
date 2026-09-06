# Worker preflight usage-error redaction

Primary arc42 block: `orchestration`. Goal #170, after the #169 diagnostic.

## Decision and regression

`notification_worker_cli_parser.py` owns the existing argument definitions and
uses a fixed program name and value-independent usage error. The command delegates
argument parsing without changing its database, evidence or exit-status logic.
Unknown options and values assigned to boolean flags previously reached normal
argparse error rendering before the runtime redaction handler. Those messages
can contain a mistakenly supplied credential. The focused tests demonstrate the
original behavior with an explicitly synthetic sentinel, then reject the same
inputs without echoing that sentinel.

Invalid usage still exits 2. Help still exits 0, but does not include an invocation
path taken from `sys.argv[0]`. Source-mode exclusivity, required identities and
slot, configuration defaults and disabled abbreviation are preserved. No DSN
argument is added. The runtime redaction handler remains unchanged.

## Limits and verification

This is output redaction, not protection against shell history, process argument
inspection or an operator deliberately printing credentials. Credentials still
belong only in the configured environment variable. Fixed errors trade detailed
invalid-value diagnostics for a bounded predictable surface; `--help` retains
ordinary option guidance.

```bash
python -m pytest -q tests/unit/test_notification_worker_cli_parser.py
make quality-check
make security-check
```

The parser module and its focused tests use only the standard library plus pytest;
the existing command regression suite verifies integration in full repository CI.
Reference: Python's documented `ArgumentParser.error` customization point,
<https://docs.python.org/3.11/library/argparse.html#argparse.ArgumentParser.error>.

No database read/write, configuration switch, schema, workflow, dependency,
notification, scheduler activation, deployment or Terraform apply. This candidate
remains pending explicit engineer acceptance, together with its predecessors.
