# Select daily-risk dependencies by presence

Primary arc42 block: `orchestration`.

`run_daily_risk` accepts optional configuration-loader, reader and writer
callbacks. Only `None` means use the built-in dependency. A supplied callable
must be used even when its `__bool__` returns false or its `__len__` returns zero.
Selecting with `callback or default` discarded such adapters and could invoke
unrequested default storage operations. Selection now uses explicit None checks.

The runner neither evaluates dependency truth values nor retries a failing
supplied adapter through a default. Existing redacted error handling, numerical
and date preflight, reader/writer order and complete valid output are unchanged.
Omitting a callback and explicitly passing None remain equivalent. Non-callable
values are still outside the annotated API contract.

```bash
python -m pytest -q tests/unit/test_daily_risk_callback_selection.py
```

The tests use the real runner, analytical builder and event schema with recorded
I/O callbacks. They cover false Boolean values, zero lengths, truth-testing that
raises, unchanged complete records, adapter failures, omitted/None defaults and
rejection before activating callbacks. Default-boundary spies prevent actual
storage fallback during tests and record any attempted selection.

These are dependency-injection contract tests, not real Parquet or warehouse
publication evidence. The existing integration suites remain responsible for
those boundaries. No dependency, dataset schema, risk model or deployment
configuration changes.
