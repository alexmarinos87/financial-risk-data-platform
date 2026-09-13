# Validate daily-risk parameters before storage access

Primary arc42 block: `orchestration`. The interface crossing into `analytics`
has one end-to-end contract: an invalid numerical request must not call the
configuration loader, raw-data reader or curated writer.

The runner previously loaded configuration and raw history before the builder
rejected an invalid volatility window, VaR window or confidence. For example,
`--vol-window 1` is a positive integer accepted by argument parsing, but violates
the analytical minimum of two observations. That failure did not require a file
scan and could be masked by an unrelated storage failure.

`validate_daily_risk_parameters` composes the existing analytics validators.
Both the runner and builder call it. No numerical rule is duplicated: the
volatility window remains an integer in [2, 252], the VaR window in [2, 2520],
and confidence remains finite and strictly between zero and one. Boolean and
unsupported values remain rejected with the existing diagnostics. The runner
normalizes confidence before I/O and passes that ordinary float onward, so
custom numeric conversion cannot change the request between reading and output.
The builder still validates independently for callers that bypass the runner.

Invalid numerical requests now take precedence over storage failures. CLI
semantic failures keep exit code 1 and the existing redacted diagnostic; parser
usage handling is unchanged. Defaults, date/symbol policy, mathematical methods,
model identifiers, valid records and publication ordering are unchanged. Valid
requests still perform the existing configuration, raw-read and writer actions.
This validation does not establish source availability or statistical adequacy.

```bash
python -m pytest -q tests/unit/test_daily_risk_preflight.py
make quality-check
make security-check
make readiness-check
```

Tests retain the real runner, builder and event schema, with explicit reader,
configuration-loader and writer spies. Both injected and default callback paths
must show no attempted I/O for invalid parameters. Actual CLI tests check
rejection before storage access and summary creation. Positive cases compare
all emitted records to the real builder and verify the original I/O order.
These are injected-boundary tests, not real Parquet/warehouse publication proofs.

The local development harness substitutes unused provider/writer import modules
because the full checkout is unavailable. Those substitutes are not committed;
full-repository CI must exercise the tests with normal imports. No dependency,
schema, workflow, activation, provider request or deployment change is involved.
