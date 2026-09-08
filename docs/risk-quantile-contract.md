# Finite signed risk quantiles

Primary arc42 block: `analytics`. Goal #224, under roadmap #76.

## Decision

`src/analytics/risk_metrics.py` retains the historical signed lower-tail return
quantile. It is not a nonnegative loss amount. Valid confidence is a finite real
scalar strictly between zero and one; booleans are not probabilities. This is
checked even when the input Series is empty.

The helper requires a pandas Series. Missing observations are excluded, as in
the prior quantile path, but a nonempty all-missing history is now rejected. The
legacy result for an actually empty Series remains 0.0 for compatibility; callers
must not mistake that sentinel for sufficient history or evidence of low risk.
All observed values must be finite real numbers. Strings, booleans, complex
values and Decimal objects are not silently coerced into return observations.

Numeric observations are converted to float64 before linear interpolation. This
avoids signed/unsigned integer subtraction wrapping inside the quantile backend.
Ordinary pipeline float64 histories retain their existing values and quantile
method. Other accepted real numeric representations use binary64 precision;
this is not arbitrary precision and may round large integers or change float32
rounding. Missing points are not forward-filled, and finite values are not clipped.

The quantile result itself must be finite. Extreme but finite endpoints can
overflow interpolation. That is rejected rather than returned as infinity/NaN
or changed to zero. Local NumPy overflow/invalid warning suppression surrounds
only that calculation; the explicit finite-result check remains mandatory.
This is a numerical safety boundary, not a proof of statistical suitability.

## Why it matters

A positive infinite return quantile can become zero when a caller calculates
`max(0.0, -quantile)`. Rejecting invalid observations/results prevents this
misleading outcome in callers of the helper. Separate daily-output checks are
still needed: early histories may emit returns before any quantile is computed,
and rolling variance can overflow independently. That is goal #225, not a new
risk model or a change to the documented sign convention.

Pandas documents the quantile fraction and default linear interpolation:
https://pandas.pydata.org/docs/reference/api/pandas.Series.quantile.html

## Evidence and boundaries

```bash
python -m pytest -q tests/unit/test_risk_quantile_validation.py
make quality-check
make security-check
make readiness-check
```

Tests compare ordinary float64 results directly with the existing pandas
calculation and assert input immutability. Other cases cover invalid confidence
on empty/nonempty histories, partial/all-missing data, numeric representations,
integer interpolation, non-finite inputs/results and fixed backend diagnostics.
No observation values are included in validation errors.

No source identities, model versions, risk thresholds, schemas, dependencies,
portfolio positions or deployment defaults change. This candidate is independent
of pending processing and notification PRs. Automated validation and self-review
remain distinct from independent review and final engineer acceptance.
