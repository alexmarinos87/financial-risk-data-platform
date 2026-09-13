# Finite signed risk quantiles

Primary arc42 block: `analytics`. Goal #224, under roadmap #76.

## Decision

`src/analytics/risk_metrics.py` retains the historical signed lower-tail return
quantile. It is not a nonnegative loss amount. Valid confidence is a finite real
scalar strictly between zero and one; booleans are not probabilities. This is
checked even when the input Series is empty.

The helper requires a pandas Series. Explicit missing observations are excluded,
but a nonempty all-missing history is rejected. The
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
python -m pytest -q tests/unit/test_risk_quantile_validation.py \
  tests/unit/test_risk_quantile_missing_policy.py
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


## Missing values cannot hide invalid observations

Validation inspects the original scalar observations before constructing the
float64 quantile input. Missing means `None`, `pd.NA`, `pd.NaT` or a real numeric
NaN. Positive and negative infinity are always errors, including when a pandas 2
process has enabled `mode.use_inf_as_na`. The helper neither reads nor changes
that global setting and does not delegate its input policy to `dropna`.

At the previous candidate, `[0.0, inf, 0.0]` and `[0.0, -inf, 0.0]` could both
return 0.0 with that option enabled. Explicit classification prevents filtering
an invalid observation into an apparently valid risk estimate. Ordinary real
returns and the empty-Series sentinel retain their prior semantics.

This tightens missing-value compatibility for unsupported scalar types: Decimal
NaN, complex NaN and NumPy datetime/timedelta NaT are rejected rather than silently
discarded as absent returns. The common missing sentinels listed above remain
supported, including within nullable, object and numeric categorical Series.
Already-lost information cannot be recovered: an upstream caller that replaces
infinity with NaN before calling this helper has removed the distinction.

Pandas 2 documents the infinity-as-missing option, which pandas 3 removed:
https://pandas.pydata.org/pandas-docs/version/2.2/reference/api/pandas.Series.isna.html
https://pandas.pydata.org/docs/whatsnew/v3.0.0.html

The new tests exercise both real option settings when available, restore the
previous setting, and still run all contracts on pandas 3 without skips. The
injected `dropna` regression additionally challenges validation order regardless
of library version. Input immutability, true missing values and rejection before
quantile calculation are checked; no end-to-end warehouse publication is claimed
by these unit tests.
