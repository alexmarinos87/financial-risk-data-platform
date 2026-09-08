# Reject non-finite daily analytics before publication

Primary arc42 block: `analytics`. Goal #225, under roadmap #76.

## Reproduced failure

The existing source contract requires finite positive prices, but arithmetic
on such prices can still overflow. Two closes of `1e-308` and `1e308` previously
produced an infinite return before any quantile was calculated. Adding another
`1e308` close could yield NaN volatility and a zero loss after the signed
infinite quantile was negated and clamped. A separate history `[1, 1e200, 1]`
has finite returns but overflows rolling sample variance.

## Decision

`build_daily_risk_outputs` now checks every calculated return after the expected
first missing return, all fully warmed-up annualized volatility, and the drawdown
series before cumulative minimum selection. It checks the signed quantile before
converting it to a nonnegative loss. No invalid result is clipped, zero-filled,
dropped or reclassified as ordinary warm-up.

Expected warm-up stays unchanged: the first close has no prior return, and
volatility/loss values remain None where their existing observation requirements
are not met. The model version, calculation IDs, annualization, sample-standard-
deviation method, quantile convention and ordinary date-selection results are
unchanged. Finite metrics retain normal binary64 limitations; rejection is not
arbitrary-precision recalculation or proof of statistical adequacy.

Checks cover the retained history after the existing end-date filter. Start date
continues to filter only outputs; it cannot hide invalid historical observations.
The policy is conservative: an invalid earlier fully formed volatility value
also rejects the retained history even when later output dates are requested.

Pandas describes pct_change as fractional change rather than percentage units:
https://pandas.pydata.org/docs/reference/api/pandas.Series.pct_change.html

## Publication boundary

The existing runner constructs the complete DailyRiskOutputs before invoking a
curated writer. A numerical ValidationError therefore prevents publication of
all three datasets in that run, including when a preceding symbol/date had valid
candidate records. No production runner or storage code changes are required.
This is not a general transaction across three datasets: unrelated writer failures
retain the runner's prior partial-publication/replay behavior.

The shared quantile helper has a separate repair in goal #224 / PR #228. This
candidate deliberately does not depend on it: short histories and volatility
need their own boundary, and this layer also checks the helper's returned value.
Both candidates are directly based on accepted main and touch different source
files. Their combined integration still needs validation after acceptance.

## Validation

```bash
python -m pytest -q tests/unit/test_daily_risk_finite_outputs.py \
  tests/unit/test_daily_risk_publication_guard.py
make quality-check
make security-check
make readiness-check
```

Unit tests use the actual MarketEvent model and daily builder. They cover finite
price/variance overflow, invalid backend results, warm-up, date filtering, replay,
input immutability and fixed baseline calculation identifiers. Runner tests use
the real builder with injected readers/writers: bad data never reaches any write,
while ordinary history publishes eight records across the three datasets. A CLI
case checks exit 1, no curated writes and no summary-file creation. These do not
claim live provider access or a real database write.

No schema, thresholds, source IDs, dependencies, activation defaults, portfolio
positions, scheduling or deployment changes. Independent review and explicit
engineer acceptance remain separate from tests and self-review.
