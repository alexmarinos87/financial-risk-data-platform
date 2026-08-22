# Effective-Dated Portfolio Mandates

## Purpose

A portfolio definition is not only a set of weights. It is a mandate that owns a
bounded period of time. Historical calculations must resolve the definition that
was effective for the requested date rather than silently applying today's
weights to earlier observations.

The mandate selector lives in:

```text
src/analytics/portfolio_mandates.py
```

The repository configuration now gives each demonstration portfolio an explicit
mandate identity and validity range:

```yaml
portfolios:
  us-tech-equal:
    mandate_id: us-tech-equal-v1
    effective_from: 2020-01-01
    effective_to: null
    base_currency: USD
    constituents:
      - source: alpha_vantage
        symbol: AAPL
        weight: 0.5
      - source: alpha_vantage
        symbol: MSFT
        weight: 0.5
```

`effective_from` is inclusive. `effective_to` is exclusive. A null upper bound is
open-ended and may appear only on the final mandate.

## Multiple Mandates

The selector also accepts an ordered mandate history:

```yaml
portfolios:
  us-tech:
    description: Effective-dated portfolio
    mandates:
      - mandate_id: us-tech-v1
        effective_from: 2025-01-01
        effective_to: 2026-01-01
        base_currency: USD
        constituents:
          - source: alpha_vantage
            symbol: AAPL
            weight: 0.5
          - source: alpha_vantage
            symbol: MSFT
            weight: 0.5

      - mandate_id: us-tech-v2
        effective_from: 2026-01-01
        effective_to: null
        base_currency: USD
        constituents:
          - source: alpha_vantage
            symbol: AAPL
            weight: 0.6
          - source: alpha_vantage
            symbol: MSFT
            weight: 0.4
```

All mandates are parsed and validated before one is selected. The contract rejects:

- duplicate mandate IDs;
- invalid or non-canonical dates;
- an end date that is not after its start date;
- overlapping mandate ranges;
- an open-ended mandate followed by another mandate;
- mixed direct and `mandates` definitions; and
- a requested date that has no unique covering mandate.

Gaps are permitted because a portfolio may be inactive. A calculation within a
gap fails explicitly rather than selecting the nearest definition.

## Identity

Two deterministic identities are exposed:

```text
constituent_definition_fingerprint
mandate_fingerprint
```

The constituent-definition fingerprint matches the existing portfolio identity:
portfolio ID, base currency, sources, symbols and weights.

The mandate fingerprint additionally binds:

```text
mandate ID
effective_from
effective_to
constituent-definition fingerprint
```

Therefore a formally renewed mandate remains distinguishable even when its
weights are unchanged.

## Range Semantics

A bounded calculation must fit within one selected mandate:

```text
selected effective_from <= start_date <= end_date < selected effective_to
```

The upper check is omitted for an open-ended mandate. A request that crosses a
boundary must be split into one run per mandate. The selector does not blend two
weight definitions into one return, covariance or risk-limit calculation.

When a caller omits `start_date`, records can be filtered through
`filter_records_to_mandate`. This excludes observations before the selected
mandate without accepting malformed or timezone-naive event timestamps.

## Compatibility Boundary

`parse_portfolio_definition` remains available for pure analytical unit tests and
legacy in-memory payloads. It ignores the additional direct mandate metadata and
continues to produce the existing constituent definition.

The effective selector is the governed contract for operator-facing workflows:

```python
mandate = load_portfolio_mandate(
    Path("config/portfolios.yaml"),
    "us-tech-equal",
    date(2026, 1, 26),
)
```

The governed cross-stage operator path is:

```text
src/orchestration/run_governed_portfolio_cycle.py
```

It selects the mandate containing the requested end date, validates that the
effective calculation range stays inside that mandate, filters every input
reader, and injects the same mandate into portfolio risk, rolling attribution and
risk-limit evaluation. See `docs/governed-portfolio-cycle.md`.

The individual portfolio, attribution and risk-limit commands remain lower-level
development entry points. Use the governed cycle when one operator run must
guarantee cross-stage mandate consistency.

## Remaining Boundary

This repository does not yet provide automatic range splitting, mandate
activation, a database-managed configuration registry or automated notification
delivery. YAML remains the reviewed source of truth. A new mandate still requires
a normal pull request and green CI evidence; the existing append-only
acknowledgement contract records review evidence but is not an automatic
approval system.
