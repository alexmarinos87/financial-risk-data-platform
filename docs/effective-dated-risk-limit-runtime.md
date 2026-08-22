# Effective-Dated Risk-Limit Runtime Selection

## Outcome

The portfolio risk-limit runner selects the policy version valid at the requested
end date and rejects a bounded request that crosses its temporal boundary:

```text
policy history + end date
  -> exactly one effective-dated policy version
  -> validate the requested range is contained by that version
  -> evaluate current attribution snapshots
  -> bind evaluation identity to the temporal policy fingerprint
  -> publish and serve retained versions as before
```

This is a conservative first runtime rule. Operators split a broad historical
request at policy boundaries rather than allowing one run to silently relabel
observations under several threshold regimes.

## Selection

`run_portfolio_risk_limits` calls the effective-dated policy loader with:

- the policy configuration path;
- the policy ID; and
- the requested inclusive end date.

Selection uses inclusive `effective_from` and exclusive nullable `effective_to`.
A gap, overlap, malformed policy history, or uncovered end date fails before
attribution Parquet is read.

## Range Boundary

After selection, `validate_policy_range` checks the optional start date and the
required end date. Both must be contained by the selected version.

For example:

```text
v1: [2026-01-01, 2026-07-01)
v2: [2026-07-01, infinity)
```

This request is valid:

```text
2026-01-01 through 2026-06-30 -> v1
```

This request is rejected and must be split:

```text
2026-06-01 through 2026-07-31 -> crosses v1/v2
```

Boundary rejection happens before local attribution input is read or output is
written.

## Versioned Evidence

The existing evaluation calculation ID already binds `policy.fingerprint`.
Because the runtime now supplies the temporal fingerprint:

- a policy renewal receives new evaluation IDs even when thresholds are
  unchanged;
- a threshold change receives new evaluation IDs;
- replay under one policy version converges on the same IDs; and
- earlier Parquet and PostgreSQL evidence remains retained.

No evaluation schema migration is required. The stable policy ID remains
queryable while the policy fingerprint distinguishes versions.

## Run Summary

The credential-free JSON summary now includes:

```text
policy_version_id
policy_fingerprint
limit_definition_fingerprint
effective_from
effective_to
```

This makes the selected governance state visible to the operator without placing
free text, credentials, or mutable approval state in evaluation rows.

## Boundary

This increment does not automatically segment one request across multiple policy
versions. It also does not change portfolio-mandate selection, PostgreSQL schema,
breach lifecycle, acknowledgements, notification candidates, provider access,
scheduling, deployment, or Terraform.
