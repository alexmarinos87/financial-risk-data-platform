# PostgreSQL CI Contract

## Outcome

The repository validates its SQL and loader boundary against PostgreSQL 16 on
every pull request. This complements the fast DuckDB contract tests with one
real-engine path:

```text
seeded core warehouse contract
  -> core reconciliation must pass
  -> deterministic Alpha Vantage-shaped local fixture
  -> daily, portfolio, attribution and risk-limit Parquet
  -> calculation-ID upserts into PostgreSQL
  -> all focused reconciliation suites must pass
  -> identical second load must remain convergent
```

The job is named `PostgreSQL contract` in `.github/workflows/ci.yml`.

## Safety Boundary

The job starts only the repository's loopback-bound PostgreSQL Docker service.
It does not start MongoDB, call Alpha Vantage, use credentials, deploy
infrastructure, run Terraform apply or contact a managed database.

The fixture is built by:

```bash
python -m src.orchestration.build_postgres_contract_fixture
```

It creates 26 deterministic daily observations for AAPL and MSFT, then uses the
normal application code to produce:

- 25 daily returns per symbol;
- 25 complete-date portfolio returns;
- six rolling 20-observation attribution snapshots; and
- twelve risk-limit evaluations.

The dates and prices are fixed. No current clock value enters a calculation ID.
Generated Parquet and summaries remain under ignored local paths.

## Failure Semantics

The existing SQL files return `check_name`, `expected`, `actual` and `status`.
Running them with `psql` alone prints failed rows but still exits successfully.
`src.warehouse.postgres_consistency` executes the files in a read-only database
transaction and returns a non-zero process status when:

- SQL execution fails;
- a result set is missing the required columns;
- a query returns no checks;
- a status is not `pass` or `fail`; or
- one or more checks return `fail`.

Only regular `.sql` files under the repository `sql/` directory are accepted,
and each file is capped at 2 MB.

## Local Command

Run the same contract locally with Docker available:

```bash
PYTHON=.venv/bin/python make postgres-contract-check
```

The target recreates the disposable local PostgreSQL container and leaves it
running for inspection. Stop it with:

```bash
make local-db-down
```

The contract first checks the seeded minute-oriented demo before loading the
daily data. This ordering is deliberate: the core reconciliation has exact
six-event expectations, while the later stages add Alpha Vantage daily raw rows
to the same warehouse.

## Evidence Covered

The job executes:

- `sql/consistency_checks.sql` against the seeded core demo;
- `sql/daily_risk_consistency_checks.sql`;
- `sql/portfolio_risk_consistency_checks.sql`;
- `sql/portfolio_attribution_consistency_checks.sql`; and
- `sql/portfolio_risk_limits_consistency_checks.sql`.

The daily-to-limit loaders run twice before the final checks. Primary keys and
calculation-ID upserts must converge without duplicate current grains.

## Remaining Boundary

This is an ephemeral CI database, not production persistence. Backup, restore,
high availability, migrations across deployed environments, access control,
monitoring and managed-database operations remain outside this repository's
implemented runtime.
