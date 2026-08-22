from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(
        encoding="utf-8"
    )


def test_policy_schedule_schema_adds_period_serving() -> None:
    sql = _read(
        "sql/portfolio_risk_limit_policy_schedule_schema.sql"
    )

    assert (
        "ADD COLUMN IF NOT EXISTS "
        "policy_effective_from DATE"
        in sql
    )
    assert (
        "ADD COLUMN IF NOT EXISTS "
        "policy_effective_to DATE"
        in sql
    )
    assert (
        "ADD COLUMN IF NOT EXISTS "
        "policy_period_source TEXT"
        in sql
    )
    assert "policy_effective_to IS NULL" in sql
    assert "policy_period_source IN" in sql
    assert (
        "policy_effective_from::TIMESTAMP"
        in sql
    )
    for view_name in (
        "latest_portfolio_risk_limit_policy_evaluations",
        "portfolio_risk_limit_policy_breaches",
        "portfolio_risk_limit_policy_snapshot_status",
        "portfolio_risk_limit_policy_versions_observed",
    ):
        assert (
            "CREATE OR REPLACE VIEW "
            f"risk_platform.{view_name}"
            in sql
        )


def test_policy_schedule_checks_cover_contract() -> None:
    sql = _read(
        "sql/portfolio_risk_limit_policy_schedule_consistency_checks.sql"
    )
    for check_name in (
        "portfolio_risk_limit_policy_periods_cover_evaluations",
        "portfolio_risk_limit_policy_fingerprint_has_one_period",
        "portfolio_risk_limit_policy_periods_do_not_overlap",
        "latest_policy_evaluation_rows_match_latest",
        "policy_breach_view_matches_latest_breaches",
        "policy_snapshot_status_rows_match_current_snapshots",
        "policy_snapshot_status_values_reconcile",
    ):
        assert check_name in sql
    assert "LAG(policy_effective_to)" in sql
    assert (
        "IS NOT DISTINCT FROM actual.policy_effective_to"
        in sql
    )


def test_docker_and_docs_expose_operator_path() -> None:
    compose = _read("docker-compose.yml")
    schedule_doc = _read(
        "docs/portfolio-risk-limit-policy-schedule.md"
    )

    assert (
        "08_portfolio_risk_limit_policy_schedule_schema.sql"
        in compose
    )
    for text in (
        "effective_from",
        "effective_to",
        "configured",
        "legacy_unbounded",
        "inferred_event_date",
        "current-version history",
        "portfolio_risk_limit_policy_versions_observed",
    ):
        assert text in schedule_doc
