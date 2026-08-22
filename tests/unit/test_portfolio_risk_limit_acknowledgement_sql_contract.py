from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_risk_limit_schema_exposes_append_only_acknowledgements() -> None:
    sql = _read("sql/portfolio_risk_limits_schema.sql")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "risk_platform.portfolio_risk_limit_acknowledgements" in sql
    )
    assert "portfolio-risk-limit-ack-v1" in sql
    assert "UNIQUE (evaluation_calculation_id, request_id)" in sql
    assert "validate_risk_limit_acknowledgement_insert" in sql
    assert "prevent_risk_limit_acknowledgement_update" in sql
    assert "prevent_risk_limit_acknowledgement_delete" in sql
    assert "risk-limit acknowledgements are append-only" in sql

    for view in (
        "portfolio_risk_limit_acknowledgement_history",
        "portfolio_risk_limit_breach_status",
        "portfolio_risk_limit_open_breaches",
        "portfolio_risk_limit_acknowledged_breaches",
    ):
        assert f"CREATE OR REPLACE VIEW risk_platform.{view}" in sql

    assert (
        "acknowledgement.evaluation_calculation_id = breach.calculation_id" in sql
    )
    assert "acknowledgement.acknowledged_at DESC" in sql
    assert "acknowledgement.acknowledgement_id DESC" in sql
    assert "THEN 'unacknowledged'" in sql


def test_acknowledgement_consistency_contract_covers_views_and_controls() -> None:
    sql = _read("sql/portfolio_risk_limits_consistency_checks.sql")

    for check_name in (
        "portfolio_risk_limit_acknowledgement_rows_valid",
        "portfolio_risk_limit_acknowledgements_reference_breaches",
        "portfolio_risk_limit_acknowledgement_targets_are_valid",
        "portfolio_risk_limit_acknowledgement_requests_unique",
        "portfolio_risk_limit_acknowledgement_history_matches_base",
        "portfolio_risk_limit_breach_status_matches_current_breaches",
        "portfolio_risk_limit_open_and_acknowledged_partition_breaches",
        "portfolio_risk_limit_breach_status_selects_latest_acknowledgement",
        "portfolio_risk_limit_acknowledgement_triggers_enabled",
    ):
        assert check_name in sql

    assert "pg_trigger" in sql
    assert "enabled_control_triggers = 3" in sql
    assert "IS DISTINCT FROM" in sql


def test_acknowledgement_documentation_keeps_human_authority_explicit() -> None:
    documentation = _read("docs/portfolio-risk-limits.md")

    for required in (
        "portfolio_risk_limit_acknowledgements",
        "portfolio_risk_limit_open_breaches",
        "portfolio_risk_limit_acknowledged_breaches",
        "portfolio-risk-limit-ack-v1",
        "append-only",
        "does not change the breach status",
    ):
        assert required in documentation
