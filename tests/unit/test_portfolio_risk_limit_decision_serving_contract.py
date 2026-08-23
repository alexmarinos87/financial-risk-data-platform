from pathlib import Path


def test_decision_schema_preserves_history_and_derives_lifecycle() -> None:
    sql = Path("sql/portfolio_risk_limit_decisions_schema.sql").read_text(
        encoding="utf-8"
    )

    for required in (
        "portfolio_risk_limit_decisions",
        "latest_portfolio_risk_limit_decisions",
        "portfolio_risk_limit_breach_lifecycle",
        "open_portfolio_risk_limit_breaches",
        "acknowledged_portfolio_risk_limit_breaches",
        "resolved_portfolio_risk_limit_breaches",
        "waived_portfolio_risk_limit_breaches",
        "portfolio_risk_limit_decision_summary",
        "PARTITION BY notification_id",
        "ORDER BY decided_at DESC, decision_id DESC",
        "current_portfolio_risk_limit_notifications",
    ):
        assert required in sql

    assert "UPDATE risk_platform.portfolio_risk_limit_notifications" not in sql
    assert "DELETE FROM risk_platform.portfolio_risk_limit_notifications" not in sql


def test_decision_consistency_checks_cover_lifecycle_invariants() -> None:
    sql = Path(
        "sql/portfolio_risk_limit_decisions_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for check_name in (
        "risk_limit_decisions_reference_notifications",
        "risk_limit_decision_ids_unique",
        "latest_decision_notification_grain_unique",
        "latest_decision_selects_current_version",
        "risk_limit_decision_contract_valid",
        "risk_limit_decision_lifecycle_valid",
        "decision_lifecycle_rows_match_current_notifications",
        "decision_lifecycle_partitions_current_notifications",
    ):
        assert check_name in sql
