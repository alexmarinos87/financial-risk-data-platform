from pathlib import Path


def test_operational_readiness_schema_exposes_append_only_serving_contract() -> None:
    sql = Path("sql/operational_readiness_decisions_schema.sql").read_text(
        encoding="utf-8"
    )

    assert (
        "CREATE TABLE IF NOT EXISTS risk_platform.operational_readiness_decisions"
        in sql
    )
    assert "REFERENCES\n        risk_platform.operational_service_level_reports" in sql
    for view in (
        "operational_readiness_decision_history",
        "operational_readiness_reason_history",
        "latest_operational_readiness_decisions",
        "current_allowed_operational_readiness_decisions",
        "current_blocked_operational_readiness_decisions",
    ):
        assert f"risk_platform.{view}" in sql
    assert "ORDER BY evaluated_at DESC, decision_id DESC" in sql
    assert "operational readiness decisions are append-only" in sql
    assert "prevent_operational_readiness_decision_update" in sql
    assert "prevent_operational_readiness_decision_delete" in sql


def test_operational_readiness_consistency_checks_cover_identity_and_views() -> None:
    sql = Path(
        "sql/operational_readiness_decisions_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for check_name in (
        "operational_readiness_history_rows_match",
        "operational_readiness_reason_rows_match",
        "operational_readiness_current_views_partition_latest",
        "operational_readiness_report_references_exist",
        "operational_readiness_report_evidence_matches_source",
        "operational_readiness_report_age_reconciles",
        "operational_readiness_reason_semantics_reconcile",
        "operational_readiness_decision_ids_unique",
        "latest_operational_readiness_grain_unique",
        "latest_operational_readiness_selects_current_decision",
        "operational_readiness_append_only_triggers_present",
    ):
        assert check_name in sql
    assert "check_name" in sql
    assert "expected" in sql
    assert "actual" in sql
    assert "status" in sql
