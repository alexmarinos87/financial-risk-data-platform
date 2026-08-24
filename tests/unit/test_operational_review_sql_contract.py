from pathlib import Path


def test_operational_review_schema_exposes_dashboard_ready_grains() -> None:
    sql = Path("sql/operational_review_schema.sql").read_text(encoding="utf-8")

    for view in (
        "current_operational_health_summary",
        "current_operational_exception_summary",
        "recent_operational_readiness_decisions",
        "rolling_operational_objective_attainment",
        "operational_evidence_drillthrough",
    ):
        assert f"risk_platform.{view}" in sql
    assert "readiness_missing" in sql
    assert "service_level_missing" in sql
    assert "objective_missing" in sql
    assert "decision_recency_rank" in sql
    assert "attainment_gap" in sql
    assert "parent_evidence_ids" in sql


def test_operational_review_consistency_contract_covers_all_views() -> None:
    sql = Path("sql/operational_review_consistency_checks.sql").read_text(
        encoding="utf-8"
    )

    for check_name in (
        "operational_review_health_rows_match_contracts",
        "operational_review_health_grain_unique",
        "operational_review_health_status_reconciles",
        "operational_review_exception_rows_match_sources",
        "operational_review_exception_identity_unique",
        "operational_review_recent_decisions_match_history",
        "operational_review_recent_decision_rank_matches_latest",
        "operational_review_objective_trend_rows_match_history",
        "operational_review_objective_trend_arithmetic_reconciles",
        "operational_review_drillthrough_rows_match_evidence",
        "operational_review_drillthrough_identity_unique",
        "operational_review_drillthrough_parents_exist",
    ):
        assert check_name in sql
