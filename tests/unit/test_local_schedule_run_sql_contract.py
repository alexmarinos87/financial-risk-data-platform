from __future__ import annotations

from pathlib import Path


def test_local_schedule_run_schema_exposes_append_only_serving_contract() -> None:
    schema = Path("sql/local_schedule_runs_schema.sql").read_text(encoding="utf-8")

    for fragment in (
        "CREATE TABLE IF NOT EXISTS risk_platform.local_schedule_runs",
        "operational_readiness_decisions (decision_id)",
        "operational_readiness_overrides (override_id)",
        "validate_local_schedule_run_authority",
        "prevent_local_schedule_run_update",
        "prevent_local_schedule_run_delete",
        "local_schedule_run_session_history",
        "local_schedule_run_stage_history",
        "recent_local_schedule_runs",
        "current_local_schedule_run_status",
        "current_local_schedule_run_failures",
        "incomplete_local_schedule_sessions",
        "jsonb_array_elements(run.run_json -> 'sessions')",
    ):
        assert fragment in schema

    assert "authority_type IN ('gate_allow', 'active_override')" in schema
    assert "run_status IN ('completed', 'failed')" in schema
    assert "provider_request_performed')::BOOLEAN = FALSE" in schema
    assert "notification_delivery_performed')::BOOLEAN = FALSE" in schema
    assert "cloud_schedule_activated')::BOOLEAN = FALSE" in schema


def test_local_schedule_run_reconciliation_uses_shared_result_contract() -> None:
    checks = Path("sql/local_schedule_runs_consistency_checks.sql").read_text(
        encoding="utf-8"
    )

    for fragment in (
        "AS check_name",
        "AS expected",
        "AS actual",
        "AS status",
        "local_schedule_run_sessions_expand_exactly",
        "local_schedule_run_stages_expand_exactly",
        "local_schedule_run_readiness_contracts_match",
        "local_schedule_run_override_contracts_match",
        "current_local_schedule_run_selects_latest",
        "current_local_schedule_failures_match_current_status",
        "incomplete_local_schedule_sessions_match_terminal_status",
        "local_schedule_run_append_only_triggers_present",
    ):
        assert fragment in checks
