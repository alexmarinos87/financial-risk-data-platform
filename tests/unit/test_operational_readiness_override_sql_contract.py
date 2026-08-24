from pathlib import Path


def test_readiness_override_schema_is_append_only_and_time_bounded() -> None:
    sql = Path("sql/operational_readiness_overrides_schema.sql").read_text(
        encoding="utf-8"
    )

    assert "risk_platform.operational_readiness_overrides" in sql
    assert "risk_platform.operational_readiness_override_revocations" in sql
    assert "INTERVAL '24 hours'" in sql
    assert "readiness override target must be blocked" in sql
    assert "readiness override metadata does not match target" in sql
    for view in (
        "operational_readiness_override_history",
        "current_operational_readiness_override_status",
        "active_operational_readiness_overrides",
    ):
        assert f"risk_platform.{view}" in sql
    for trigger in (
        "prevent_operational_readiness_override_update",
        "prevent_operational_readiness_override_delete",
        "prevent_operational_readiness_override_revocation_update",
        "prevent_operational_readiness_override_revocation_delete",
    ):
        assert trigger in sql


def test_readiness_override_reconciliation_covers_targets_and_current_status() -> None:
    sql = Path(
        "sql/operational_readiness_overrides_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for check_name in (
        "operational_readiness_override_history_rows_match",
        "operational_readiness_override_targets_exist",
        "operational_readiness_override_targets_blocked_exact_contract",
        "operational_readiness_override_windows_bounded",
        "operational_readiness_override_revocations_valid",
        "operational_readiness_override_identities_unique",
        "operational_readiness_override_revocation_identities_unique",
        "current_operational_readiness_override_grain_unique",
        "current_operational_readiness_override_selects_latest",
        "current_operational_readiness_override_status_reconciles",
        "active_operational_readiness_overrides_match_current_status",
        "operational_readiness_override_append_only_triggers_present",
    ):
        assert check_name in sql
