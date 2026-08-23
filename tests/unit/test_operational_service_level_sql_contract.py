from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_operational_service_level_schema_is_append_only_and_queryable() -> None:
    sql = _read("sql/operational_service_levels_schema.sql")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "risk_platform.operational_service_level_reports"
    ) in sql
    for view_name in (
        "operational_service_level_metric_history",
        "latest_operational_service_level_reports",
        "current_operational_service_level_metric_status",
        "current_operational_service_level_exceptions",
    ):
        assert f"risk_platform.{view_name}" in sql
    assert "prevent_operational_service_level_report_update" in sql
    assert "prevent_operational_service_level_report_delete" in sql
    assert "operational service-level reports are append-only" in sql
    assert "jsonb_array_length(metrics_json) = 4" in sql
    assert "document_sha256 ~ '^[0-9a-f]{64}$'" in sql
    assert "NOT provider_request_performed" in sql
    assert "NOT external_delivery_performed" in sql
    assert "NOT cloud_schedule_activated" in sql


def test_operational_service_level_consistency_suite_covers_current_views() -> None:
    sql = _read("sql/operational_service_levels_consistency_checks.sql")
    for check_name in (
        "operational_service_level_reports_present",
        "operational_service_level_metric_contracts_valid",
        "operational_service_level_metric_values_valid",
        "operational_service_level_overall_status_reconciles",
        "operational_service_level_calculation_ids_unique",
        "latest_operational_service_level_grain_unique",
        "latest_operational_service_level_selects_current_report",
        "operational_service_level_metric_history_row_count",
        "current_operational_service_level_metric_row_count",
        "current_operational_service_level_exception_count",
    ):
        assert check_name in sql
