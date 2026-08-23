from pathlib import Path


def test_operational_objective_schema_exposes_append_only_serving_contract() -> None:
    schema = Path(
        "sql/operational_service_level_objectives_schema.sql"
    ).read_text(encoding="utf-8")

    for required in (
        "CREATE TABLE IF NOT EXISTS risk_platform.operational_service_level_objective_reports",
        "operational-slo-attainment-v1",
        "operational-slo-objective-policy-",
        "prevent_operational_slo_objective_report_update",
        "prevent_operational_slo_objective_report_delete",
        "operational_service_level_objective_metric_history",
        "latest_operational_service_level_objective_reports",
        "current_operational_service_level_objective_status",
        "current_operational_service_level_objective_exceptions",
        "ORDER BY calculated_at DESC, calculation_id DESC",
        "jsonb_array_length(objectives_json) = 4",
        "NOT automated_remediation_performed",
    ):
        assert required in schema


def test_operational_objective_consistency_checks_cover_current_contract() -> None:
    checks = Path(
        "sql/operational_service_level_objectives_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for check_name in (
        "operational_slo_objective_source_report_references_valid",
        "operational_slo_objective_source_contracts_align",
        "operational_slo_objective_counts_reconcile",
        "operational_slo_objective_sources_align",
        "operational_slo_objective_statuses_reconcile",
        "operational_slo_objective_overall_status_reconciles",
        "operational_slo_objective_calculation_ids_unique",
        "latest_operational_slo_objective_grain_unique",
        "latest_operational_slo_objective_selects_current_version",
        "operational_slo_objective_history_rows_match_reports",
        "current_operational_slo_objective_rows_match_latest",
        "current_operational_slo_objective_exceptions_match_status",
    ):
        assert check_name in checks
