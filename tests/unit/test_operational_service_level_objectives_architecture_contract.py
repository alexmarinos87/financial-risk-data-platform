from pathlib import Path


def test_operational_objective_documentation_matches_persistent_contract() -> None:
    documentation = " ".join(
        Path("docs/operational-service-level-objectives.md")
        .read_text(encoding="utf-8")
        .split()
    )

    for required in (
        "operational-slo-attainment-v1",
        "operational-slo-objective-policy-",
        "minimum_observations",
        "history_status = insufficient",
        "missing report",
        "as_of DESC",
        "calculation_id DESC",
        "run_operational_service_level_objectives",
        "operational_service_level_objective_recorder",
        "operational_service_level_objective_reports",
        "current_operational_service_level_objective_status",
        "current_operational_service_level_objective_exceptions",
        "provider",
        "automated remediation",
        "append-only PostgreSQL history",
    ):
        assert required.lower() in documentation.lower()

    assert (
        "does not yet persist objective attainment in PostgreSQL"
        not in documentation.lower()
    )
