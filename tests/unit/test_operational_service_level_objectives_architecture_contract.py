from pathlib import Path


def test_operational_objective_documentation_matches_report_only_contract() -> None:
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
        "provider",
        "automated remediation",
        "does not yet persist objective attainment in PostgreSQL",
    ):
        assert required.lower() in documentation.lower()
