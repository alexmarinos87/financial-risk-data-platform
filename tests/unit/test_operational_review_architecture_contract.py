from pathlib import Path


def test_operational_review_documentation_does_not_claim_a_deployed_dashboard() -> None:
    documentation = Path("docs/operational-review-queries.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "current_operational_health_summary",
        "current_operational_exception_summary",
        "recent_operational_readiness_decisions",
        "rolling_operational_objective_attainment",
        "operational_evidence_drillthrough",
        "Missing evidence remains explicit",
        "No dashboard is deployed",
    ):
        assert required in documentation
