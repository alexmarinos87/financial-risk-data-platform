from pathlib import Path


def test_operational_readiness_decision_documentation_matches_runtime() -> None:
    documentation = Path("docs/operational-readiness-decisions.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "operational-readiness-gate-v1",
        "risk_platform.operational_readiness_decisions",
        "latest_operational_readiness_decisions",
        "current_allowed_operational_readiness_decisions",
        "current_blocked_operational_readiness_decisions",
        "evaluated_at DESC",
        "decision_id DESC",
        "append-only",
        "No schedule command is executed",
    ):
        assert required in documentation
