from pathlib import Path


def test_readiness_enforced_schedule_documentation_preserves_control_boundary() -> None:
    documentation = Path(
        "docs/readiness-enforced-local-schedule.md"
    ).read_text(encoding="utf-8")

    for required in (
        "operational-readiness-execution-authority-v1",
        "authority_type = gate_allow",
        "authority_type = active_override",
        "A missing decision cannot be overridden",
        "fails before commands",
        "checkpoint is written only after every command",
        "No live provider request is added to CI",
    ):
        assert required in documentation
