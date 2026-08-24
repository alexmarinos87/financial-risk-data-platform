from pathlib import Path


def test_readiness_aware_schedule_plan_is_documented_as_plan_only() -> None:
    documentation = Path("docs/readiness-aware-schedule-planning.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "would_run, would_block or no_work",
        "decision_missing",
        "readiness-aware-schedule-plan-v1",
        "The command exposes no `--execute` option",
        "checkpoint_updated = false",
        "This increment does not grant execution authority",
    ):
        assert required in documentation
