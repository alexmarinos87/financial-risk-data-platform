from __future__ import annotations

from pathlib import Path


def test_local_schedule_run_history_documentation_preserves_arc42_boundary() -> None:
    document = Path("docs/local-schedule-run-history.md").read_text(encoding="utf-8")

    for fragment in (
        "# Append-Only Local Schedule Run History",
        "Primary arc42 block: `warehouse`",
        "local-schedule-run-v1",
        "exact retries converge",
        "current_local_schedule_run_failures",
        "incomplete_local_schedule_sessions",
        "No command arguments",
        "no external notification delivery",
        "no cloud schedule activation",
        "no `terraform apply`",
        "P4b",
    ):
        assert fragment in document
