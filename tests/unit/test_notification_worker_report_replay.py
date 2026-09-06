from __future__ import annotations

import copy
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration import check_notification_worker_preflight as command
from test_check_notification_worker_preflight import arguments, forbidden_reader
from test_reviewed_notification_worker_authority import WORKER_ID, configurations as configurations
from test_reviewed_notification_worker_preflight import bundle


def report(paths: dict[str, Path], mode: str = "live_database_read") -> dict[str, Any]:
    return {"source_mode": mode, "database_read_performed": mode == "live_database_read",
            "runtime_permission_granted": False, "result": bundle(paths)}


def selection(value: dict[str, Any]) -> dict[str, str]:
    evidence = value["result"]["evidence"]
    return {key: evidence[key] for key in ("worker_id", "selected_transition_id", "scheduled_for")}


@pytest.mark.parametrize("mode", ["live_database_read", "retained_file", "retained_report"])
def test_all_capture_modes_round_trip_without_database(
    configurations: dict[str, Path], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: Any, mode: str,
) -> None:
    captured = report(configurations, mode)
    original = copy.deepcopy(captured)
    path = tmp_path / "report.json"
    path.write_text(json.dumps(captured), encoding="utf-8")
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "never-use-offline")
    monkeypatch.setattr(command, "read_worker_authority_snapshot", forbidden_reader)
    args = arguments(configurations, captured["result"]["snapshot"], ["--report", str(path)])
    assert command.main(args) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["source_mode"] == "retained_report"
    assert output["database_read_performed"] is False
    assert output["runtime_permission_granted"] is False
    assert output["result"] == captured["result"]
    assert command.validate_preflight_report(output, **selection(captured), **configurations) == captured["result"]
    assert captured == original


@pytest.mark.parametrize(("field", "value"), [
    ("source_mode", "unknown"), ("source_mode", []),
    ("source_mode", "retained_file"), ("database_read_performed", False),
    ("database_read_performed", 1), ("runtime_permission_granted", True),
    ("runtime_permission_granted", 0), ("extra", "not permitted"),
])
def test_wrapper_tampering_is_rejected(configurations: dict[str, Path], field: str, value: Any) -> None:
    captured = report(configurations)
    wanted = selection(captured)
    captured[field] = value
    with pytest.raises(ValidationError):
        command.validate_preflight_report(captured, **wanted, **configurations)


@pytest.mark.parametrize("field", ["worker_id", "selected_transition_id", "scheduled_for"])
def test_report_cannot_change_explicit_selection(configurations: dict[str, Path], field: str) -> None:
    captured = report(configurations)
    wanted = selection(captured)
    wanted[field] = "different" if field != "scheduled_for" else "2026-01-01T00:00:00+00:00"
    with pytest.raises(ValidationError):
        command.validate_preflight_report(captured, **wanted, **configurations)


def test_equivalent_selected_timezone_normalizes_and_result_is_detached(configurations: dict[str, Path]) -> None:
    captured = report(configurations)
    wanted = selection(captured)
    wanted["scheduled_for"] = datetime.fromisoformat(wanted["scheduled_for"]).astimezone(
        timezone(timedelta(hours=1))
    ).isoformat()
    result = command.validate_preflight_report(captured, **wanted, **configurations)
    assert result == captured["result"]
    result["preflight"]["reasons"].append("tampered")
    assert captured["result"]["preflight"]["reasons"] == []


def test_inner_outcome_and_changed_configuration_rebuild(configurations: dict[str, Path]) -> None:
    captured = report(configurations)
    wanted = selection(captured)
    broken = copy.deepcopy(captured)
    broken["result"]["preflight"]["outcome"] = "may_run"
    with pytest.raises(ValidationError):
        command.validate_preflight_report(broken, **wanted, **configurations)
    path = configurations["worker_config_path"]
    document = yaml.safe_load(path.read_text())
    document["workers"][WORKER_ID]["enabled"] = False
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    with pytest.raises(ValidationError):
        command.validate_preflight_report(captured, **wanted, **configurations)


def test_report_input_reuses_bounded_non_symlink_duplicate_rejection(
    configurations: dict[str, Path], tmp_path: Path, capsys: Any,
) -> None:
    captured = report(configurations)
    path = tmp_path / "report.json"
    args = arguments(configurations, captured["result"]["snapshot"], ["--report", str(path)])
    for raw in (b'{"source_mode":"a","source_mode":"b"}', b'null',
                b' ' * (command.MAX_BUNDLE_BYTES + 1)):
        path.write_bytes(raw)
        assert command.main(args) == 1
        assert capsys.readouterr().out == ""
    target = tmp_path / "target.json"
    target.write_text(json.dumps(captured), encoding="utf-8")
    path.unlink()
    path.symlink_to(target)
    assert command.main(args) == 1
    assert capsys.readouterr().out == ""


def test_report_and_live_modes_are_exclusive(configurations: dict[str, Path], tmp_path: Path) -> None:
    captured = report(configurations)
    args = arguments(configurations, captured["result"]["snapshot"], ["--report", "unused", "--read-current"])
    with pytest.raises(SystemExit) as caught:
        command.main(args)
    assert caught.value.code == 2
