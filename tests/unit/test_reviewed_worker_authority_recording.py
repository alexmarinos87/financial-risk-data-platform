from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.warehouse import reviewed_notification_worker_authority_history as reviewed
from src.warehouse import notification_worker_authority_history as history
from test_notification_worker_authority_history import Cursor, row
from test_reviewed_notification_worker_authority import (
    _grant, _plan, configurations as configurations,
)


def test_preparation_is_exact_and_detached(configurations: dict[str, Path]) -> None:
    transition = _grant(_plan(configurations), configurations)
    result = reviewed.prepare_reviewed_worker_authority(transition=transition, **configurations)
    assert result == transition
    transition["plan"]["execution"]["work_items"].clear()
    assert result["plan"]["execution"]["work_items"]


def test_recording_passes_only_canonical_transition_to_existing_recorder(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition = _grant(_plan(configurations), configurations)
    calls: list[dict[str, Any]] = []

    def recorder(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {"created": True, "runtime_permission_granted": False}

    monkeypatch.setattr(reviewed, "record_worker_authority", recorder)
    result = reviewed.record_reviewed_worker_authority(
        dsn="injected-local-dsn", transition=transition, **configurations,
    )
    assert result == {"created": True, "runtime_permission_granted": False}
    assert calls == [{"dsn": "injected-local-dsn", "transition": transition}]


def test_configuration_mismatch_never_reaches_persistence(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition = _grant(_plan(configurations), configurations)
    path = configurations["worker_config_path"]
    document = yaml.safe_load(path.read_text())
    document["workers"]["risk-operations-managed"]["enabled"] = False
    path.write_text(yaml.safe_dump(document), encoding="utf-8")

    def forbidden(**kwargs: Any) -> dict[str, Any]:
        raise AssertionError("persistence must not be called")

    monkeypatch.setattr(reviewed, "record_worker_authority", forbidden)
    with pytest.raises(ValidationError, match="does not match"):
        reviewed.record_reviewed_worker_authority(
            dsn="unused", transition=transition, **configurations,
        )


def test_delegation_preserves_exact_replay_and_locked_head_rejection(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition = _grant(_plan(configurations), configurations)
    replay_cursor = Cursor([row(transition)])

    def replay(**kwargs: Any) -> dict[str, Any]:
        return history.record_worker_authority_with_cursor(
            replay_cursor, transition=kwargs["transition"],
        )

    monkeypatch.setattr(reviewed, "record_worker_authority", replay)
    assert reviewed.record_reviewed_worker_authority(
        dsn="unused", transition=transition, **configurations,
    )["created"] is False
    assert not any("INSERT" in sql for sql, _ in replay_cursor.calls)

    def stale(**kwargs: Any) -> dict[str, Any]:
        return history.record_worker_authority_with_cursor(
            Cursor([None, row(transition)]), transition=kwargs["transition"],
        )

    competing = _grant(_plan(configurations), configurations, request_id="competing-root")
    monkeypatch.setattr(reviewed, "record_worker_authority", stale)
    with pytest.raises(ValidationError, match="predecessor"):
        reviewed.record_reviewed_worker_authority(
            dsn="unused", transition=competing, **configurations,
        )


def test_uncertain_storage_failure_is_not_reported_as_success(
    configurations: dict[str, Path], monkeypatch: pytest.MonkeyPatch,
) -> None:
    transition = _grant(_plan(configurations), configurations)

    def failed(**kwargs: Any) -> dict[str, Any]:
        raise StorageError("worker authority transaction failed; no success is confirmed")

    monkeypatch.setattr(reviewed, "record_worker_authority", failed)
    with pytest.raises(StorageError, match="no success is confirmed"):
        reviewed.record_reviewed_worker_authority(
            dsn="unused", transition=transition, **configurations,
        )


def _arguments(configurations: dict[str, Path], target: Path) -> list[str]:
    return [
        "--transition", str(target),
        "--worker-config", str(configurations["worker_config_path"]),
        "--delivery-config", str(configurations["delivery_config_path"]),
        "--destination-config", str(configurations["destination_config_path"]),
    ]


def test_cli_default_never_calls_recorder(
    configurations: dict[str, Path], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    transition = _grant(_plan(configurations), configurations)
    target = tmp_path / "transition.json"
    target.write_text(json.dumps(transition), encoding="utf-8")

    def forbidden(**kwargs: Any) -> dict[str, Any]:
        raise AssertionError("validation-only command must not record")

    monkeypatch.setattr(reviewed, "record_worker_authority", forbidden)
    assert reviewed.main(_arguments(configurations, target)) == 0
    assert json.loads(capsys.readouterr().out) == {
        "transition_id": transition["transition_id"], "configuration_validated": True,
        "persisted": False, "runtime_permission_granted": False,
    }


def test_cli_record_requires_explicit_dsn_and_sanitizes_failure(
    configurations: dict[str, Path], tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    transition = _grant(_plan(configurations), configurations)
    target = tmp_path / "transition.json"
    target.write_text(json.dumps(transition), encoding="utf-8")
    monkeypatch.delenv("WAREHOUSE_POSTGRES_DSN", raising=False)
    assert reviewed.main(_arguments(configurations, target) + ["--record"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "WAREHOUSE_POSTGRES_DSN is required" in captured.err


def test_retained_predecessor_cannot_be_invented(configurations: dict[str, Path]) -> None:
    transition = _grant(_plan(configurations), configurations)
    with pytest.raises(ValidationError, match="predecessor"):
        reviewed.prepare_reviewed_worker_authority(
            transition=transition, previous=copy.deepcopy(transition), **configurations,
        )
