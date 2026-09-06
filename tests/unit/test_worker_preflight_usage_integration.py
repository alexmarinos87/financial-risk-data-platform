from __future__ import annotations

from typing import Any

import pytest

from src.orchestration import check_notification_worker_preflight as command
from src.orchestration.notification_worker_cli_parser import USAGE_ERROR


@pytest.mark.parametrize("source", [
    ["--read-current", "--dsn", "synthetic-secret-never-real"],
    ["--read-current=synthetic-secret-never-real"],
    ["--read-current", "synthetic-secret-never-real"],
    ["--read-current", "--snapshot", "synthetic-secret-never-real"],
])
def test_real_entrypoint_redacts_usage_before_any_io(
    source: list[str], monkeypatch: pytest.MonkeyPatch, capsys: Any,
) -> None:
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("unexpected I/O")
        raise AssertionError("invalid usage must not read configuration or PostgreSQL")

    for name in ("read_worker_authority_snapshot", "load_authority_snapshot", "build_reviewed_worker_preflight"):
        monkeypatch.setattr(command, name, forbidden)
    with pytest.raises(SystemExit) as caught:
        command.main([*source, "--worker-id", "worker", "--selected-transition-id", "transition",
                      "--scheduled-for", "2026-09-06T12:00:00+00:00"])
    assert caught.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == USAGE_ERROR
    assert calls == []
