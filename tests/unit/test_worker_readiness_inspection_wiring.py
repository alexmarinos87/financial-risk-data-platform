from __future__ import annotations

import json
from typing import Any

import pytest

from src.warehouse import inspect_worker_readiness as command
from src.warehouse import notification_worker_readiness_reader as reader
from test_worker_readiness_reader import Cursor, envelope


@pytest.mark.parametrize("tamper", [False, True])
def test_command_reuses_real_reader_and_semantic_source_validation(tamper: bool, monkeypatch: Any, capsys: Any) -> None:
    value = envelope()
    if tamper:
        value[5][0]["document_sha256"] = "0" * 64
    cursor = Cursor(value)
    class Connection:
        closed = False
        autocommit = True
        def __init__(self) -> None:
            self.info = cursor.connection.info
        def __enter__(self) -> Connection:
            return self
        def __exit__(self, *args: Any) -> None:
            self.closed = True
        def cursor(self) -> Cursor:
            cursor.connection = self
            return cursor
    connection = Connection()
    monkeypatch.setattr(reader, "_connect", lambda dsn: connection)
    monkeypatch.setenv("WAREHOUSE_POSTGRES_DSN", "synthetic")
    # Do not mock command._read, the snapshot builder or semantic validators.
    code = command.main(["--worker-id", "authority-worker", "--read-database"])
    output = capsys.readouterr()
    assert code == (1 if tamper else 0)
    report = json.loads(output.err if tamper else output.out)
    assert report["status"] == ("failed" if tamper else "ready_sources")
    assert connection.closed is True
    assert len(cursor.calls) == 4
    assert cursor.calls[-1] == (reader.READINESS_SOURCE_SQL, ("authority-worker",))
    assert report["runtime_permission_granted"] is False
    assert report["failure_history_verified"] is False
    if not tamper:
        assert [row["execution_kind"] for row in report["readiness"]] == ["initial", "retry"]
        assert "record" not in report and "snapshot" not in report
