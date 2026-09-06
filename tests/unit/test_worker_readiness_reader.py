from __future__ import annotations

import hashlib
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse import notification_worker_readiness_reader as reader
from src.warehouse.notification_worker_readiness_source import source_bytes
from test_worker_readiness_snapshot import OBSERVED, readiness_authority, source_entry


class Cursor:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.calls: list[tuple[str, Any]] = []
    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
    def fetchone(self) -> Any:
        return self.row
    def __enter__(self) -> Cursor:
        return self
    def __exit__(self, *args: Any) -> None:
        pass


def envelope() -> list[Any]:
    document = readiness_authority()
    return [OBSERVED, OBSERVED, "read committed", "on", {
        "document": document, "document_sha256": hashlib.sha256(source_bytes(document)).hexdigest(),
        "authority_sequence": 1, "oversized": False,
    }, [{**source_entry(kind), "oversized": False} for kind in ("initial", "retry")]]


def test_single_parameterized_statement_reopens_current_sources() -> None:
    cursor = Cursor(envelope())
    result = reader.read_worker_readiness_with_cursor(cursor, worker_id="authority-worker")
    assert cursor.calls == [(reader.READINESS_SOURCE_SQL, ("authority-worker",))]
    assert result["status"] == "ready_sources"
    assert result["single_statement_read_only"] is True
    assert result["authority_transition_id"] == readiness_authority()["transition_id"]
    assert result["failure_history_verified"] is False
    assert result["runtime_permission_granted"] is False
    assert len(result["snapshot"]["readiness"]) == 2


def test_unknown_worker_is_not_healthy() -> None:
    result = reader.read_worker_readiness_with_cursor(Cursor([OBSERVED, OBSERVED, "read committed", "on", None, []]), worker_id="unknown")
    assert result["status"] == "authority_missing"
    assert result["snapshot"] is None


@pytest.mark.parametrize("index,value", [(1, OBSERVED - timedelta(microseconds=1)), (2, "repeatable read"), (3, "off")])
def test_old_transaction_or_wrong_session_mode_cannot_supply_current_evidence(index: int, value: Any) -> None:
    row = envelope()
    row[index] = value
    with pytest.raises(StorageError, match="fresh read-only"):
        reader.read_worker_readiness_with_cursor(Cursor(row), worker_id="authority-worker")


@pytest.mark.parametrize("variant", ["oversize_authority", "oversize_record", "three_rows", "wrong_worker", "digest", "bool_sequence", "orphan", "bad_envelope"])
def test_invalid_or_ambiguous_database_sources_fail_closed(variant: str) -> None:
    row: Any = envelope()
    worker_id = "authority-worker"
    if variant == "oversize_authority":
        row[4]["oversized"] = True
    elif variant == "oversize_record":
        row[5][0]["oversized"] = True
    elif variant == "three_rows":
        row[5].append(row[5][0])
    elif variant == "wrong_worker":
        worker_id = "other-worker"
    elif variant == "digest":
        row[4]["document_sha256"] = "0" * 64
    elif variant == "bool_sequence":
        row[4]["authority_sequence"] = True
    elif variant == "orphan":
        row[4] = None
    else:
        row = None
    with pytest.raises(StorageError):
        reader.read_worker_readiness_with_cursor(Cursor(row), worker_id=worker_id)


def test_database_failure_does_not_expose_provider_diagnostics() -> None:
    class FailureCursor(Cursor):
        def execute(self, sql: str, params: Any = None) -> None:
            raise RuntimeError("private-dsn-and-provider-detail")
    with pytest.raises(StorageError, match="source lookup failed") as caught:
        reader.read_worker_readiness_with_cursor(FailureCursor(None), worker_id="worker")
    assert "private-dsn" not in str(caught.value)


def test_owning_reader_sets_only_bounded_session_options_and_closes(monkeypatch: Any) -> None:
    cursor = Cursor(envelope())
    class Connection:
        closed = False
        def __enter__(self) -> Connection:
            return self
        def __exit__(self, *args: Any) -> None:
            self.closed = True
        def cursor(self) -> Cursor:
            return cursor
    connection = Connection()
    monkeypatch.setattr(reader, "_connect", lambda dsn: connection)
    result = reader.read_current_worker_readiness(dsn="not-used", worker_id="authority-worker")
    assert connection.closed is True
    assert [sql for sql, _ in cursor.calls[:3]] == [
        "SET default_transaction_read_only = on", "SET default_transaction_isolation = 'read committed'",
        "SET statement_timeout = '10s'",
    ]
    assert result["runtime_permission_granted"] is False


def test_invalid_selection_fails_before_database_access(monkeypatch: Any) -> None:
    def forbidden(dsn: str) -> Any:
        raise AssertionError("connection was not permitted")
    monkeypatch.setattr(reader, "_connect", forbidden)
    with pytest.raises(ValidationError):
        reader.read_current_worker_readiness(dsn="not-used", worker_id="not valid")


def test_query_and_real_postgres_proof_remain_bounded_and_wired() -> None:
    sql = reader.READINESS_SOURCE_SQL
    assert "LIMIT 1" in sql and "LIMIT 3" in sql
    assert "r.record_id = v.readiness_record_id" in sql
    assert "ORDER BY authority_sequence DESC" in sql
    assert "octet_length(r.record_json::TEXT) <= 1048576" in sql
    assert "pg_advisory" not in sql
    assert "worker_readiness_postgres_contract_check" in Path("Makefile").read_text()
