from __future__ import annotations

import copy
from datetime import timedelta
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.warehouse import notification_worker_readiness_sources_reader as reader
from src.warehouse.notification_worker_readiness_sources import validate_worker_readiness_sources
from test_worker_readiness_sources import BASE_TIME, source_fixture


class Cursor:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, Any]] = []
        self.fetch_sizes: list[int] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))

    def fetchmany(self, size: int) -> list[Any]:
        self.fetch_sizes.append(size)
        return self.rows

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


def database_rows(sources: list[dict[str, Any]]) -> list[list[Any]]:
    return [[BASE_TIME + timedelta(seconds=5), source["execution_kind"], source["destination_id"],
             source["readiness_record_id"], source["readiness_review_status"], source["execution_ready"],
             source["decision_matches_current_evidence"], source["record_json"], source["document_sha256"],
             len(canonical_bytes(source["record_json"]))] for source in sources]


@pytest.mark.parametrize("initial_only", [False, True])
def test_one_bounded_source_statement_binds_only_selected_kinds(initial_only: bool) -> None:
    plan, sources = source_fixture(initial_only=initial_only)
    cursor = Cursor(database_rows(sources))
    result = reader.read_worker_readiness_sources_with_cursor(cursor, plan=plan)
    assert result["all_sources_allowed"] is True
    assert result == validate_worker_readiness_sources(result)
    kinds = [row["execution_kind"] for row in sources]
    assert cursor.calls == [("SET LOCAL statement_timeout = '10s'", None),
                            (reader.READINESS_SOURCES_SQL,
                             (kinds, reader.MAX_SOURCE_BYTES, plan["destination"]["destination_id"]))]
    assert cursor.fetch_sizes == [3]
    assert "LIMIT 3" in reader.READINESS_SOURCES_SQL
    assert "statement_timestamp()" in reader.READINESS_SOURCES_SQL
    assert "CASE WHEN octet_length" in reader.READINESS_SOURCES_SQL


def test_missing_view_rows_have_clocked_missing_outcomes() -> None:
    plan, _ = source_fixture()
    rows = [[BASE_TIME, kind, *([None] * 8)] for kind in ("initial", "retry")]
    result = reader.read_worker_readiness_sources_with_cursor(Cursor(rows), plan=plan)
    assert result["observed_at"] == BASE_TIME.isoformat()
    assert [row["status"] for row in result["readiness"]] == ["missing", "missing"]
    assert result["all_sources_allowed"] is False


@pytest.mark.parametrize("variant", ["empty", "duplicate", "too_many", "bad_clock", "mixed_clock",
                                      "missing_join", "oversize", "bool_size", "digest", "shape"])
def test_invalid_database_results_fail_closed(variant: str) -> None:
    plan, sources = source_fixture()
    rows = database_rows(sources)
    if variant == "empty":
        rows = []
    elif variant == "duplicate":
        rows[1] = copy.deepcopy(rows[0])
    elif variant == "too_many":
        rows.append(copy.deepcopy(rows[0]))
    elif variant == "bad_clock":
        rows[0][0] = BASE_TIME.isoformat()
    elif variant == "mixed_clock":
        rows[1][0] += timedelta(seconds=1)
    elif variant == "missing_join":
        rows[0][7] = None
    elif variant == "oversize":
        rows[0][7], rows[0][9] = None, reader.MAX_SOURCE_BYTES + 1
    elif variant == "bool_size":
        rows[0][9] = True
    elif variant == "digest":
        rows[0][8] = "0" * 64
    else:
        rows[0].pop()
    with pytest.raises(StorageError, match="unable to capture verified"):
        reader.read_worker_readiness_sources_with_cursor(Cursor(rows), plan=plan)


class Connection:
    def __init__(self, cursor: Cursor, *, exit_failure: bool = False) -> None:
        self.selected_cursor = cursor
        self.exit_failure = exit_failure
        self.exited = False

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, *args: Any) -> None:
        self.exited = True
        if self.exit_failure:
            raise RuntimeError("private-provider-details")

    def cursor(self) -> Cursor:
        return self.selected_cursor


def test_public_reader_uses_read_only_transaction_and_waits_for_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    plan, sources = source_fixture()
    cursor = Cursor(database_rows(sources))
    connection = Connection(cursor)
    calls = []

    def connect(dsn: str) -> Connection:
        calls.append(dsn)
        return connection

    monkeypatch.setattr(reader, "_connect", connect)
    result = reader.read_worker_readiness_sources(dsn="injected-dsn", plan=plan)
    assert connection.exited is True
    assert calls == ["injected-dsn"]
    assert cursor.calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"
    assert cursor.calls[1][0] == "SET LOCAL lock_timeout = '5s'"
    assert result["runtime_permission_granted"] is False


def test_context_exit_failure_is_redacted_not_success(monkeypatch: pytest.MonkeyPatch) -> None:
    plan, sources = source_fixture()
    connection = Connection(Cursor(database_rows(sources)), exit_failure=True)
    monkeypatch.setattr(reader, "_connect", lambda dsn: connection)
    with pytest.raises(StorageError) as error:
        reader.read_worker_readiness_sources(dsn="secret-dsn", plan=plan)
    assert str(error.value) == "unable to capture verified worker readiness sources"


@pytest.mark.parametrize("variant", ["plan", "empty_dsn", "invalid_dsn"])
def test_bad_request_never_opens_database(variant: str, monkeypatch: pytest.MonkeyPatch) -> None:
    plan, _ = source_fixture()
    dsn: Any = "unused"
    if variant == "plan":
        plan["status"] = "not-a-status"
    else:
        dsn = "" if variant == "empty_dsn" else None

    def forbidden(*args: Any) -> Any:
        raise AssertionError("connection should not be attempted")

    monkeypatch.setattr(reader, "_connect", forbidden)
    with pytest.raises(ValidationError):
        reader.read_worker_readiness_sources(dsn=dsn, plan=plan)


def test_provider_error_is_redacted_at_cursor_boundary() -> None:
    plan, _ = source_fixture()

    class FailedCursor(Cursor):
        def execute(self, sql: str, params: Any = None) -> None:
            raise RuntimeError("credential-bearing-provider-details")

    with pytest.raises(StorageError) as error:
        reader.read_worker_readiness_sources_with_cursor(FailedCursor([]), plan=plan)
    assert "credential" not in str(error.value)
