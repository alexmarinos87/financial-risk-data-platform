from __future__ import annotations

import copy
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse import notification_worker_attempt_attribution_history as history
from test_worker_attempt_attribution_contract import record


class Connection:
    autocommit = False
    closed = False
    savepoint_rolled_back = False
    fail_commit = False

    def __init__(self, rows: list[Any]) -> None:
        self.handle = Cursor(rows, self)

    @contextmanager
    def transaction(self):
        try:
            yield
        except BaseException:
            self.savepoint_rolled_back = True
            raise

    def __enter__(self) -> Connection:
        return self

    def __exit__(self, exc_type: Any, *args: Any) -> None:
        self.closed = True
        if exc_type is None and self.fail_commit:
            raise RuntimeError("private-commit-diagnostic")

    def cursor(self) -> Cursor:
        return self.handle


class Cursor:
    def __init__(self, rows: list[Any], connection: Connection) -> None:
        self.rows = iter(rows)
        self.connection = connection
        self.calls: list[tuple[str, Any]] = []
        self.fail_attribution: BaseException | None = None

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
        if sql.startswith("INSERT INTO " + history.ATTRIBUTION_TABLE) and self.fail_attribution:
            raise self.fail_attribution

    def fetchone(self) -> Any:
        return next(self.rows)

    def fetchall(self) -> Any:
        return next(self.rows)

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def encoded(value: Any) -> tuple[Any, str]:
    return value, history._sha(value)


def source(value: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(value["attempt"][key] for key in history.ATTEMPT_COLUMNS)


def rows(value: dict[str, Any], *, replay: bool = False) -> list[Any]:
    now = datetime.fromisoformat(value["attempt"]["attempted_at"]) + timedelta(seconds=1)
    initial: list[Any] = [("read committed",), (*encoded(value["authority"]), now)]
    if replay:
        return [*initial, encoded(value), [encoded(value["context"])], source(value)]
    return [*initial, None, [], encoded(value["context"]), (0,), source(value), encoded(value)]


def test_new_operation_writes_exact_context_source_and_record_in_order() -> None:
    value = record()
    connection = Connection(rows(value))
    result = history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    inserts = [(sql, params) for sql, params in connection.handle.calls if sql.startswith("INSERT")]
    assert len(inserts) == 3
    assert history.CONTEXT_TABLE in inserts[0][0]
    assert history.ATTEMPT_TABLE in inserts[1][0]
    assert history.ATTRIBUTION_TABLE in inserts[2][0]
    assert inserts[1][1] == source(value)
    assert result["created"] is True and result["commit_acknowledged"] is False
    assert result["failure_history_complete"] is result["runtime_permission_granted"] is False
    assert connection.savepoint_rolled_back is False
    assert (history.authority_history.LOCK_SQL, ("notification-worker-authority:authority-worker",)) in connection.handle.calls


def test_exact_historical_replay_reopens_all_sources_without_insert_or_head_lookup() -> None:
    value = record()
    connection = Connection(rows(value, replay=True))
    result = history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert result["created"] is False
    assert result["source_and_attribution_reconciled"] is True
    assert not any("INSERT" in sql or "authority_sequence" in sql for sql, _ in connection.handle.calls)


@pytest.mark.parametrize("index", [1, 2, 3, 4])
def test_replay_rejects_missing_or_changed_authority_record_context_and_source(index: int) -> None:
    value = record()
    responses = rows(value, replay=True)
    if index == 1:
        responses[index] = None
    elif index == 2:
        responses[index] = (value, "0" * 64)
    elif index == 3:
        responses[index] = []
    else:
        changed = list(source(value))
        changed[-1] = "b" * 64
        responses[index] = tuple(changed)
    connection = Connection(responses)
    with pytest.raises(StorageError):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert connection.savepoint_rolled_back is True


def test_existing_unattributed_source_cannot_be_silently_backfilled() -> None:
    value = record()
    responses = rows(value)
    responses[6] = None
    connection = Connection(responses)
    with pytest.raises(ValidationError, match="cannot be backfilled"):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert connection.savepoint_rolled_back is True
    assert not any(sql.startswith("INSERT INTO " + history.ATTRIBUTION_TABLE) for sql, _ in connection.handle.calls)


@pytest.mark.parametrize("variant", ["different", "ambiguous", "racing_insert"])
def test_scope_request_and_context_conflicts_are_not_overwritten(variant: str) -> None:
    value = record()
    responses = rows(value)
    changed = copy.deepcopy(value["context"])
    changed["request_id"] = "OTHER"
    if variant == "different":
        responses[3] = [encoded(changed)]
    elif variant == "ambiguous":
        responses[3] = [encoded(changed), encoded(value["context"])]
    else:
        responses[4] = None
    connection = Connection(responses)
    with pytest.raises((ValidationError, StorageError)):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert connection.savepoint_rolled_back is True


@pytest.mark.parametrize("count", [25, 26, -1, True, None])
def test_event_capacity_or_invalid_count_prevents_source_write(count: Any) -> None:
    value = record()
    responses = rows(value)
    responses[5] = (count,)
    connection = Connection(responses)
    with pytest.raises(ValidationError, match="event limit"):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert not any(sql.startswith("INSERT INTO " + history.ATTEMPT_TABLE) for sql, _ in connection.handle.calls)


@pytest.mark.parametrize("autocommit", [True, None, 0])
def test_autocommit_is_rejected_before_any_query(autocommit: Any) -> None:
    connection = Connection([])
    connection.autocommit = autocommit
    with pytest.raises(ValidationError, match="caller-owned"):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=record())
    assert connection.handle.calls == []


def test_wrong_isolation_and_future_attempt_are_rejected() -> None:
    value = record()
    with pytest.raises(ValidationError, match="READ COMMITTED"):
        history.record_worker_attempt_attribution_with_cursor(Connection([("repeatable read",)]).handle, record=value)
    responses = rows(value)
    responses[1] = (*encoded(value["authority"]), datetime.fromisoformat(value["attempt"]["attempted_at"]) - timedelta(microseconds=1))
    with pytest.raises(ValidationError, match="future-dated"):
        history.record_worker_attempt_attribution_with_cursor(Connection(responses).handle, record=value)


@pytest.mark.parametrize("interrupt", [False, True])
def test_final_write_failure_rolls_back_operation_before_propagating(interrupt: bool) -> None:
    value = record()
    connection = Connection(rows(value))
    connection.handle.fail_attribution = KeyboardInterrupt() if interrupt else RuntimeError("private-provider-detail")
    with pytest.raises(KeyboardInterrupt if interrupt else RuntimeError):
        history.record_worker_attempt_attribution_with_cursor(connection.handle, record=value)
    assert connection.savepoint_rolled_back is True
    assert len([sql for sql, _ in connection.handle.calls if sql.startswith("INSERT")]) == 3


@pytest.mark.parametrize("fail_commit", [False, True])
def test_public_api_acknowledges_only_after_context_commit(fail_commit: bool, monkeypatch: Any) -> None:
    value = record()
    connection = Connection(rows(value))
    connection.fail_commit = fail_commit
    monkeypatch.setattr(history.authority_history, "_connect", lambda dsn: connection)
    if fail_commit:
        with pytest.raises(StorageError, match="unconfirmed") as caught:
            history.record_worker_attempt_attribution(dsn="unused", record=value)
        assert "private-commit-diagnostic" not in str(caught.value)
    else:
        result = history.record_worker_attempt_attribution(dsn="unused", record=value)
        assert result["commit_acknowledged"] is True
    assert connection.closed is True


def test_public_database_failure_is_sanitized_and_invalid_record_never_connects(monkeypatch: Any) -> None:
    connection = Connection(rows(record()))
    connection.handle.fail_attribution = RuntimeError("private-provider-detail")
    calls = []
    def connect(dsn: str) -> Connection:
        calls.append(True)
        return connection
    monkeypatch.setattr(history.authority_history, "_connect", connect)
    with pytest.raises(ValidationError):
        history.record_worker_attempt_attribution(dsn="unused", record={})
    assert calls == []
    with pytest.raises(StorageError, match="unconfirmed") as caught:
        history.record_worker_attempt_attribution(dsn="unused", record=record())
    assert "private-provider-detail" not in str(caught.value)
    assert connection.savepoint_rolled_back is True


def test_schema_is_append_only_and_preserves_exact_source_join() -> None:
    sql = Path("sql/notification_worker_attempt_attribution_schema.sql").read_text()
    assert "UNIQUE (context_id, event_id)" in sql
    assert "scope_id TEXT NOT NULL UNIQUE" in sql
    assert "request_id TEXT NOT NULL UNIQUE" in sql
    assert "BEFORE UPDATE OR DELETE" in sql and "BEFORE TRUNCATE" in sql
    assert "source.attempted_at IS DISTINCT FROM" in sql
    assert "existing_count >= selected.max_events" in sql


def test_real_postgres_proof_is_wired_once_after_original_mutation_checks() -> None:
    fixture = Path("src/warehouse/notification_worker_authority_postgres_contract_check.py").read_text()
    call = "results.update(check_worker_attempt_attribution_contract(connection, cursor, first))"
    assert fixture.count(call) == 1
    assert fixture.index('results["update_delete_truncate_rejected"] = True') < fixture.index(call)
    proof = Path("src/warehouse/worker_attempt_attribution_postgres_contract_check.py").read_text()
    assert "connection.transaction(force_rollback=True)" in proof
    assert "set(checks) != EXPECTED_CHECKS" in proof
