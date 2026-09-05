from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.orchestration.notification_worker_suspension_transition import build_worker_suspension_bundle
from src.warehouse import notification_worker_suspension_history as history
from test_notification_worker_authority_contract import grant
from test_notification_worker_suspension import AT, decision
from test_notification_worker_suspension_transition import bundle


class Cursor:
    def __init__(self, rows: list[Any], *, fail_second_write: bool = False) -> None:
        self.rows = iter(rows)
        self.calls: list[tuple[str, Any]] = []
        self.fail_second_write = fail_second_write

    def execute(self, statement: str, params: Any = None) -> None:
        self.calls.append((statement, params))
        if self.fail_second_write and statement.startswith("INSERT INTO " + history.TABLE):
            raise RuntimeError("private-database-diagnostic")

    def fetchone(self) -> Any:
        return next(self.rows)

    def __enter__(self) -> Cursor:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


def source_row(document: dict[str, Any], sequence: int = 1) -> tuple[Any, ...]:
    return document, hashlib.sha256(canonical_bytes(document)).hexdigest(), sequence, AT


def evidence_row(value: dict[str, Any]) -> tuple[Any, ...]:
    return value, hashlib.sha256(canonical_bytes(value)).hexdigest()


def new_rows(value: dict[str, Any], now: datetime = AT) -> list[Any]:
    return [None, None, (*source_row(value["authority"]), now), (now,), None,
            source_row(value["authority"]), source_row(value["transition"], 2), evidence_row(value)]


def test_atomic_append_uses_exact_worker_lock_and_retains_complete_bundle() -> None:
    value = bundle()
    cursor = Cursor(new_rows(value))
    result = history.record_worker_suspension_with_cursor(cursor, bundle=value)
    assert result["created"] is True
    assert result["authority_sequence"] == 2
    assert result["suspension_evidence_recorded"] is True
    assert result["runtime_permission_granted"] is False
    assert cursor.calls[2] == (history.authority_history.LOCK_SQL, ("notification-worker-authority:authority-worker",))
    inserts = [(sql, params) for sql, params in cursor.calls if sql.startswith("INSERT") or "\n        INSERT" in sql]
    assert len(inserts) == 2
    assert "notification_worker_authority_history" in inserts[0][0]
    assert history.TABLE in inserts[1][0]
    assert inserts[1][1][-1] == hashlib.sha256(canonical_bytes(value)).hexdigest()


def test_historical_replay_reopens_both_sources_without_head_read_or_write() -> None:
    value = bundle()
    cursor = Cursor([evidence_row(value), source_row(value["authority"])[:2],
                     source_row(value["transition"], 2)[:2], source_row(value["transition"], 2)])
    result = history.record_worker_suspension_with_cursor(cursor, bundle=value)
    assert result["created"] is False
    assert result["authority_sequence"] == 2
    assert not any("INSERT" in sql or "ORDER BY" in sql or "clock_timestamp" in sql for sql, _ in cursor.calls)


def test_changed_operator_cannot_reuse_decision_id() -> None:
    first = bundle()
    changed = build_worker_suspension_bundle(authority=grant(), decision=decision(), operator_id="other")
    with pytest.raises(ValidationError, match="different suspension evidence"):
        history.record_worker_suspension_with_cursor(Cursor([evidence_row(first)]), bundle=changed)


def test_legacy_stop_is_not_silently_backfilled() -> None:
    value = bundle()
    cursor = Cursor([None, (value["transition"]["transition_id"],)])
    with pytest.raises(ValidationError, match="cannot be backfilled"):
        history.record_worker_suspension_with_cursor(cursor, bundle=value)
    assert not any("INSERT" in sql for sql, _ in cursor.calls)


@pytest.mark.parametrize("missing", [False, True])
def test_stale_or_missing_current_authority_is_rejected(missing: bool) -> None:
    value = bundle()
    row = None if missing else (*source_row(value["transition"], 2), AT)
    with pytest.raises(ValidationError, match="exact current head"):
        history.record_worker_suspension_with_cursor(Cursor([None, None, row]), bundle=value)


@pytest.mark.parametrize("offset", [-0.000001, 300.000001])
def test_database_clock_rejects_future_or_stale_decisions(offset: float) -> None:
    value = bundle()
    with pytest.raises(ValidationError, match="stale or future"):
        history.record_worker_suspension_with_cursor(Cursor(new_rows(value, AT + timedelta(seconds=offset))), bundle=value)


def test_new_record_rejects_authority_that_expired_after_evaluation() -> None:
    prior = grant()
    expiry = datetime.fromisoformat(prior["expires_at"])
    evaluated = decision(authority=prior, evaluated_at=expiry - timedelta(seconds=1))
    value = build_worker_suspension_bundle(authority=prior, decision=evaluated, operator_id="observer")
    with pytest.raises(ValidationError, match="no longer active"):
        history.record_worker_suspension_with_cursor(Cursor(new_rows(value, expiry)), bundle=value)


@pytest.mark.parametrize("index", [1, 2])
def test_replay_rejects_missing_or_changed_retained_sources(index: int) -> None:
    value = bundle()
    rows: list[Any] = [evidence_row(value), source_row(value["authority"])[:2],
                       source_row(value["transition"], 2)[:2]]
    rows[index] = None
    with pytest.raises(StorageError, match="source is missing"):
        history.record_worker_suspension_with_cursor(Cursor(rows), bundle=value)


def test_retained_bundle_digest_is_independently_checked() -> None:
    with pytest.raises(StorageError, match="digest differs"):
        history.record_worker_suspension_with_cursor(Cursor([(bundle(), "0" * 64)]), bundle=bundle())


def test_second_write_failure_requires_rollback_and_diagnostics_are_sanitized(monkeypatch: Any) -> None:
    value = bundle()
    cursor = Cursor(new_rows(value), fail_second_write=True)

    class Connection:
        rolled_back = False
        def __enter__(self) -> Connection:
            return self
        def __exit__(self, exc_type: Any, *args: Any) -> None:
            self.rolled_back = exc_type is not None
        def cursor(self) -> Cursor:
            return cursor

    connection = Connection()
    monkeypatch.setattr(history.authority_history, "_connect", lambda dsn: connection)
    with pytest.raises(StorageError, match="no success is confirmed") as caught:
        history.record_worker_suspension(dsn="not-used", bundle=value)
    assert connection.rolled_back is True
    assert "private-database-diagnostic" not in str(caught.value)
    assert any("INSERT INTO risk_platform.notification_worker_authority_history" in sql for sql, _ in cursor.calls)


def test_malformed_bundle_is_rejected_before_connect(monkeypatch: Any) -> None:
    def forbidden(dsn: str) -> Any:
        raise AssertionError("must not connect")
    monkeypatch.setattr(history.authority_history, "_connect", forbidden)
    with pytest.raises(ValidationError):
        history.record_worker_suspension(dsn="not-used", bundle={})


def test_database_fixture_and_schema_are_wired_into_existing_validation() -> None:
    schema = Path("sql/notification_worker_suspension_schema.sql").read_text()
    assert "BEFORE UPDATE OR DELETE" in schema
    assert "BEFORE TRUNCATE" in schema
    assert "FOREIGN KEY (transition_id, worker_id, destination_id)" in schema
    assert "head.transition_id" in schema
    assert "29_notification_worker_suspension_schema.sql:ro" in Path("docker-compose.yml").read_text()
    fixture = Path("src/warehouse/notification_worker_authority_postgres_contract_check.py").read_text()
    assert "check_worker_suspension_contract(connection, cursor, active)" in fixture
    assert "notification_worker_authority_postgres_contract_check" in Path("Makefile").read_text()
