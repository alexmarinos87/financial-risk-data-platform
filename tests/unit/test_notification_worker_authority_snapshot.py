from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.warehouse import notification_worker_authority_snapshot as snapshots
from test_notification_worker_authority_contract import NOW, grant, stop


class SnapshotCursor:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))

    def fetchone(self) -> Any:
        return self.row

    def __enter__(self) -> SnapshotCursor:
        return self

    def __exit__(self, *args: Any) -> None:
        return None


def snapshot_row(document: dict[str, Any] | None, *, observed: datetime | None = None) -> list[Any]:
    instant = NOW + timedelta(seconds=5) if observed is None else observed
    if document is None:
        return [instant, None, None, None, None]
    return [instant, document, hashlib.sha256(canonical_bytes(document)).hexdigest(),
            1, datetime.fromisoformat(document["effective_at"])]


def read(document: dict[str, Any] | None, **kwargs: Any) -> dict[str, Any]:
    return snapshots.read_worker_authority_snapshot_with_cursor(
        SnapshotCursor(snapshot_row(document, **kwargs)), worker_id="authority-worker",
    )


def rehash_snapshot(value: dict[str, Any]) -> dict[str, Any]:
    identity = {key: item for key, item in value.items() if key != "snapshot_id"}
    value["snapshot_id"] = f"{snapshots.MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"
    return value


def test_snapshot_uses_one_parameterized_head_and_clock_select() -> None:
    cursor = SnapshotCursor(snapshot_row(grant()))
    result = snapshots.read_worker_authority_snapshot_with_cursor(cursor, worker_id="authority-worker")
    assert result["authority_state"] == "active"
    assert result["runtime_permission_granted"] is False
    assert result["observed_at"] == (NOW + timedelta(seconds=5)).isoformat()
    assert cursor.calls == [
        ("SET LOCAL statement_timeout = '10s'", None),
        (snapshots.CURRENT_SNAPSHOT_SQL, ("authority-worker",)),
    ]
    sql = snapshots.CURRENT_SNAPSHOT_SQL
    assert "statement_timestamp()" in sql and "LEFT JOIN LATERAL" in sql
    assert "ORDER BY authority_sequence DESC LIMIT 1" in sql
    assert not any(word in sql.upper() for word in ("INSERT", "UPDATE", "DELETE", "PG_ADVISORY"))


def test_unknown_worker_still_has_clock_and_no_invented_head() -> None:
    result = read(None)
    assert result["observed_at"]
    assert result["authority_state"] == "inactive"
    assert result["transition"] is None
    assert result["document_sha256"] is None
    assert result["authority_sequence"] is None
    assert result["recorded_at"] is None
    assert snapshots.validate_worker_authority_snapshot(result) == result


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_stop_head_remains_stopped_even_after_cooldown(action: str) -> None:
    current = stop(grant(), action=action)
    result = read(current, observed=NOW + timedelta(days=1))
    assert result["authority_state"] == current["to_state"]
    assert result["transition"] == current


def test_expiry_is_exclusive_and_snapshots_are_detached() -> None:
    document = grant()
    expiry = datetime.fromisoformat(document["expires_at"])
    assert read(document, observed=expiry)["authority_state"] == "expired"
    result = read(document, observed=expiry - timedelta(microseconds=1))
    assert result["authority_state"] == "active"
    rebuilt = snapshots.validate_worker_authority_snapshot(result)
    result["transition"]["reason_codes"].append("operator_request")
    document["plan"]["execution"]["work_items"].clear()
    assert rebuilt["transition"]["reason_codes"] == []
    assert rebuilt["transition"]["plan"]["execution"]["work_items"]


@pytest.mark.parametrize(("field", "value"), [
    ("worker_id", "wrong-worker"), ("authority_state", "disabled"),
    ("authority_sequence", True), ("authority_sequence", 0),
    ("authority_sequence", 2**63), ("document_sha256", "0" * 64),
    ("recorded_at", "2026-09-05T19:00:00+00:00"),
    ("observed_at", "2026-09-05T19:00:00+00:00"),
    ("read_consistency", "repeatable_read"), ("runtime_permission_granted", True),
    ("runtime_permission_granted", 0), ("model_version", "other"),
])
def test_rehashed_contradictory_metadata_is_rejected(field: str, value: Any) -> None:
    result = read(grant())
    result[field] = value
    with pytest.raises(ValidationError):
        snapshots.validate_worker_authority_snapshot(rehash_snapshot(result))


@pytest.mark.parametrize("field", ["authority_sequence", "recorded_at", "document_sha256"])
def test_missing_head_cannot_carry_metadata(field: str) -> None:
    result = read(None)
    result[field] = 1
    with pytest.raises(ValidationError):
        snapshots.validate_worker_authority_snapshot(rehash_snapshot(result))


@pytest.mark.parametrize("row", [None, (), [NOW] * 4, [NOW] * 6,
    [NOW, None, None, 1, None], ["2026-09-05T20:00:00+00:00", None, None, None, None],
    [NOW.replace(tzinfo=None), None, None, None, None],
])
def test_invalid_database_rows_fail_closed(row: Any) -> None:
    with pytest.raises(StorageError, match="failed validation"):
        snapshots.read_worker_authority_snapshot_with_cursor(SnapshotCursor(row), worker_id="authority-worker")


def test_valid_timezone_is_normalized_at_the_database_boundary() -> None:
    observed = NOW.astimezone(timezone(timedelta(hours=1)))
    assert read(None, observed=observed)["observed_at"] == NOW.isoformat()


def test_invalid_worker_is_rejected_before_cursor_use() -> None:
    cursor = SnapshotCursor(None)
    with pytest.raises(ValidationError):
        snapshots.read_worker_authority_snapshot_with_cursor(cursor, worker_id="not a safe worker")
    assert not cursor.calls


def test_snapshot_size_and_extra_fields_are_bounded() -> None:
    result = read(None)
    result["worker_id"] = "x" * snapshots.MAX_SNAPSHOT_BYTES
    with pytest.raises(ValidationError):
        snapshots.validate_worker_authority_snapshot(result)
    result = read(None)
    result["extra"] = "not permitted"
    with pytest.raises(ValidationError):
        snapshots.validate_worker_authority_snapshot(result)


def test_public_adapter_enforces_read_only_before_select(monkeypatch: pytest.MonkeyPatch) -> None:
    cursor = SnapshotCursor(snapshot_row(grant()))

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: Any) -> None:
            return None

        def cursor(self) -> SnapshotCursor:
            return cursor

    monkeypatch.setattr(snapshots, "_connect", lambda dsn: Connection())
    assert snapshots.read_worker_authority_snapshot(dsn="injected", worker_id="authority-worker")["authority_state"] == "active"
    assert cursor.calls[0] == ("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY", None)
    assert cursor.calls[1] == ("SET LOCAL lock_timeout = '5s'", None)


def test_connection_errors_do_not_expose_provider_diagnostics(monkeypatch: pytest.MonkeyPatch) -> None:
    def failed(dsn: str) -> Any:
        raise RuntimeError("sensitive provider diagnostic " + dsn)

    monkeypatch.setattr(snapshots, "_connect", failed)
    with pytest.raises(StorageError) as caught:
        snapshots.read_worker_authority_snapshot(dsn="secret-dsn", worker_id="authority-worker")
    assert str(caught.value) == "unable to read a verified worker authority snapshot"
    with pytest.raises(ValidationError):
        snapshots.read_worker_authority_snapshot(dsn="", worker_id="authority-worker")


def test_replay_preserves_input_bytes() -> None:
    result = read(grant())
    before = copy.deepcopy(result)
    assert snapshots.validate_worker_authority_snapshot(result) == before
    assert result == before
