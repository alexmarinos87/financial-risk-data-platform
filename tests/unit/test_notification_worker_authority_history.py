from __future__ import annotations

import copy
import hashlib
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from src.warehouse import notification_worker_authority_history as history
from test_notification_worker_authority_contract import NOW, grant, stop


class Cursor:
    def __init__(self, rows: list[Any]) -> None:
        self.rows = iter(rows)
        self.calls: list[tuple[str, Any]] = []

    def execute(self, statement: str, params: Any = None) -> None:
        self.calls.append((statement, params))

    def fetchone(self) -> Any:
        return next(self.rows)


def row(document: dict[str, Any], sequence: int = 1) -> tuple[Any, ...]:
    return document, hashlib.sha256(canonical_bytes(document)).hexdigest(), sequence, NOW


def test_cursor_append_serializes_and_reconciles_predecessor() -> None:
    first = grant()
    cursor = Cursor([None, None, row(first)])
    result = history.record_worker_authority_with_cursor(cursor, transition=first)
    assert result['created'] is True
    assert result['authority_sequence'] == 1
    assert result['runtime_permission_granted'] is False
    assert cursor.calls[2][0] == history.LOCK_SQL
    assert cursor.calls[2][1] == ('notification-worker-authority:authority-worker',)
    assert 'request_id = %s' in cursor.calls[3][0]
    assert 'ORDER BY authority_sequence DESC' in cursor.calls[4][0]
    assert cursor.calls[-1][1][-1] == result['document_sha256']
    later = stop(first)
    assert history.record_worker_authority_with_cursor(
        Cursor([None, row(first), row(later, 2)]), transition=later,
    )['authority_sequence'] == 2


def test_exact_old_request_replay_does_not_read_or_promote_current_head() -> None:
    first = grant()
    cursor = Cursor([row(first)])
    assert history.record_worker_authority_with_cursor(cursor, transition=first)['created'] is False
    assert len(cursor.calls) == 4
    assert not any('INSERT' in sql for sql, _ in cursor.calls)


def test_conflicting_request_and_stale_head_fail_before_insert() -> None:
    first = grant()
    changed = grant(operator_id='another-operator')
    cursor = Cursor([row(first)])
    with pytest.raises(ValidationError, match='different worker authority'):
        history.record_worker_authority_with_cursor(cursor, transition=changed)
    later = stop(first)
    cursor = Cursor([None, row(later, 2)])
    with pytest.raises(ValidationError, match='predecessor'):
        history.record_worker_authority_with_cursor(cursor, transition=first)
    assert not any('INSERT' in sql for sql, _ in cursor.calls)


def test_corrupt_retained_document_is_not_trusted() -> None:
    first = grant()
    with pytest.raises(StorageError, match='integrity'):
        history.record_worker_authority_with_cursor(
            Cursor([(first, '0' * 64, 1, NOW)]), transition=first,
        )
    changed = copy.deepcopy(first)
    changed['scheduler_mutated'] = True
    with pytest.raises(StorageError, match='document is invalid'):
        history.record_worker_authority_with_cursor(Cursor([row(changed)]), transition=first)


@pytest.mark.parametrize('instant,state', [
    (NOW, 'inactive'), (NOW + timedelta(seconds=2), 'active'),
    (NOW + timedelta(days=1), 'expired'),
])
def test_current_reader_uses_database_evaluation_time(instant: Any, state: str) -> None:
    first = grant()
    result = history.read_current_worker_authority_with_cursor(
        Cursor([(*row(first), instant)]), worker_id='authority-worker',
    )
    assert result['authority_state'] == state
    assert result['runtime_permission_granted'] is False


def test_unknown_worker_is_inactive() -> None:
    assert history.read_current_worker_authority_with_cursor(
        Cursor([None]), worker_id='unknown-worker',
    )['authority_state'] == 'inactive'


def test_cli_defaults_to_validation_without_database_access(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    path = tmp_path / 'authority.json'
    path.write_bytes(canonical_bytes(grant()))
    def unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError('validation must not connect')
    monkeypatch.setattr(history, '_connect', unexpected)
    assert history.main(['--transition', str(path)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['persisted'] is False
    assert result['runtime_permission_granted'] is False


@pytest.mark.parametrize('content', [b'{"action":"activate","action":"disable"}', b'[]', b'{', b'NaN'])
def test_input_rejects_invalid_json_and_duplicate_keys(tmp_path: Path, content: bytes) -> None:
    path = tmp_path / 'input.json'
    path.write_bytes(content)
    with pytest.raises(ValidationError):
        history.load_worker_authority(path)


def test_input_rejects_symlinks_large_files_and_fifos(tmp_path: Path) -> None:
    original = tmp_path / 'original.json'
    original.write_bytes(canonical_bytes(grant()))
    link = tmp_path / 'link.json'
    link.symlink_to(original)
    with pytest.raises(ValidationError, match='symbolic link'):
        history.load_worker_authority(link)
    original.write_bytes(b' ' * (history.MAX_DOCUMENT_BYTES + 1))
    with pytest.raises(ValidationError, match='exceeds'):
        history.load_worker_authority(original)
    fifo = tmp_path / 'fifo'
    if hasattr(os, 'mkfifo'):
        os.mkfifo(fifo)
        with pytest.raises(ValidationError, match='regular file'):
            history.load_worker_authority(fifo)


def test_driver_failure_is_sanitized_and_invalid_evidence_never_connects(monkeypatch: Any) -> None:
    def broken(*args: Any, **kwargs: Any) -> Any:
        raise RuntimeError('SECRET_DSN_AND_PAYLOAD')
    monkeypatch.setattr(history, '_connect', broken)
    with pytest.raises(StorageError) as exc:
        history.record_worker_authority(dsn='secret', transition=grant())
    assert 'SECRET' not in str(exc.value)
    changed = grant()
    changed['scheduler_mutated'] = True
    with pytest.raises(ValidationError):
        history.record_worker_authority(dsn='secret', transition=changed)


def test_connection_context_rolls_back_on_write_failure(monkeypatch: Any) -> None:
    entered = []
    class BrokenCursor(Cursor):
        def __enter__(self) -> Any:
            return self
        def __exit__(self, *args: Any) -> None:
            pass
        def execute(self, statement: str, params: Any = None) -> None:
            if 'INSERT' in statement:
                raise RuntimeError('simulated write failure')
            super().execute(statement, params)
    class Connection:
        def __enter__(self) -> Any:
            return self
        def __exit__(self, exc_type: Any, *args: Any) -> None:
            entered.append(exc_type)
        def cursor(self) -> Any:
            return BrokenCursor([None, None])
    monkeypatch.setattr(history, '_connect', lambda dsn: Connection())
    with pytest.raises(StorageError):
        history.record_worker_authority(dsn='local', transition=grant())
    assert entered == [RuntimeError]
