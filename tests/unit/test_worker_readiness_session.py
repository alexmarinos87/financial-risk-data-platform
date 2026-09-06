from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.common.exceptions import StorageError
from src.warehouse.notification_worker_readiness_session import (
    require_fresh_readiness_session,
)


def session_cursor() -> SimpleNamespace:
    return SimpleNamespace(connection=SimpleNamespace(
        closed=False, autocommit=True,
        info=SimpleNamespace(
            transaction_status=SimpleNamespace(name="IDLE"),
            pipeline_status=SimpleNamespace(name="OFF"),
        ),
    ))


def test_idle_autocommit_session_is_accepted_without_io() -> None:
    # Deliberately no execute method: state validation cannot send SQL.
    require_fresh_readiness_session(session_cursor())


@pytest.mark.parametrize("state", ["ACTIVE", "INTRANS", "INERROR", "UNKNOWN"])
def test_nonidle_sessions_are_rejected_before_io(state: str) -> None:
    cursor = session_cursor()
    cursor.connection.info.transaction_status.name = state
    with pytest.raises(StorageError, match="fresh read-only statement"):
        require_fresh_readiness_session(cursor)


@pytest.mark.parametrize("state", ["ON", "ABORTED"])
def test_pipeline_sessions_are_rejected(state: str) -> None:
    cursor = session_cursor()
    cursor.connection.info.pipeline_status.name = state
    with pytest.raises(StorageError):
        require_fresh_readiness_session(cursor)


@pytest.mark.parametrize("value", [False, None, 1, "true"])
def test_autocommit_must_be_explicitly_true(value: object) -> None:
    cursor = session_cursor()
    cursor.connection.autocommit = value
    with pytest.raises(StorageError):
        require_fresh_readiness_session(cursor)


@pytest.mark.parametrize("value", [True, None, 0, "false"])
def test_connection_must_be_explicitly_open(value: object) -> None:
    cursor = session_cursor()
    cursor.connection.closed = value
    with pytest.raises(StorageError):
        require_fresh_readiness_session(cursor)


@pytest.mark.parametrize("cursor", [None, SimpleNamespace(), SimpleNamespace(connection=None)])
def test_missing_session_evidence_is_rejected(cursor: object) -> None:
    with pytest.raises(StorageError):
        require_fresh_readiness_session(cursor)
