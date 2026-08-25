from __future__ import annotations

from typing import Any

import pytest

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY,
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
    acquire_notification_delivery_lock,
)


class _Cursor:
    def __init__(self, responses: list[tuple[bool]]) -> None:
        self.responses = responses
        self.statements: list[tuple[str, list[int]]] = []

    def __enter__(self) -> _Cursor:
        return self

    def __exit__(self, *_: Any) -> None:
        return None

    def execute(self, statement: str, parameters: list[int]) -> None:
        self.statements.append((statement, parameters))

    def fetchone(self) -> tuple[bool]:
        return self.responses.pop(0)


class _Connection:
    def __init__(self, responses: list[tuple[bool]]) -> None:
        self.cursor_instance = _Cursor(responses)
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_instance

    def close(self) -> None:
        self.closed = True


def test_delivery_lock_identity_is_stable_and_postgres_safe() -> None:
    assert -(2**63) <= LOCK_KEY < 2**63
    assert len(LOCK_KEY_FINGERPRINT) == 24
    assert LOCK_MODEL_VERSION == "portfolio-risk-notification-delivery-lock-v1"
    assert LOCK_SCOPE == "portfolio-risk-notification-delivery"


def test_delivery_lock_is_acquired_and_released_on_one_session() -> None:
    connection = _Connection([(True,), (True,)])

    with acquire_notification_delivery_lock(
        dsn="postgresql://secret-value",
        connection_factory=lambda _: connection,
    ) as evidence:
        assert connection.closed is False
        assert evidence == {
            "model_version": LOCK_MODEL_VERSION,
            "scope": LOCK_SCOPE,
            "key_fingerprint": LOCK_KEY_FINGERPRINT,
            "acquired": True,
        }

    assert connection.closed is True
    assert connection.cursor_instance.statements == [
        ("SELECT pg_try_advisory_lock(%s)", [LOCK_KEY]),
        ("SELECT pg_advisory_unlock(%s)", [LOCK_KEY]),
    ]


def test_held_delivery_lock_fails_closed_without_waiting() -> None:
    connection = _Connection([(False,)])

    with pytest.raises(OverlapError, match="already holds"):
        with acquire_notification_delivery_lock(
            dsn="postgresql://secret-value",
            connection_factory=lambda _: connection,
        ):
            raise AssertionError("lock body must not run")

    assert connection.closed is True
    assert connection.cursor_instance.statements == [
        ("SELECT pg_try_advisory_lock(%s)", [LOCK_KEY])
    ]


def test_release_failure_is_reported_after_successful_body() -> None:
    connection = _Connection([(True,), (False,)])

    with pytest.raises(StorageError, match="release"):
        with acquire_notification_delivery_lock(
            dsn="postgresql://secret-value",
            connection_factory=lambda _: connection,
        ):
            pass

    assert connection.closed is True


def test_body_failure_is_preserved_while_connection_close_releases_lock() -> None:
    connection = _Connection([(True,), (False,)])

    with pytest.raises(RuntimeError, match="body failed"):
        with acquire_notification_delivery_lock(
            dsn="postgresql://secret-value",
            connection_factory=lambda _: connection,
        ):
            raise RuntimeError("body failed")

    assert connection.closed is True


def test_invalid_dsn_fails_before_connection_creation() -> None:
    calls: list[str] = []

    with pytest.raises(ValidationError, match="DSN"):
        with acquire_notification_delivery_lock(
            dsn="",
            connection_factory=lambda dsn: calls.append(dsn),
        ):
            pass

    assert calls == []
