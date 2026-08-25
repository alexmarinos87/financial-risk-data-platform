from __future__ import annotations

import hashlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError

LOCK_MODEL_VERSION = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"

ConnectionFactory = Callable[[str], Any]
DeliveryLockFactory = Callable[..., AbstractContextManager[Mapping[str, Any]]]


def _signed_advisory_key(value: str) -> int:
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    unsigned = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return unsigned if unsigned < 2**63 else unsigned - 2**64


LOCK_KEY = _signed_advisory_key(f"{LOCK_MODEL_VERSION}:{LOCK_SCOPE}")
LOCK_KEY_FINGERPRINT = hashlib.sha256(str(LOCK_KEY).encode("ascii")).hexdigest()[:24]


def _default_connection_factory(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL notification locking requires psycopg. Run `make setup` first."
        ) from exc
    return psycopg.connect(dsn, autocommit=True)


def _boolean_result(row: Any, label: str) -> bool:
    if (
        not isinstance(row, Sequence)
        or isinstance(row, (str, bytes, bytearray))
        or len(row) != 1
        or type(row[0]) is not bool
    ):
        raise StorageError(f"PostgreSQL returned invalid {label} evidence")
    return bool(row[0])


def _close_quietly(connection: Any) -> None:
    try:
        connection.close()
    except Exception:
        pass


@contextmanager
def acquire_notification_delivery_lock(
    *,
    dsn: str,
    connection_factory: ConnectionFactory | None = None,
) -> Iterator[Mapping[str, Any]]:
    """Hold one repository-wide delivery lock across evidence reads and writes.

    The lock is session-scoped, non-blocking and shared by initial delivery and
    governed retry execution. Closing the PostgreSQL connection also releases the
    lock, including after local process failure.
    """

    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    selected_factory = connection_factory or _default_connection_factory
    try:
        connection = selected_factory(dsn)
    except Exception:
        raise StorageError("Unable to open the notification delivery lock session") from None

    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", [LOCK_KEY])
            acquired = _boolean_result(cursor.fetchone(), "delivery lock acquisition")
    except StorageError:
        _close_quietly(connection)
        raise
    except Exception:
        _close_quietly(connection)
        raise StorageError("Unable to acquire the notification delivery lock") from None

    if not acquired:
        _close_quietly(connection)
        raise OverlapError(
            "Another notification delivery execution already holds the global lock"
        )

    evidence = {
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
        "acquired": True,
    }
    body_failed = False
    release_failed = False
    try:
        yield evidence
    except BaseException:
        body_failed = True
        raise
    finally:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", [LOCK_KEY])
                released = _boolean_result(
                    cursor.fetchone(),
                    "delivery lock release",
                )
                if not released:
                    release_failed = True
        except Exception:
            release_failed = True
        try:
            connection.close()
        except Exception:
            release_failed = True
        if release_failed and not body_failed:
            raise StorageError("Unable to release the notification delivery lock")
