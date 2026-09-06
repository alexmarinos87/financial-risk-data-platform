"""Check session state before reading transaction-time readiness views."""
from __future__ import annotations

from typing import Any

from src.common.exceptions import StorageError


def require_fresh_readiness_session(cursor: Any) -> None:
    """Require an exclusively owned, idle autocommit connection outside pipelines.

    Timestamp equality is not a transaction-state test. In particular, protocol
    preparation can give a fresh parameterized statement distinct timestamps.
    No query or caller transaction mutation is performed by this guard.
    """
    connection = getattr(cursor, "connection", None)
    info = getattr(connection, "info", None)
    transaction = getattr(info, "transaction_status", None)
    pipeline = getattr(info, "pipeline_status", None)
    if (
        getattr(connection, "closed", None) is not False
        or getattr(connection, "autocommit", None) is not True
        or getattr(transaction, "name", None) != "IDLE"
        or getattr(pipeline, "name", None) != "OFF"
    ):
        raise StorageError(
            "worker readiness sources require a fresh read-only statement"
        )
