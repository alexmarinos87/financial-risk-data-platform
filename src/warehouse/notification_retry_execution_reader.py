from __future__ import annotations

import hashlib
import re
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_execution_contract import (
    canonical_retry_execution_record_bytes,
    validate_retry_execution_record,
)

SAFE_REQUEST_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")


def _history_summary(
    record: dict[str, Any],
    *,
    digest: str,
) -> dict[str, Any]:
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "execution_id": record["execution_id"],
        "plan_id": record["plan_id"],
        "terminal_status": record["terminal_status"],
        "failure_code": record["failure_code"],
        "request_count": record["request_count"],
        "attempts_persisted": record["attempts_persisted"],
        "requested_events": len(record["requested_event_ids"]),
        "document_sha256": digest,
        "created": False,
    }


def read_notification_retry_execution_request(
    *,
    dsn: str,
    request_id: str,
) -> dict[str, Any] | None:
    """Return one validated terminal request record before retry side effects."""

    if not isinstance(request_id, str) or not SAFE_REQUEST_ID.fullmatch(request_id):
        raise ValidationError("request_id must be one safe text segment")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry history requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_executions
                    WHERE request_id = %s
                    """,
                    (request_id,),
                )
                row = cursor.fetchone()
    except Exception:
        raise StorageError(
            "notification retry history lookup failed before execution"
        ) from None

    if row is None:
        return None
    record_json, stored_digest = row
    validated = validate_retry_execution_record(record_json)
    canonical = canonical_retry_execution_record_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    if stored_digest != digest:
        raise ValidationError("retained retry execution document digest is invalid")
    if validated["request_id"] != request_id:
        raise ValidationError("retained retry execution request identity is invalid")
    return {
        "record": validated,
        "history": _history_summary(validated, digest=digest),
    }
