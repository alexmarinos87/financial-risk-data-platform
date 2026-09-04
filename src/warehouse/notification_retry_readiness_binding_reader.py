from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_readiness_binding_contract import (
    canonical_notification_retry_readiness_binding_bytes,
    validate_notification_retry_readiness_binding,
)

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")


def _safe_id(value: str, label: str) -> str:
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def read_notification_retry_readiness_binding(
    *,
    dsn: str,
    terminal_record_id: str,
) -> dict[str, Any] | None:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    selected_record_id = _safe_id(terminal_record_id, "terminal_record_id")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT binding_json, document_sha256
                    FROM risk_platform.notification_retry_readiness_bindings
                    WHERE terminal_record_id = %s
                    """,
                    (selected_record_id,),
                )
                row = cursor.fetchone()
    except Exception:
        raise StorageError(
            "notification retry readiness history lookup failed"
        ) from None

    if row is None:
        return None
    binding_json, stored_digest = row
    if not isinstance(binding_json, Mapping):
        raise StorageError("retained retry readiness binding is invalid")
    binding = validate_notification_retry_readiness_binding(binding_json)
    digest = hashlib.sha256(
        canonical_notification_retry_readiness_binding_bytes(binding)
    ).hexdigest()
    if stored_digest != digest:
        raise ValidationError("retained retry readiness binding digest is invalid")
    terminal = binding["terminal_execution"]
    enforcement = binding["readiness_enforcement"]
    if terminal["record_id"] != selected_record_id:
        raise ValidationError("retained retry readiness terminal identity is invalid")
    return {
        "binding": binding,
        "history": {
            "binding_id": binding["binding_id"],
            "terminal_record_id": terminal["record_id"],
            "terminal_request_id": terminal["request_id"],
            "readiness_record_id": enforcement["readiness_record_id"],
            "retained_decision_id": enforcement["retained_decision_id"],
            "refreshed_decision_id": enforcement["refreshed_decision_id"],
            "enforcement_id": enforcement["enforcement_id"],
            "destination_id": enforcement["destination_id"],
            "document_sha256": digest,
        },
    }
