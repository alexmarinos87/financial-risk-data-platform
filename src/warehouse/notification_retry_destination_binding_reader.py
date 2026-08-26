from __future__ import annotations

import hashlib
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_destination_binding_contract import (
    canonical_binding_bytes,
    validate_retry_destination_binding,
)


def read_notification_retry_destination_binding(
    *,
    dsn: str,
    record_id: str,
) -> dict[str, Any] | None:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry destination history requires psycopg") from exc

    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT binding_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_destination_bindings
                    WHERE record_id = %s
                    """,
                    (record_id,),
                )
                row = cursor.fetchone()
    except Exception:
        raise StorageError(
            "notification retry destination history could not be read"
        ) from None
    if row is None:
        return None
    validated = validate_retry_destination_binding(row["binding_json"])
    digest = hashlib.sha256(canonical_binding_bytes(validated)).hexdigest()
    if row["document_sha256"] != digest:
        raise ValidationError("retained destination binding digest is invalid")
    authority = validated["destination_authority"]
    return {
        "binding_id": validated["binding_id"],
        "record_id": validated["record_id"],
        "destination_id": authority["destination_id"],
        "destination_fingerprint": authority["destination_fingerprint"],
        "authority_id": authority["authority_id"],
        "document_sha256": digest,
        "created": False,
    }
