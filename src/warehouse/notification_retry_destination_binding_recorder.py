from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_destination_binding_contract import (
    canonical_binding_bytes,
    validate_retry_destination_binding,
)


def record_notification_retry_destination_binding(
    *,
    dsn: str,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_retry_destination_binding(binding)
    digest = hashlib.sha256(canonical_binding_bytes(validated)).hexdigest()
    authority = validated["destination_authority"]
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry destination history requires psycopg") from exc

    created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT binding_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_destination_bindings
                    WHERE record_id = %s
                    """,
                    (validated["record_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_json, existing_digest = existing
                    if existing_digest != digest or existing_json != validated:
                        raise ValidationError(
                            "retry record already has different destination evidence"
                        )
                    return {
                        "binding_id": validated["binding_id"],
                        "record_id": validated["record_id"],
                        "destination_id": authority["destination_id"],
                        "destination_fingerprint": authority[
                            "destination_fingerprint"
                        ],
                        "document_sha256": digest,
                        "created": False,
                    }

                cursor.execute(
                    """
                    INSERT INTO
                    risk_platform.portfolio_risk_notification_retry_destination_bindings (
                        binding_id,
                        model_version,
                        record_id,
                        request_id,
                        plan_id,
                        execution_id,
                        authority_id,
                        destination_id,
                        destination_fingerprint,
                        endpoint_environment_variable,
                        evaluated_at,
                        evaluated_event_types_json,
                        authority_json,
                        recorded_at,
                        binding_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING binding_id
                    """,
                    (
                        validated["binding_id"],
                        validated["model_version"],
                        validated["record_id"],
                        validated["request_id"],
                        validated["plan_id"],
                        validated["execution_id"],
                        authority["authority_id"],
                        authority["destination_id"],
                        authority["destination_fingerprint"],
                        authority["endpoint_environment_variable"],
                        authority["evaluated_at"],
                        Jsonb(authority["evaluated_event_types"]),
                        Jsonb(authority),
                        validated["recorded_at"],
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT binding_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_destination_bindings
                    WHERE binding_id = %s
                    """,
                    (validated["binding_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "notification retry destination binding was not retained"
                    )
                stored_json, stored_digest = stored
                if stored_digest != digest or stored_json != validated:
                    raise ValidationError(
                        "binding_id already exists with different destination evidence"
                    )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "notification retry destination history database operation failed"
        ) from None

    return {
        "binding_id": validated["binding_id"],
        "record_id": validated["record_id"],
        "destination_id": authority["destination_id"],
        "destination_fingerprint": authority["destination_fingerprint"],
        "document_sha256": digest,
        "created": created,
    }
