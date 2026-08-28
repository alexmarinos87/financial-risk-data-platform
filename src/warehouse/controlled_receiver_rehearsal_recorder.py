from __future__ import annotations

import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.controlled_receiver_rehearsal_contract import (
    activation_checklist_sha256,
    canonical_controlled_receiver_rehearsal_bytes,
    validate_controlled_receiver_rehearsal_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _summary(
    record: Mapping[str, Any],
    *,
    created: bool,
    digest: str,
    checklist_created: bool,
) -> dict[str, Any]:
    checklist = record["activation_checklist"]
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "rehearsal_id": record["rehearsal_id"],
        "terminal_status": record["terminal_status"],
        "failure_code": record["failure_code"],
        "checklist_id": checklist["checklist_id"],
        "destination_id": checklist["destination_id"],
        "request_count": record["request_count"],
        "document_sha256": digest,
        "checklist_created": checklist_created,
        "created": created,
    }


def record_controlled_receiver_rehearsal(
    *,
    dsn: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_controlled_receiver_rehearsal_record(record)
    canonical = canonical_controlled_receiver_rehearsal_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    checklist = validated["activation_checklist"]
    checklist_digest = activation_checklist_sha256(checklist)
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Controlled receiver rehearsal history requires psycopg") from exc

    created = False
    checklist_created = False
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT checklist_json, document_sha256
                    FROM risk_platform.notification_activation_checklists
                    WHERE checklist_id = %s
                    """,
                    (checklist["checklist_id"],),
                )
                existing_checklist = cursor.fetchone()
                if existing_checklist is not None:
                    stored_json, stored_digest = existing_checklist
                    if stored_json != checklist or stored_digest != checklist_digest:
                        raise ValidationError(
                            "checklist_id already exists with different evidence"
                        )
                else:
                    cursor.execute(
                        """
                        INSERT INTO risk_platform.notification_activation_checklists (
                            checklist_id,
                            model_version,
                            destination_id,
                            destination_fingerprint,
                            authority_id,
                            reviewed_at,
                            review_expires_at,
                            reviewed_by_json,
                            controls_json,
                            activation_ready,
                            checklist_json,
                            document_sha256
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING checklist_id
                        """,
                        (
                            checklist["checklist_id"],
                            checklist["model_version"],
                            checklist["destination_id"],
                            checklist["destination_fingerprint"],
                            checklist["authority_id"],
                            checklist["reviewed_at"],
                            checklist["review_expires_at"],
                            Jsonb(checklist["reviewed_by"]),
                            Jsonb(checklist["controls"]),
                            checklist["activation_ready"],
                            Jsonb(checklist),
                            checklist_digest,
                        ),
                    )
                    checklist_created = cursor.fetchone() is not None
                    cursor.execute(
                        """
                        SELECT checklist_json, document_sha256
                        FROM risk_platform.notification_activation_checklists
                        WHERE checklist_id = %s
                        """,
                        (checklist["checklist_id"],),
                    )
                    stored_checklist = cursor.fetchone()
                    if stored_checklist is None:
                        raise StorageError(
                            "activation checklist could not be read after insert"
                        )
                    stored_json, stored_digest = stored_checklist
                    if stored_json != checklist or stored_digest != checklist_digest:
                        raise ValidationError(
                            "checklist_id already exists with different evidence"
                        )

                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.controlled_notification_receiver_rehearsals
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    stored_json, stored_digest = existing
                    if stored_json != validated or stored_digest != digest:
                        raise ValidationError(
                            "request_id already exists with different rehearsal evidence"
                        )
                    return _summary(
                        validated,
                        created=False,
                        digest=digest,
                        checklist_created=False,
                    )

                cursor.execute(
                    """
                    INSERT INTO
                    risk_platform.controlled_notification_receiver_rehearsals (
                        record_id,
                        model_version,
                        request_id,
                        terminal_status,
                        failure_code,
                        checklist_id,
                        destination_id,
                        destination_fingerprint,
                        authority_id,
                        receiver_model_version,
                        rehearsal_id,
                        allowed_hosts_json,
                        allowed_event_types_json,
                        response_status,
                        started_at,
                        finished_at,
                        recorded_at,
                        attempted_request_count,
                        request_count,
                        unique_idempotency_keys,
                        same_content_duplicate_count,
                        receipts_json,
                        receiver_summary_json,
                        record_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING record_id
                    """,
                    (
                        validated["record_id"],
                        validated["model_version"],
                        validated["request_id"],
                        validated["terminal_status"],
                        validated["failure_code"],
                        checklist["checklist_id"],
                        checklist["destination_id"],
                        checklist["destination_fingerprint"],
                        checklist["authority_id"],
                        validated["receiver_model_version"],
                        validated["rehearsal_id"],
                        Jsonb(validated["allowed_hosts"]),
                        Jsonb(validated["allowed_event_types"]),
                        validated["response_status"],
                        validated["started_at"],
                        validated["finished_at"],
                        validated["recorded_at"],
                        validated["attempted_request_count"],
                        validated["request_count"],
                        validated["unique_idempotency_keys"],
                        validated["same_content_duplicate_count"],
                        Jsonb(validated["receipts"]),
                        (
                            None
                            if validated["receiver_summary"] is None
                            else Jsonb(validated["receiver_summary"])
                        ),
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.controlled_notification_receiver_rehearsals
                    WHERE record_id = %s
                    """,
                    (validated["record_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "controlled receiver rehearsal could not be read after insert"
                    )
                stored_json, stored_digest = stored
                if stored_json != validated or stored_digest != digest:
                    raise ValidationError(
                        "record_id already exists with different rehearsal evidence"
                    )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "controlled receiver rehearsal database operation failed"
        ) from None

    return _summary(
        validated,
        created=created,
        digest=digest,
        checklist_created=checklist_created,
    )


def _read_record(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError("controlled receiver record must not be a symbolic link")
    if not path.is_file():
        raise ValidationError("controlled receiver record must be a regular file")
    if path.stat().st_size > 1_048_576:
        raise ValidationError("controlled receiver record exceeds 1 MB")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValidationError("controlled receiver record could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("controlled receiver record must be a JSON object")
    return dict(value)


def _build_parser() -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        description="Append one controlled receiver rehearsal to PostgreSQL."
    )
    parser.add_argument("--record", required=True, type=Path)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = record_controlled_receiver_rehearsal(
            dsn=args.dsn,
            record=_read_record(args.record),
        )
    except ValidationError as exc:
        print(f"Controlled receiver rehearsal rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Controlled receiver rehearsal history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
