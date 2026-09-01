from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    canonical_transition_rehearsal_record_bytes,
    validate_notification_destination_transition_rehearsal_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_RECORD_BYTES = 1_048_576


def _result(
    record: Mapping[str, Any],
    *,
    digest: str,
    created: bool,
) -> dict[str, Any]:
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "rehearsal_id": record["rehearsal_id"],
        "destination_id": record["destination_id"],
        "rotate_plan_id": record["rotate_plan_id"],
        "disable_plan_id": record["disable_plan_id"],
        "rollback_plan_id": record["rollback_plan_id"],
        "document_sha256": digest,
        "created": created,
    }


def record_notification_destination_transition_rehearsal(
    *,
    dsn: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_notification_destination_transition_rehearsal_record(
        record
    )
    digest = hashlib.sha256(
        canonical_transition_rehearsal_record_bytes(validated)
    ).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError(
            "Notification destination transition history requires psycopg"
        ) from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.notification_destination_transition_rehearsals
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_json, existing_digest = existing
                    if existing_json != validated or existing_digest != digest:
                        raise ValidationError(
                            "request_id already exists with different transition evidence"
                        )
                    return _result(validated, digest=digest, created=False)

                cursor.execute(
                    """
                    INSERT INTO
                    risk_platform.notification_destination_transition_rehearsals (
                        record_id,
                        model_version,
                        request_id,
                        rehearsal_id,
                        destination_id,
                        rotate_plan_id,
                        disable_plan_id,
                        rollback_plan_id,
                        baseline_authority_id,
                        rotate_authority_id,
                        rollback_authority_id,
                        rotate_checklist_id,
                        rollback_checklist_id,
                        rotate_destination_fingerprint,
                        disable_destination_fingerprint,
                        rollback_destination_fingerprint,
                        rotate_endpoint_environment_variable,
                        disable_endpoint_environment_variable,
                        rollback_endpoint_environment_variable,
                        started_at,
                        finished_at,
                        recorded_at,
                        rotate_request_count,
                        rollback_request_count,
                        same_content_duplicate_count,
                        rehearsal_json,
                        record_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING record_id
                    """,
                    (
                        validated["record_id"],
                        validated["model_version"],
                        validated["request_id"],
                        validated["rehearsal_id"],
                        validated["destination_id"],
                        validated["rotate_plan_id"],
                        validated["disable_plan_id"],
                        validated["rollback_plan_id"],
                        validated["baseline_authority_id"],
                        validated["rotate_authority_id"],
                        validated["rollback_authority_id"],
                        validated["rotate_checklist_id"],
                        validated["rollback_checklist_id"],
                        validated["rotate_destination_fingerprint"],
                        validated["disable_destination_fingerprint"],
                        validated["rollback_destination_fingerprint"],
                        validated["rotate_endpoint_environment_variable"],
                        validated["disable_endpoint_environment_variable"],
                        validated["rollback_endpoint_environment_variable"],
                        validated["started_at"],
                        validated["finished_at"],
                        validated["recorded_at"],
                        validated["rotate_request_count"],
                        validated["rollback_request_count"],
                        validated["same_content_duplicate_count"],
                        Jsonb(validated["rehearsal_summary"]),
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                if not created:
                    cursor.execute(
                        """
                        SELECT request_id, record_json, document_sha256
                        FROM risk_platform.notification_destination_transition_rehearsals
                        WHERE rehearsal_id = %s
                        """,
                        (validated["rehearsal_id"],),
                    )
                    rehearsal_conflict = cursor.fetchone()
                    if rehearsal_conflict is not None:
                        conflict_request, stored_json, stored_digest = rehearsal_conflict
                        if (
                            conflict_request != validated["request_id"]
                            or stored_json != validated
                            or stored_digest != digest
                        ):
                            raise ValidationError(
                                "rehearsal_id already exists under different evidence"
                            )

                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.notification_destination_transition_rehearsals
                    WHERE record_id = %s
                    """,
                    (validated["record_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "notification destination transition rehearsal was not retained"
                    )
                stored_json, stored_digest = stored
                if stored_json != validated or stored_digest != digest:
                    raise ValidationError(
                        "record_id already exists with different transition evidence"
                    )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "notification destination transition history database operation failed"
        ) from None

    return _result(validated, digest=digest, created=created)


def _read_record(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError("transition rehearsal record must not be a symbolic link")
    if not path.is_file():
        raise ValidationError("transition rehearsal record must be a regular file")
    if path.stat().st_size > MAX_RECORD_BYTES:
        raise ValidationError("transition rehearsal record exceeds 1 MB")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValidationError("transition rehearsal record could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("transition rehearsal record must be a JSON object")
    return dict(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Append one notification destination transition rehearsal to PostgreSQL."
        )
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
        result = record_notification_destination_transition_rehearsal(
            dsn=args.dsn,
            record=_read_record(args.record),
        )
    except ValidationError as exc:
        print(f"Destination transition rehearsal rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Destination transition history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
