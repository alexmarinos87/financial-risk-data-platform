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
from src.warehouse.notification_retry_execution_contract import (
    canonical_retry_execution_record_bytes,
    validate_retry_execution_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _summary(
    record: Mapping[str, Any],
    *,
    created: bool,
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
        "created": created,
    }


def record_notification_retry_execution(
    *,
    dsn: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_retry_execution_record(record)
    canonical = canonical_retry_execution_record_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry history requires psycopg") from exc

    created = False
    stored: dict[str, Any] | None = None
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_executions
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    existing_json, existing_digest = existing
                    if existing_digest != digest or existing_json != validated:
                        raise ValidationError(
                            "request_id already exists with different retry evidence"
                        )
                    return _summary(validated, created=False, digest=digest)

                cursor.execute(
                    """
                    INSERT INTO
                        risk_platform.portfolio_risk_notification_retry_executions (
                            record_id,
                            model_version,
                            request_id,
                            execution_id,
                            plan_id,
                            terminal_status,
                            failure_code,
                            channel,
                            endpoint_host,
                            started_at,
                            finished_at,
                            recorded_at,
                            request_count,
                            attempts_persisted,
                            succeeded_count,
                            failed_count,
                            attempt_ids_json,
                            requested_event_ids_json,
                            persisted_event_ids_json,
                            delivery_fingerprint,
                            retry_policy_fingerprint,
                            retry_execution_policy_fingerprint,
                            lock_model_version,
                            lock_key_fingerprint,
                            lock_acquired,
                            lock_released,
                            execution_summary_json,
                            record_json,
                            document_sha256
                        )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING record_id
                    """,
                    (
                        validated["record_id"],
                        validated["model_version"],
                        validated["request_id"],
                        validated["execution_id"],
                        validated["plan_id"],
                        validated["terminal_status"],
                        validated["failure_code"],
                        validated["channel"],
                        validated["endpoint_host"],
                        validated["started_at"],
                        validated["finished_at"],
                        validated["recorded_at"],
                        validated["request_count"],
                        validated["attempts_persisted"],
                        validated["succeeded_count"],
                        validated["failed_count"],
                        Jsonb(validated["attempt_ids"]),
                        Jsonb(validated["requested_event_ids"]),
                        Jsonb(validated["persisted_event_ids"]),
                        validated["delivery_fingerprint"],
                        validated["retry_policy_fingerprint"],
                        validated["retry_execution_policy_fingerprint"],
                        validated["lock_model_version"],
                        validated["lock_key_fingerprint"],
                        validated["lock_acquired"],
                        validated["lock_released"],
                        (
                            None
                            if validated["execution_summary"] is None
                            else Jsonb(validated["execution_summary"])
                        ),
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_executions
                    WHERE record_id = %s
                    """,
                    (validated["record_id"],),
                )
                row = cursor.fetchone()
                if row is None:
                    raise StorageError(
                        "notification retry record could not be read after insert"
                    )
                stored_json, stored_digest = row
                if stored_digest != digest or stored_json != validated:
                    raise ValidationError(
                        "record_id already exists with different retry evidence"
                    )
                stored = dict(stored_json)
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError(
            "notification retry history database operation failed"
        ) from None

    if stored is None:  # pragma: no cover - guarded by the transaction.
        raise StorageError("notification retry history result is unavailable")
    return _summary(stored, created=created, digest=digest)


def _read_record(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError("retry execution record must not be a symbolic link")
    if not path.is_file():
        raise ValidationError("retry execution record must be a regular file")
    if path.stat().st_size > 1_048_576:
        raise ValidationError("retry execution record exceeds 1 MB")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValidationError("retry execution record could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("retry execution record must be a JSON object")
    return dict(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append one terminal notification retry execution to PostgreSQL."
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
        result = record_notification_retry_execution(
            dsn=args.dsn,
            record=_read_record(args.record),
        )
    except ValidationError as exc:
        print(f"Notification retry execution rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Notification retry history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
