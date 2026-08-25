from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_execution_contract import (
    notification_retry_execution_document_sha256,
    validate_notification_retry_execution_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _result_summary(
    record: Mapping[str, Any],
    *,
    created: bool,
    document_sha256: str,
) -> dict[str, Any]:
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "plan_id": record["plan_id"],
        "execution_id": record["execution_id"],
        "terminal_status": record["terminal_status"],
        "requested_event_count": len(record["requested_event_ids"]),
        "persisted_event_count": len(record["persisted_event_ids"]),
        "document_sha256": document_sha256,
        "created": created,
    }


def read_notification_retry_execution_by_request_id(
    *,
    dsn: str,
    request_id: str,
) -> dict[str, Any] | None:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValidationError("request_id must be non-empty text")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Retry execution history requires psycopg") from exc
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
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
        raise StorageError("Unable to read notification retry execution history") from None
    if row is None:
        return None
    record = validate_notification_retry_execution_record(row["record_json"])
    expected_digest = notification_retry_execution_document_sha256(record)
    if row["document_sha256"] != expected_digest:
        raise StorageError("Stored notification retry execution digest is invalid")
    return record


def record_notification_retry_execution(
    *,
    dsn: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    validated = validate_notification_retry_execution_record(record)
    digest = notification_retry_execution_document_sha256(validated)
    try:
        import psycopg
        from psycopg.rows import dict_row
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Retry execution history requires psycopg") from exc

    configuration = validated["configuration"]
    concurrency = validated["concurrency_control"]
    requested_event_ids = validated["requested_event_ids"]
    persisted_event_ids = validated["persisted_event_ids"]
    persisted_attempt_ids = validated["persisted_attempt_ids"]
    created = False
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
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
                    existing_record = validate_notification_retry_execution_record(
                        existing["record_json"]
                    )
                    if (
                        existing["document_sha256"] != digest
                        or existing_record != validated
                    ):
                        raise ValidationError(
                            "request_id already exists with different retry execution "
                            "evidence"
                        )
                    return _result_summary(
                        existing_record,
                        created=False,
                        document_sha256=digest,
                    )

                cursor.execute(
                    """
                    INSERT INTO
                    risk_platform.portfolio_risk_notification_retry_executions (
                        record_id,
                        model_version,
                        request_id,
                        plan_id,
                        execution_id,
                        terminal_status,
                        started_at,
                        finished_at,
                        failure_stage,
                        failure_code,
                        delivery_fingerprint,
                        retry_policy_fingerprint,
                        retry_execution_policy_fingerprint,
                        delivery_lock_model_version,
                        delivery_lock_key_fingerprint,
                        requested_event_count,
                        persisted_event_count,
                        requested_event_ids,
                        persisted_event_ids,
                        persisted_attempt_ids,
                        execution_json,
                        record_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING record_id
                    """,
                    (
                        validated["record_id"],
                        validated["model_version"],
                        validated["request_id"],
                        validated["plan_id"],
                        validated["execution_id"],
                        validated["terminal_status"],
                        validated["started_at"],
                        validated["finished_at"],
                        validated["failure_stage"],
                        validated["failure_code"],
                        configuration["delivery_fingerprint"],
                        configuration["retry_policy_fingerprint"],
                        configuration["retry_execution_policy_fingerprint"],
                        concurrency["model_version"],
                        concurrency["key_fingerprint"],
                        len(requested_event_ids),
                        len(persisted_event_ids),
                        Jsonb(requested_event_ids),
                        Jsonb(persisted_event_ids),
                        Jsonb(persisted_attempt_ids),
                        Jsonb(validated["execution"])
                        if validated["execution"] is not None
                        else None,
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.portfolio_risk_notification_retry_executions
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "Notification retry execution was not readable after insert"
                    )
                stored_record = validate_notification_retry_execution_record(
                    stored["record_json"]
                )
                if stored["document_sha256"] != digest or stored_record != validated:
                    raise ValidationError(
                        "request_id already exists with different retry execution "
                        "evidence"
                    )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Notification retry execution recording failed") from None
    return _result_summary(
        validated,
        created=created,
        document_sha256=digest,
    )


def _read_record(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise ValidationError("retry execution record must be one regular file")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise ValidationError("retry execution record could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("retry execution record must contain an object")
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
        print(f"Notification retry execution recording failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
