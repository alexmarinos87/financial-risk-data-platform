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
from src.warehouse.notification_execution_readiness_history_contract import (
    canonical_notification_execution_readiness_record_bytes,
    validate_notification_execution_readiness_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_RECORD_BYTES = 1_048_576


def _result(
    record: Mapping[str, Any],
    *,
    digest: str,
    created: bool,
) -> dict[str, Any]:
    decision = record["decision"]
    return {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "decision_id": decision["decision_id"],
        "destination_id": decision["destination"]["destination_id"],
        "execution_kind": decision["execution_kind"],
        "decision": decision["decision"],
        "document_sha256": digest,
        "created": created,
    }


def record_notification_execution_readiness(
    *,
    dsn: str,
    record: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_notification_execution_readiness_record(record)
    canonical = canonical_notification_execution_readiness_record_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    decision = validated["decision"]
    configuration = decision["configuration"]
    destination = decision["destination"]
    activation = decision["activation_review"] or {}
    transition = decision["transition_review"] or {}
    ambiguity = decision["ambiguity"]

    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness history requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.notification_execution_readiness_decisions
                    WHERE request_id = %s
                    """,
                    (validated["request_id"],),
                )
                existing = cursor.fetchone()
                if existing is not None:
                    stored_json, stored_digest = existing
                    if stored_json != validated or stored_digest != digest:
                        raise ValidationError(
                            "request_id already exists with different readiness evidence"
                        )
                    return _result(validated, digest=digest, created=False)

                cursor.execute(
                    """
                    INSERT INTO
                    risk_platform.notification_execution_readiness_decisions (
                        record_id,
                        model_version,
                        request_id,
                        decision_id,
                        destination_id,
                        execution_kind,
                        evaluated_at,
                        recorded_at,
                        decision,
                        blocking_reasons_json,
                        delivery_fingerprint,
                        retry_policy_fingerprint,
                        retry_execution_policy_fingerprint,
                        endpoint_environment_variable,
                        destination_fingerprint,
                        destination_activation_status,
                        activation_authority_id,
                        activation_checklist_id,
                        activation_review_status,
                        activation_ready,
                        transition_record_id,
                        transition_rehearsal_id,
                        transition_review_status,
                        transition_ready,
                        ambiguity_count,
                        ambiguity_event_ids_json,
                        ambiguity_record_ids_json,
                        unbound_ambiguity_event_ids_json,
                        decision_json,
                        record_json,
                        document_sha256
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT DO NOTHING
                    RETURNING record_id
                    """,
                    (
                        validated["record_id"],
                        validated["model_version"],
                        validated["request_id"],
                        decision["decision_id"],
                        destination["destination_id"],
                        decision["execution_kind"],
                        decision["evaluated_at"],
                        validated["recorded_at"],
                        decision["decision"],
                        Jsonb(decision["blocking_reasons"]),
                        configuration["delivery_fingerprint"],
                        configuration["retry_policy_fingerprint"],
                        configuration["retry_execution_policy_fingerprint"],
                        configuration["endpoint_environment_variable"],
                        destination["fingerprint"],
                        destination["activation_status"],
                        activation.get("authority_id"),
                        activation.get("checklist_id"),
                        activation.get("review_status"),
                        activation.get("operational_activation_ready"),
                        transition.get("transition_record_id"),
                        transition.get("transition_rehearsal_id"),
                        transition.get("transition_review_status"),
                        transition.get("transition_ready"),
                        ambiguity["count"],
                        Jsonb(ambiguity["event_ids"]),
                        Jsonb(ambiguity["record_ids"]),
                        Jsonb(ambiguity["unbound_event_ids"]),
                        Jsonb(decision),
                        Jsonb(validated),
                        digest,
                    ),
                )
                created = cursor.fetchone() is not None
                if not created:
                    cursor.execute(
                        """
                        SELECT request_id, record_json, document_sha256
                        FROM risk_platform.notification_execution_readiness_decisions
                        WHERE decision_id = %s
                        """,
                        (decision["decision_id"],),
                    )
                    conflict = cursor.fetchone()
                    if conflict is not None:
                        conflict_request_id, stored_json, stored_digest = conflict
                        if (
                            conflict_request_id != validated["request_id"]
                            or stored_json != validated
                            or stored_digest != digest
                        ):
                            raise ValidationError(
                                "decision_id already exists under different readiness evidence"
                            )

                cursor.execute(
                    """
                    SELECT record_json, document_sha256
                    FROM risk_platform.notification_execution_readiness_decisions
                    WHERE record_id = %s
                    """,
                    (validated["record_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError("notification readiness decision was not retained")
                stored_json, stored_digest = stored
                if stored_json != validated or stored_digest != digest:
                    raise ValidationError(
                        "record_id already exists with different readiness evidence"
                    )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "notification readiness history database operation failed"
        ) from None

    return _result(validated, digest=digest, created=created)


def _read_record(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError("notification readiness record must not be a symbolic link")
    if not path.is_file():
        raise ValidationError("notification readiness record must be a regular file")
    if path.stat().st_size > MAX_RECORD_BYTES:
        raise ValidationError("notification readiness record exceeds 1 MB")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValidationError("notification readiness record could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("notification readiness record must be a JSON object")
    return dict(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append one notification execution readiness decision to PostgreSQL."
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
        result = record_notification_execution_readiness(
            dsn=args.dsn,
            record=_read_record(args.record),
        )
    except ValidationError as exc:
        print(f"Notification readiness record rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Notification readiness history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
