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
from src.warehouse.notification_retry_execution_contract import (
    canonical_retry_execution_record_bytes,
    validate_retry_execution_record,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    canonical_notification_retry_readiness_binding_bytes,
    validate_notification_retry_readiness_binding,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_RECORD_BYTES = 1_048_576


def _result(
    binding: Mapping[str, Any],
    *,
    digest: str,
    created: bool,
) -> dict[str, Any]:
    terminal = binding["terminal_execution"]
    enforcement = binding["readiness_enforcement"]
    return {
        "binding_id": binding["binding_id"],
        "terminal_record_id": terminal["record_id"],
        "terminal_request_id": terminal["request_id"],
        "readiness_record_id": enforcement["readiness_record_id"],
        "retained_decision_id": enforcement["retained_decision_id"],
        "refreshed_decision_id": enforcement["refreshed_decision_id"],
        "enforcement_id": enforcement["enforcement_id"],
        "destination_id": enforcement["destination_id"],
        "document_sha256": digest,
        "created": created,
    }


def _reconcile_terminal_source(
    cursor: Any,
    *,
    binding: Mapping[str, Any],
) -> None:
    terminal = binding["terminal_execution"]
    cursor.execute(
        """
        SELECT record_json, document_sha256
        FROM risk_platform.portfolio_risk_notification_retry_executions
        WHERE record_id = %s
        """,
        (terminal["record_id"],),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValidationError("retry readiness binding terminal source is missing")
    source_json, stored_digest = row
    if not isinstance(source_json, Mapping):
        raise StorageError("retry readiness binding terminal source is invalid")
    source = validate_retry_execution_record(source_json)
    source_digest = hashlib.sha256(
        canonical_retry_execution_record_bytes(source)
    ).hexdigest()
    if stored_digest != source_digest:
        raise ValidationError("retry terminal source digest is invalid")
    expected = {
        "attempts_persisted": source["attempts_persisted"],
        "document_sha256": source_digest,
        "execution_id": source["execution_id"],
        "finished_at": source["finished_at"],
        "plan_id": source["plan_id"],
        "record_id": source["record_id"],
        "recorded_at": source["recorded_at"],
        "request_count": source["request_count"],
        "request_id": source["request_id"],
        "started_at": source["started_at"],
        "terminal_status": source["terminal_status"],
    }
    if terminal != expected:
        raise ValidationError(
            "retry readiness binding does not match retained terminal evidence"
        )


def _reconcile_readiness_source(
    cursor: Any,
    *,
    binding: Mapping[str, Any],
) -> None:
    enforcement = binding["readiness_enforcement"]
    cursor.execute(
        """
        SELECT record_json, document_sha256
        FROM risk_platform.notification_execution_readiness_decisions
        WHERE record_id = %s
        """,
        (enforcement["readiness_record_id"],),
    )
    row = cursor.fetchone()
    if row is None:
        raise ValidationError("retry readiness binding readiness source is missing")
    source_json, stored_digest = row
    if not isinstance(source_json, Mapping):
        raise StorageError("retry readiness binding readiness source is invalid")
    source = validate_notification_execution_readiness_record(source_json)
    source_digest = hashlib.sha256(
        canonical_notification_execution_readiness_record_bytes(source)
    ).hexdigest()
    if stored_digest != source_digest:
        raise ValidationError("retained readiness source digest is invalid")
    decision = source["decision"]
    if source["request_id"] != enforcement["readiness_request_id"]:
        raise ValidationError("retry readiness binding readiness request changed")
    if decision["decision_id"] != enforcement["retained_decision_id"]:
        raise ValidationError("retry readiness binding retained decision changed")
    if decision["execution_kind"] != "retry":
        raise ValidationError("retry readiness binding source is not retry readiness")
    if decision["decision"] != "allow" or decision["blocking_reasons"]:
        raise ValidationError("retry readiness binding source does not allow execution")
    if (
        decision["destination"]["destination_id"]
        != enforcement["destination_id"]
    ):
        raise ValidationError("retry readiness binding destination changed")
    if decision["evaluated_at"] != enforcement["retained_decision_evaluated_at"]:
        raise ValidationError("retry readiness binding evaluation time changed")


def record_notification_retry_readiness_binding_with_cursor(
    cursor: Any,
    *,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_notification_retry_readiness_binding(binding)
    canonical = canonical_notification_retry_readiness_binding_bytes(validated)
    digest = hashlib.sha256(canonical).hexdigest()
    terminal = validated["terminal_execution"]
    enforcement = validated["readiness_enforcement"]
    lock = enforcement["lock"]

    _reconcile_terminal_source(cursor, binding=validated)
    _reconcile_readiness_source(cursor, binding=validated)

    cursor.execute(
        """
        SELECT binding_json, document_sha256
        FROM risk_platform.notification_retry_readiness_bindings
        WHERE terminal_record_id = %s
        """,
        (terminal["record_id"],),
    )
    existing = cursor.fetchone()
    if existing is not None:
        existing_json, existing_digest = existing
        if existing_json != validated or existing_digest != digest:
            raise ValidationError(
                "retry terminal record already has different readiness evidence"
            )
        return _result(validated, digest=digest, created=False)

    try:
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc

    cursor.execute(
        """
        INSERT INTO risk_platform.notification_retry_readiness_bindings (
            binding_id,
            model_version,
            terminal_record_id,
            terminal_request_id,
            terminal_plan_id,
            terminal_execution_id,
            terminal_status,
            terminal_started_at,
            terminal_finished_at,
            terminal_recorded_at,
            terminal_request_count,
            terminal_attempts_persisted,
            terminal_document_sha256,
            readiness_record_id,
            readiness_request_id,
            retained_decision_id,
            refreshed_decision_id,
            enforcement_id,
            destination_id,
            execution_kind,
            enforced_at,
            retained_decision_evaluated_at,
            refreshed_decision_evaluated_at,
            lock_model_version,
            lock_scope,
            lock_key_fingerprint,
            readiness_enforcement_sha256,
            binding_recorded_at,
            readiness_enforcement_json,
            binding_json,
            document_sha256
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
        RETURNING binding_id
        """,
        (
            validated["binding_id"],
            validated["model_version"],
            terminal["record_id"],
            terminal["request_id"],
            terminal["plan_id"],
            terminal["execution_id"],
            terminal["terminal_status"],
            terminal["started_at"],
            terminal["finished_at"],
            terminal["recorded_at"],
            terminal["request_count"],
            terminal["attempts_persisted"],
            terminal["document_sha256"],
            enforcement["readiness_record_id"],
            enforcement["readiness_request_id"],
            enforcement["retained_decision_id"],
            enforcement["refreshed_decision_id"],
            enforcement["enforcement_id"],
            enforcement["destination_id"],
            enforcement["execution_kind"],
            enforcement["enforced_at"],
            enforcement["retained_decision_evaluated_at"],
            enforcement["refreshed_decision_evaluated_at"],
            lock["model_version"],
            lock["scope"],
            lock["key_fingerprint"],
            validated["readiness_enforcement_sha256"],
            validated["recorded_at"],
            Jsonb(enforcement),
            Jsonb(validated),
            digest,
        ),
    )
    created = cursor.fetchone() is not None
    cursor.execute(
        """
        SELECT binding_json, document_sha256
        FROM risk_platform.notification_retry_readiness_bindings
        WHERE terminal_record_id = %s
        """,
        (terminal["record_id"],),
    )
    stored = cursor.fetchone()
    if stored is None:
        raise ValidationError(
            "binding or enforcement identity already exists under different evidence"
        )
    stored_json, stored_digest = stored
    if stored_json != validated or stored_digest != digest:
        raise ValidationError(
            "retry readiness binding identity already exists under different evidence"
        )
    return _result(validated, digest=digest, created=created)


def record_notification_retry_readiness_binding(
    *,
    dsn: str,
    binding: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc

    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                result = record_notification_retry_readiness_binding_with_cursor(
                    cursor,
                    binding=binding,
                )
            connection.commit()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "notification retry readiness history database operation failed"
        ) from None
    return result


def _read_binding(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError("retry readiness binding must not be a symbolic link")
    if not path.is_file():
        raise ValidationError("retry readiness binding must be a regular file")
    if path.stat().st_size > MAX_RECORD_BYTES:
        raise ValidationError("retry readiness binding exceeds 1 MB")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError):
        raise ValidationError("retry readiness binding could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError("retry readiness binding must be a JSON object")
    return dict(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Append one retry readiness binding to PostgreSQL."
    )
    parser.add_argument("--binding", required=True, type=Path)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = record_notification_retry_readiness_binding(
            dsn=args.dsn,
            binding=_read_binding(args.binding),
        )
    except ValidationError as exc:
        print(f"Retry readiness binding rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Retry readiness binding history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
