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
    validate_retry_execution_record,
)
from src.warehouse.notification_retry_execution_recorder import (
    record_notification_retry_execution_with_cursor,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
    validate_notification_retry_readiness_binding,
)
from src.warehouse.notification_retry_readiness_binding_recorder import (
    record_notification_retry_readiness_binding_with_cursor,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_INPUT_BYTES = 1_048_576


def validate_notification_retry_governance_bundle(
    *,
    terminal_record: Mapping[str, Any],
    readiness_binding: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate that the binding was built from the supplied terminal record."""

    terminal = validate_retry_execution_record(terminal_record)
    binding = validate_notification_retry_readiness_binding(readiness_binding)
    rebuilt = build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=binding["readiness_enforcement"],
        recorded_at=binding["recorded_at"],
    )
    if rebuilt != binding:
        raise ValidationError(
            "retry readiness binding was not built from the supplied terminal record"
        )
    return terminal, binding


def _existing_binding_is_present(cursor: Any, *, terminal_record_id: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM risk_platform.notification_retry_readiness_bindings
        WHERE terminal_record_id = %s
        """,
        (terminal_record_id,),
    )
    return cursor.fetchone() is not None


def record_notification_retry_governance_bundle(
    *,
    dsn: str,
    terminal_record: Mapping[str, Any],
    readiness_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Commit one terminal record and its readiness binding atomically."""

    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    terminal, binding = validate_notification_retry_governance_bundle(
        terminal_record=terminal_record,
        readiness_binding=readiness_binding,
    )
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Atomic notification retry history requires psycopg") from exc

    connection = psycopg.connect(dsn)
    try:
        with connection.cursor() as cursor:
            terminal_history = record_notification_retry_execution_with_cursor(
                cursor,
                record=terminal,
            )
            terminal_created = terminal_history["created"] is True
            if not terminal_created and not _existing_binding_is_present(
                cursor,
                terminal_record_id=terminal["record_id"],
            ):
                raise ValidationError(
                    "existing retry terminal has no readiness binding; "
                    "atomic replay cannot backfill legacy history"
                )
            readiness_history = record_notification_retry_readiness_binding_with_cursor(
                cursor,
                binding=binding,
            )
            readiness_created = readiness_history["created"] is True
            if terminal_created != readiness_created:
                raise ValidationError(
                    "retry terminal and readiness binding replay states disagree"
                )
        connection.commit()
    except (StorageError, ValidationError):
        connection.rollback()
        raise
    except Exception:
        connection.rollback()
        raise StorageError(
            "atomic notification retry history database operation failed"
        ) from None
    finally:
        connection.close()

    return {
        "model_version": "portfolio-risk-notification-retry-governance-bundle-v1",
        "terminal_history": terminal_history,
        "readiness_history": readiness_history,
        "terminal_record_id": terminal["record_id"],
        "binding_id": binding["binding_id"],
        "created": terminal_created,
        "exact_replay": not terminal_created,
        "atomic_commit": True,
    }


def _read_json(path: Path, label: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError(f"{label} must not be a symbolic link")
    if not path.is_file():
        raise ValidationError(f"{label} must be a regular file")
    try:
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise ValidationError(f"{label} exceeds 1 MB")
        value = json.loads(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise
    except (OSError, UnicodeError, ValueError):
        raise ValidationError(f"{label} could not be read") from None
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a JSON object")
    return dict(value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Atomically append one notification retry terminal record and its "
            "exact readiness binding to PostgreSQL."
        )
    )
    parser.add_argument("--terminal-record", required=True, type=Path)
    parser.add_argument("--readiness-binding", required=True, type=Path)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = record_notification_retry_governance_bundle(
            dsn=args.dsn,
            terminal_record=_read_json(args.terminal_record, "retry terminal record"),
            readiness_binding=_read_json(
                args.readiness_binding,
                "retry readiness binding",
            ),
        )
    except ValidationError as exc:
        print(f"Atomic notification retry history rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Atomic notification retry history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
