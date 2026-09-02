from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
)

MAX_INPUT_BYTES = 1_048_576


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError("recorded-at must be ISO-8601") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("recorded-at must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _read_json(path: Path, label: str) -> dict[str, Any]:
    if path.is_symlink():
        raise ValidationError(f"{label} must not be a symbolic link")
    if not path.is_file():
        raise ValidationError(f"{label} must be a regular file")
    try:
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise ValidationError(f"{label} exceeds 1 MB")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise
    except (OSError, UnicodeError, ValueError):
        raise ValidationError(f"{label} could not be read") from None
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a JSON object")
    return dict(payload)


def _read_enforcement(path: Path) -> dict[str, Any]:
    payload = _read_json(path, "readiness execution summary")
    enforcement = payload.get("execution_readiness")
    if not isinstance(enforcement, Mapping):
        raise ValidationError(
            "readiness execution summary has no execution_readiness evidence"
        )
    return dict(enforcement)


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("retry readiness binding summary must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except (OSError, TypeError, ValueError):
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write retry readiness binding summary") from None


def build_retry_readiness_binding_from_files(
    *,
    terminal_record_path: Path,
    execution_summary_path: Path,
    recorded_at: datetime,
) -> dict[str, Any]:
    return build_notification_retry_readiness_binding(
        terminal_record=_read_json(terminal_record_path, "retry terminal record"),
        readiness_enforcement=_read_enforcement(execution_summary_path),
        recorded_at=recorded_at,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build one deterministic binding between a retry terminal record and "
            "the readiness authority that permitted it."
        )
    )
    parser.add_argument("--terminal-record", required=True, type=Path)
    parser.add_argument("--execution-summary", required=True, type=Path)
    parser.add_argument("--recorded-at", required=True, type=_timestamp)
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        binding = build_retry_readiness_binding_from_files(
            terminal_record_path=args.terminal_record,
            execution_summary_path=args.execution_summary,
            recorded_at=args.recorded_at,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, binding)
    except ValidationError as exc:
        print(f"Retry readiness binding rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError as exc:
        print(f"Retry readiness binding failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(binding, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
