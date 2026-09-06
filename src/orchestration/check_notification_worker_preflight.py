"""Validation-first, read-only diagnostics for a captured worker authority slot."""
from __future__ import annotations

import json
import os
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_cli_parser import build_preflight_parser
from src.orchestration.notification_worker_authority_contract import canonical_bytes, identifier, utc
from src.orchestration.reviewed_notification_worker_preflight import (
    MAX_BUNDLE_BYTES, build_reviewed_worker_preflight,
)
from src.warehouse.notification_worker_authority_history import _unique_object
from src.warehouse.notification_worker_authority_snapshot import (
    MAX_SNAPSHOT_BYTES, read_worker_authority_snapshot, validate_worker_authority_snapshot,
)

EXIT_CODES = {"eligible_for_health_review": 0, "wait": 3, "blocked": 4}


def load_authority_snapshot(path: Path) -> dict[str, Any]:
    """Read bounded regular-file JSON; parent directories must be trusted."""
    try:
        if path.is_symlink():
            raise ValidationError("snapshot input must not be a symbolic link")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        with os.fdopen(os.open(path, flags), "rb") as handle:
            if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
                raise ValidationError("snapshot input must be a regular file")
            raw = handle.read(MAX_SNAPSHOT_BYTES + 1)
        if len(raw) > MAX_SNAPSHOT_BYTES:
            raise ValidationError("snapshot input exceeds 1 MB")
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object)
        return validate_worker_authority_snapshot(value)
    except (OSError, TypeError, ValueError, UnicodeError, RecursionError, OverflowError):
        raise ValidationError("unable to read a valid authority snapshot") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = build_preflight_parser().parse_args(argv)
    try:
        worker_id = identifier(args.worker_id, "worker_id")
        selected = identifier(args.selected_transition_id, "selected_transition_id")
        slot = utc(args.scheduled_for, "scheduled_for").isoformat()
        if args.read_current:
            dsn = os.environ.get("WAREHOUSE_POSTGRES_DSN", "")
            if not dsn.strip():
                raise ValidationError("WAREHOUSE_POSTGRES_DSN is required for explicit reading")
            captured = read_worker_authority_snapshot(dsn=dsn, worker_id=worker_id)
        else:
            captured = load_authority_snapshot(args.snapshot)
        captured = validate_worker_authority_snapshot(captured)
        if captured["worker_id"] != worker_id:
            raise ValidationError("snapshot does not match the selected worker")
        result = build_reviewed_worker_preflight(
            snapshot=captured, selected_transition_id=selected, scheduled_for=slot,
            worker_config_path=args.worker_config, delivery_config_path=args.delivery_config,
            destination_config_path=args.destination_config,
        )
        report = {
            "source_mode": "live_database_read" if args.read_current else "retained_file",
            "database_read_performed": args.read_current,
            "runtime_permission_granted": False, "result": result,
        }
        encoded = canonical_bytes(report)
        if len(encoded) + 1 > MAX_BUNDLE_BYTES:
            raise ValidationError("worker preflight output exceeds 1 MB")
        exit_code = EXIT_CODES[result["preflight"]["outcome"]]
    except Exception:
        # Never echo filenames, DSNs, configuration values or provider errors.
        print("Worker preflight failed; no execution permission granted", file=sys.stderr)
        return 1
    print(encoded.decode("utf-8"))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
