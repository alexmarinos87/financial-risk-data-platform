from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
    acquire_notification_delivery_lock,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def run_contract_check(dsn: str) -> dict[str, Any]:
    contender_rejected = False
    with acquire_notification_delivery_lock(dsn=dsn) as first:
        try:
            with acquire_notification_delivery_lock(dsn=dsn):
                raise AssertionError("contending lock unexpectedly entered its body")
        except OverlapError:
            contender_rejected = True

    if not contender_rejected:
        raise AssertionError("contending delivery lock was not rejected")

    with acquire_notification_delivery_lock(dsn=dsn) as second:
        if dict(second) != dict(first):
            raise AssertionError("delivery lock identity changed after release")

    summary = {
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
        "first_lock_acquired": True,
        "contender_rejected": True,
        "lock_reacquired_after_release": True,
        "external_request_performed": False,
        "delivery_attempt_written": False,
    }
    if not _summary_is_secret_safe(summary):
        raise AssertionError("notification delivery lock summary is not secret-safe")
    return summary


def _summary_is_secret_safe(summary: Mapping[str, Any]) -> bool:
    rendered = json.dumps(summary, sort_keys=True, allow_nan=False)
    forbidden = ("postgresql://", "password", "secret", "dsn")
    return not any(value in rendered.casefold() for value in forbidden)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise real PostgreSQL contention and release for the global "
            "portfolio risk notification delivery lock."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_contract_check(args.dsn)
    except (ValidationError, StorageError, AssertionError) as exc:
        print(f"Notification delivery lock contract failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Notification delivery lock contract failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
