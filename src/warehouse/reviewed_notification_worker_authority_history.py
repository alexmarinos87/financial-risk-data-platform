"""Configuration-checked admission to the existing append-only worker ledger."""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.reviewed_notification_worker_authority import (
    validate_reviewed_worker_authority_transition,
)
from src.warehouse.notification_worker_authority_history import (
    _validated,
    load_worker_authority,
    record_worker_authority,
)


def prepare_reviewed_worker_authority(
    *, transition: Mapping[str, Any], previous: Mapping[str, Any] | None = None,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Validate immutable reviewed snapshots without connecting to PostgreSQL."""
    document = _validated(transition)
    prior = None if previous is None else _validated(previous)
    return validate_reviewed_worker_authority_transition(
        document, previous=prior, worker_config_path=worker_config_path,
        delivery_config_path=delivery_config_path,
        destination_config_path=destination_config_path,
    )


def record_reviewed_worker_authority(
    *, dsn: str, transition: Mapping[str, Any],
    previous: Mapping[str, Any] | None = None,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Check configuration first; storage independently reconciles its locked head.

    Reviewed configuration paths must be immutable trusted snapshots. This API
    does not authenticate a caller or turn the ledger into runtime permission.
    """
    document = prepare_reviewed_worker_authority(
        transition=transition, previous=previous,
        worker_config_path=worker_config_path, delivery_config_path=delivery_config_path,
        destination_config_path=destination_config_path,
    )
    return record_worker_authority(dsn=dsn, transition=document)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate configuration-bound authority; record only when explicitly selected."
    )
    parser.add_argument("--transition", required=True, type=Path)
    parser.add_argument("--previous", type=Path)
    parser.add_argument("--worker-config", type=Path, default=Path("config/notification_workers.yaml"))
    parser.add_argument("--delivery-config", type=Path, default=Path("config/notification_delivery.yaml"))
    parser.add_argument("--destination-config", type=Path, default=Path("config/notification_destinations.yaml"))
    parser.add_argument("--record", action="store_true")
    args = parser.parse_args(argv)
    try:
        document = prepare_reviewed_worker_authority(
            transition=load_worker_authority(args.transition),
            previous=None if args.previous is None else load_worker_authority(args.previous),
            worker_config_path=args.worker_config, delivery_config_path=args.delivery_config,
            destination_config_path=args.destination_config,
        )
        if args.record:
            dsn = os.environ.get("WAREHOUSE_POSTGRES_DSN", "")
            if not dsn.strip():
                raise ValidationError("WAREHOUSE_POSTGRES_DSN is required for recording")
            result = record_worker_authority(dsn=dsn, transition=document)
        else:
            result = {
                "transition_id": document["transition_id"],
                "configuration_validated": True, "persisted": False,
                "runtime_permission_granted": False,
            }
    except (ValidationError, StorageError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
