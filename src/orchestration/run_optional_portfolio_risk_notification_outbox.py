from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_risk_notification_outbox import MAX_NOTIFICATION_EVENTS
from ..common.exceptions import StorageError, ValidationError
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN
from .run_portfolio_risk_notification_outbox import (
    _calendar_date,
    _max_events,
    _write_summary,
    read_actionable_transitions,
    run_portfolio_risk_notification_outbox,
)

NO_TRANSITIONS_MESSAGE = (
    "no actionable risk-limit transitions matched the requested range"
)


def run_optional_portfolio_risk_notification_outbox(
    *,
    policy_id: str,
    start_date: date | None,
    end_date: date,
    max_events: int,
    dsn: str,
    storage_config_path: Path,
) -> dict[str, Any]:
    try:
        records = read_actionable_transitions(
            dsn=dsn,
            policy_id=policy_id,
            start_date=start_date,
            end_date=end_date,
            max_events=max_events,
        )
    except ValidationError as exc:
        if str(exc) != NO_TRANSITIONS_MESSAGE:
            raise
        return {
            "run_id": str(uuid4()),
            "policy_id": policy_id,
            "selection": {
                "start_date": start_date.isoformat() if start_date else None,
                "end_date": end_date.isoformat(),
                "actionable_transitions": 0,
            },
            "parameters": {"max_events": max_events},
            "curated_output": {
                "portfolio_risk_notification_outbox": {
                    "records_selected": 0,
                    "records_written": 0,
                    "records_already_present": 0,
                    "pending_delivery_candidates": 0,
                    "suppressed_candidates": 0,
                }
            },
            "delivery": {
                "performed": False,
                "external_destinations": 0,
                "reason": "no_actionable_transitions",
            },
            "latest_event": None,
        }

    return run_portfolio_risk_notification_outbox(
        policy_id=policy_id,
        start_date=start_date,
        end_date=end_date,
        max_events=max_events,
        dsn=dsn,
        storage_config_path=storage_config_path,
        reader=lambda **_: records,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build notification-outbox evidence when actionable transitions "
            "exist and otherwise return a successful no-op summary."
        )
    )
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--max-events",
        type=_max_events,
        default=MAX_NOTIFICATION_EVENTS,
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_optional_portfolio_risk_notification_outbox(
            policy_id=args.policy_id,
            start_date=args.start_date,
            end_date=args.end_date,
            max_events=args.max_events,
            dsn=args.dsn,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Optional notification-outbox generation failed: input data or "
            "options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Optional notification-outbox generation failed: local storage or "
            "PostgreSQL operation failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Optional notification-outbox generation failed: unexpected local "
            "failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
