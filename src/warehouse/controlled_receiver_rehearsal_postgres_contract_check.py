from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.controlled_notification_receiver import (
    ControlledNotificationReceiver,
)
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    build_notification_activation_checklist,
)
from src.warehouse.controlled_receiver_rehearsal_contract import (
    build_controlled_receiver_rehearsal_record,
)
from src.warehouse.controlled_receiver_rehearsal_recorder import (
    record_controlled_receiver_rehearsal,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path(
    "sql/controlled_notification_receiver_rehearsal_consistency_checks.sql"
)
STARTED = datetime(2026, 8, 28, 10, tzinfo=timezone.utc)


def _checklist() -> dict[str, Any]:
    return build_notification_activation_checklist(
        destination_id="risk-operations-webhook",
        destination_fingerprint="contract-destination-fingerprint",
        authority_id="contract-destination-authority",
        reviewed_by=["contract-reviewer", "receiver-owner"],
        reviewed_at=STARTED - timedelta(days=1),
        review_expires_at=STARTED + timedelta(days=1),
        controls={name: True for name in CONTROL_NAMES},
    )


def _payload(*, severity: str = "critical") -> bytes:
    document = {
        "event_id": "contract-event-1",
        "event_type": "breach_opened",
        "metric_name": "portfolio_volatility_annualized",
        "payload": {"severity": severity},
        "policy_id": "us-tech-standard",
        "portfolio_id": "us-tech-equal",
        "status": severity,
        "subject_key": "us-tech-equal",
        "ts_event": "2026-08-28T09:00:00+00:00",
    }
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _headers() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Idempotency-Key": "contract-event-1",
        "User-Agent": "financial-risk-data-platform/1",
    }


def _clock(*values: datetime) -> Any:
    iterator = iter(values)
    return lambda: next(iterator)


def _completed_record() -> dict[str, Any]:
    receiver = ControlledNotificationReceiver(
        activation_checklist=_checklist(),
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        clock=_clock(
            STARTED + timedelta(seconds=1),
            STARTED + timedelta(seconds=2),
        ),
    )
    endpoint = "https://receiver.test/controlled"
    receiver(endpoint, _payload(), _headers(), 5.0)
    receiver(endpoint, _payload(), _headers(), 5.0)
    return build_controlled_receiver_rehearsal_record(
        request_id="CONTRACT-REHEARSAL-COMPLETED",
        terminal_status="completed",
        failure_code=None,
        activation_checklist=_checklist(),
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=STARTED,
        finished_at=STARTED + timedelta(seconds=3),
        recorded_at=STARTED + timedelta(seconds=4),
        attempted_request_count=2,
        receiver_summary=receiver.summary(),
    )


def _failed_record() -> dict[str, Any]:
    receiver = ControlledNotificationReceiver(
        activation_checklist=_checklist(),
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        clock=_clock(STARTED + timedelta(minutes=1, seconds=1)),
    )
    endpoint = "https://receiver.test/controlled"
    receiver(endpoint, _payload(), _headers(), 5.0)
    try:
        receiver(endpoint, _payload(severity="warning"), _headers(), 5.0)
    except ValidationError:
        pass
    else:  # pragma: no cover - receiver contract guarantees rejection.
        raise AssertionError("conflicting idempotency evidence was accepted")
    return build_controlled_receiver_rehearsal_record(
        request_id="CONTRACT-REHEARSAL-FAILED",
        terminal_status="failed_during_rehearsal",
        failure_code="validation_error",
        activation_checklist=_checklist(),
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=STARTED + timedelta(minutes=1),
        finished_at=STARTED + timedelta(minutes=1, seconds=3),
        recorded_at=STARTED + timedelta(minutes=1, seconds=4),
        attempted_request_count=2,
        receiver_summary=receiver.summary(),
    )


def _rejected_record() -> dict[str, Any]:
    return build_controlled_receiver_rehearsal_record(
        request_id="CONTRACT-REHEARSAL-REJECTED",
        terminal_status="rejected_before_request",
        failure_code="validation_error",
        activation_checklist=_checklist(),
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=STARTED + timedelta(minutes=2),
        finished_at=STARTED + timedelta(minutes=2),
        recorded_at=STARTED + timedelta(minutes=2, seconds=1),
        attempted_request_count=0,
        receiver_summary=None,
    )


def _mutation_rejected(dsn: str, statement: str) -> bool:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Controlled receiver contract requires psycopg") from exc
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement)
            connection.commit()
    except Exception as exc:
        if "append-only" not in str(exc):
            raise
        return True
    return False


def run_contract_check(dsn: str) -> dict[str, Any]:
    completed = _completed_record()
    failed = _failed_record()
    rejected = _rejected_record()
    created = [
        record_controlled_receiver_rehearsal(dsn=dsn, record=record)
        for record in (completed, failed, rejected)
    ]
    replay = record_controlled_receiver_rehearsal(dsn=dsn, record=completed)
    if replay["created"] is not False:
        raise AssertionError("exact controlled receiver retry did not converge")

    conflicting = build_controlled_receiver_rehearsal_record(
        request_id=completed["request_id"],
        terminal_status="completed",
        failure_code=None,
        activation_checklist=completed["activation_checklist"],
        allowed_hosts=completed["allowed_hosts"],
        allowed_event_types=completed["allowed_event_types"],
        response_status=completed["response_status"],
        started_at=completed["started_at"],
        finished_at=completed["finished_at"],
        recorded_at=(
            datetime.fromisoformat(completed["recorded_at"])
            + timedelta(seconds=1)
        ),
        attempted_request_count=completed["attempted_request_count"],
        receiver_summary=completed["receiver_summary"],
    )
    try:
        record_controlled_receiver_rehearsal(dsn=dsn, record=conflicting)
    except ValidationError:
        conflict_rejected = True
    else:
        conflict_rejected = False
    if not conflict_rejected:
        raise AssertionError("conflicting rehearsal request identity was accepted")

    update_rejected = _mutation_rejected(
        dsn,
        """
        UPDATE risk_platform.controlled_notification_receiver_rehearsals
        SET terminal_status = 'completed'
        WHERE request_id = 'CONTRACT-REHEARSAL-FAILED'
        """,
    )
    delete_rejected = _mutation_rejected(
        dsn,
        """
        DELETE FROM risk_platform.notification_activation_checklists
        WHERE checklist_id LIKE
            'portfolio-risk-notification-activation-checklist-v1-%'
        """,
    )
    if not update_rejected or not delete_rejected:
        raise AssertionError("append-only rehearsal mutation was accepted")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Controlled receiver contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("controlled receiver reconciliation failed: " + names)

    return {
        "model_version": "portfolio-risk-controlled-receiver-contract-v1",
        "terminal_records": len(created),
        "terminal_statuses": sorted(
            record["terminal_status"]
            for record in (completed, failed, rejected)
        ),
        "exact_retry_converged": True,
        "conflicting_request_rejected": True,
        "update_rejected": update_rejected,
        "delete_rejected": delete_rejected,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "payload_bodies_recorded": False,
        "response_bodies_recorded": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise append-only controlled receiver rehearsal history "
            "against PostgreSQL 16."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_contract_check(args.dsn)
    except Exception as exc:
        print(f"Controlled receiver contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
