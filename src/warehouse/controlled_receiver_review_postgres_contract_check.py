from __future__ import annotations

import argparse
import hashlib
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
    canonical_activation_checklist_bytes,
)
from src.warehouse.controlled_receiver_rehearsal_contract import (
    build_controlled_receiver_rehearsal_record,
)
from src.warehouse.controlled_receiver_rehearsal_recorder import (
    record_controlled_receiver_rehearsal,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path(
    "sql/controlled_notification_receiver_review_consistency_checks.sql"
)


def _controls(**overrides: bool) -> dict[str, bool]:
    values = {name: True for name in CONTROL_NAMES}
    values.update(overrides)
    return values


def _checklist(
    *,
    destination_id: str,
    reviewed_at: datetime,
    review_expires_at: datetime,
    controls: dict[str, bool] | None = None,
    version: str = "v1",
) -> dict[str, Any]:
    return build_notification_activation_checklist(
        destination_id=destination_id,
        destination_fingerprint=f"{destination_id}-fingerprint-{version}",
        authority_id=f"{destination_id}-authority-{version}",
        reviewed_by=["activation-reviewer", "receiver-owner"],
        reviewed_at=reviewed_at,
        review_expires_at=review_expires_at,
        controls=controls or _controls(),
    )


def _record_checklist(cursor: Any, checklist: dict[str, Any], jsonb: Any) -> None:
    digest = hashlib.sha256(
        canonical_activation_checklist_bytes(checklist)
    ).hexdigest()
    cursor.execute(
        """
        INSERT INTO risk_platform.notification_activation_checklists (
            checklist_id,
            model_version,
            destination_id,
            destination_fingerprint,
            authority_id,
            reviewed_at,
            review_expires_at,
            reviewed_by_json,
            controls_json,
            activation_ready,
            checklist_json,
            document_sha256
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        """,
        (
            checklist["checklist_id"],
            checklist["model_version"],
            checklist["destination_id"],
            checklist["destination_fingerprint"],
            checklist["authority_id"],
            checklist["reviewed_at"],
            checklist["review_expires_at"],
            jsonb(checklist["reviewed_by"]),
            jsonb(checklist["controls"]),
            checklist["activation_ready"],
            jsonb(checklist),
            digest,
        ),
    )


def _payload(event_id: str) -> bytes:
    document = {
        "event_id": event_id,
        "event_type": "breach_opened",
        "metric_name": "portfolio_volatility_annualized",
        "payload": {"severity": "critical"},
        "policy_id": "us-tech-standard",
        "portfolio_id": "us-tech-equal",
        "status": "critical",
        "subject_key": "us-tech-equal",
        "ts_event": "2026-08-28T09:00:00+00:00",
    }
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _headers(event_id: str) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Idempotency-Key": event_id,
        "User-Agent": "financial-risk-data-platform/1",
    }


def _clock(*values: datetime) -> Any:
    iterator = iter(values)
    return lambda: next(iterator)


def _completed_record(
    *,
    checklist: dict[str, Any],
    request_id: str,
    started_at: datetime,
) -> dict[str, Any]:
    receiver = ControlledNotificationReceiver(
        activation_checklist=checklist,
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        clock=_clock(started_at + timedelta(seconds=1)),
    )
    event_id = f"{request_id.casefold()}-event"
    receiver(
        "https://receiver.test/controlled",
        _payload(event_id),
        _headers(event_id),
        5.0,
    )
    return build_controlled_receiver_rehearsal_record(
        request_id=request_id,
        terminal_status="completed",
        failure_code=None,
        activation_checklist=checklist,
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=2),
        recorded_at=started_at + timedelta(seconds=3),
        attempted_request_count=1,
        receiver_summary=receiver.summary(),
    )


def _rejected_record(
    *,
    checklist: dict[str, Any],
    request_id: str,
    started_at: datetime,
) -> dict[str, Any]:
    return build_controlled_receiver_rehearsal_record(
        request_id=request_id,
        terminal_status="rejected_before_request",
        failure_code="validation_error",
        activation_checklist=checklist,
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=started_at,
        finished_at=started_at,
        recorded_at=started_at + timedelta(seconds=1),
        attempted_request_count=0,
        receiver_summary=None,
    )


def _failed_record(
    *,
    checklist: dict[str, Any],
    request_id: str,
    started_at: datetime,
) -> dict[str, Any]:
    receiver = ControlledNotificationReceiver(
        activation_checklist=checklist,
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        clock=_clock(started_at + timedelta(seconds=1)),
    )
    event_id = f"{request_id.casefold()}-event"
    endpoint = "https://receiver.test/controlled"
    headers = _headers(event_id)
    receiver(endpoint, _payload(event_id), headers, 5.0)
    changed = json.loads(_payload(event_id))
    changed["payload"]["severity"] = "warning"
    changed_payload = json.dumps(
        changed,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    try:
        receiver(endpoint, changed_payload, headers, 5.0)
    except ValidationError:
        pass
    else:  # pragma: no cover - receiver must reject conflicting content.
        raise AssertionError("controlled receiver accepted conflicting content")
    return build_controlled_receiver_rehearsal_record(
        request_id=request_id,
        terminal_status="failed_during_rehearsal",
        failure_code="validation_error",
        activation_checklist=checklist,
        allowed_hosts=["receiver.test"],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=3),
        recorded_at=started_at + timedelta(seconds=4),
        attempted_request_count=2,
        receiver_summary=receiver.summary(),
    )


def _insert_fixtures(dsn: str, now: datetime) -> None:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Controlled receiver review contract requires psycopg") from exc

    active_start = now - timedelta(hours=2)
    active_end = now + timedelta(hours=2)
    ready = _checklist(
        destination_id="review-ready",
        reviewed_at=active_start,
        review_expires_at=active_end,
    )
    rejected = _checklist(
        destination_id="review-rejected",
        reviewed_at=active_start,
        review_expires_at=active_end,
    )
    failed = _checklist(
        destination_id="review-failed",
        reviewed_at=active_start,
        review_expires_at=active_end,
    )
    old_superseded = _checklist(
        destination_id="review-superseded",
        reviewed_at=active_start,
        review_expires_at=active_end,
        version="old",
    )
    for record in (
        _completed_record(
            checklist=ready,
            request_id="REVIEW-READY",
            started_at=now - timedelta(minutes=30),
        ),
        _rejected_record(
            checklist=rejected,
            request_id="REVIEW-REJECTED",
            started_at=now - timedelta(minutes=25),
        ),
        _failed_record(
            checklist=failed,
            request_id="REVIEW-FAILED",
            started_at=now - timedelta(minutes=20),
        ),
        _completed_record(
            checklist=old_superseded,
            request_id="REVIEW-SUPERSEDED-OLD",
            started_at=now - timedelta(minutes=15),
        ),
    ):
        record_controlled_receiver_rehearsal(dsn=dsn, record=record)

    checklists = (
        _checklist(
            destination_id="review-incomplete",
            reviewed_at=active_start,
            review_expires_at=active_end,
            controls=_controls(rollback_tested=False),
        ),
        _checklist(
            destination_id="review-expired",
            reviewed_at=now - timedelta(days=2),
            review_expires_at=now - timedelta(days=1),
        ),
        _checklist(
            destination_id="review-missing",
            reviewed_at=active_start,
            review_expires_at=active_end,
        ),
        _checklist(
            destination_id="review-not-yet",
            reviewed_at=now + timedelta(hours=1),
            review_expires_at=now + timedelta(hours=2),
        ),
        _checklist(
            destination_id="review-superseded",
            reviewed_at=now - timedelta(minutes=5),
            review_expires_at=active_end,
            version="current",
        ),
    )
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            for value in checklists:
                _record_checklist(cursor, value, Jsonb)
        connection.commit()


def _assert_review_states(dsn: str) -> tuple[dict[str, str], int]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Controlled receiver review contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT destination_id, review_status
                FROM risk_platform.current_notification_activation_rehearsal_review
                WHERE destination_id LIKE 'review-%'
                ORDER BY destination_id
                """
            )
            actual = {str(destination): str(status) for destination, status in cursor}
            expected = {
                "review-expired": "checklist_expired",
                "review-failed": "rehearsal_failed",
                "review-incomplete": "checklist_incomplete",
                "review-missing": "rehearsal_missing",
                "review-not-yet": "checklist_not_yet_active",
                "review-ready": "ready",
                "review-rejected": "rehearsal_rejected",
                "review-superseded": "rehearsal_superseded",
            }
            if actual != expected:
                raise AssertionError(f"activation review states changed: {actual!r}")
            cursor.execute(
                """
                SELECT incomplete_controls_json
                FROM risk_platform.current_notification_activation_rehearsal_review
                WHERE destination_id = 'review-incomplete'
                """
            )
            row = cursor.fetchone()
            if row is None or row[0] != ["rollback_tested"]:
                raise AssertionError(f"incomplete controls changed: {row!r}")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM risk_platform.current_notification_activation_review_failures
                WHERE destination_id LIKE 'review-%'
                """
            )
            failure_row = cursor.fetchone()
            if failure_row is None or failure_row[0] != 7:
                raise AssertionError("activation review failure partition changed")
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM risk_platform.current_notification_activation_ready
                WHERE destination_id LIKE 'review-%'
                """
            )
            ready_row = cursor.fetchone()
            if ready_row is None or ready_row[0] != 1:
                raise AssertionError("activation-ready partition changed")
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("controlled receiver review reconciliation failed: " + names)
    return actual, len(checks)


def run_contract_check(dsn: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    _insert_fixtures(dsn, now)
    states, check_count = _assert_review_states(dsn)
    return {
        "model_version": "portfolio-risk-controlled-receiver-review-v1",
        "fixture_destinations": len(states),
        "review_states": states,
        "ready_rows": 1,
        "review_failure_rows": 7,
        "consistency_checks": check_count,
        "external_request_performed": False,
        "payload_bodies_recorded": False,
        "response_bodies_recorded": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise current notification activation and controlled receiver "
            "review views against PostgreSQL 16."
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
        print(f"Controlled receiver review contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
