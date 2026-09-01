from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

from src.common.exceptions import ValidationError
from src.orchestration.controlled_notification_receiver import (
    ControlledNotificationReceiver,
)
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    build_notification_activation_checklist,
)
from src.orchestration.notification_destination_transition_plan import (
    build_notification_destination_transition_plan,
)
from src.orchestration.notification_destination_transition_rehearsal import (
    rehearse_notification_destination_transition,
)
from src.orchestration.portfolio_risk_notification_destination_authority import (
    resolve_notification_destination_authority,
)
from src.warehouse.controlled_receiver_rehearsal_contract import (
    build_controlled_receiver_rehearsal_record,
)
from src.warehouse.controlled_receiver_rehearsal_recorder import (
    record_controlled_receiver_rehearsal,
)
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    build_notification_destination_transition_rehearsal_record,
)
from src.warehouse.notification_destination_transition_rehearsal_recorder import (
    record_notification_destination_transition_rehearsal,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path(
    "sql/notification_destination_transition_rehearsal_consistency_checks.sql"
)
DESTINATION_ID = "risk-operations-webhook"
OLD_ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL"
NEW_ENDPOINT_ENV = "RISK_NOTIFICATION_WEBHOOK_URL_V2"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _activation(
    *,
    enabled: bool,
    change_request_id: str,
    reviewed_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    if not enabled:
        return {
            "enabled": False,
            "change_request_id": None,
            "reviewed_by": [],
            "reviewed_at": None,
            "review_expires_at": None,
        }
    return {
        "enabled": True,
        "change_request_id": change_request_id,
        "reviewed_by": ["transition-contract-reviewer"],
        "reviewed_at": _iso(reviewed_at),
        "review_expires_at": _iso(expires_at),
    }


def _destination_config(
    *,
    endpoint_env: str,
    enabled: bool,
    change_request_id: str,
    reviewed_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    return {
        "model_version": "portfolio-risk-notification-destination-v1",
        "destinations": {
            DESTINATION_ID: {
                "channel": "webhook",
                "endpoint_env": endpoint_env,
                "owner": {
                    "team": "risk-operations",
                    "contact": "risk-operations-oncall",
                },
                "purpose": "portfolio-risk-breach-lifecycle",
                "recipient_scope": "risk-operations",
                "data_classification": "internal",
                "allowed_event_types": [
                    "breach_escalated",
                    "breach_opened",
                    "breach_resolved",
                ],
                "activation": _activation(
                    enabled=enabled,
                    change_request_id=change_request_id,
                    reviewed_at=reviewed_at,
                    expires_at=expires_at,
                ),
            }
        },
    }


def _write_config(root: Path, name: str, value: dict[str, Any]) -> Path:
    path = root / name
    path.write_text(yaml.safe_dump(value, sort_keys=False), encoding="utf-8")
    return path


def _clock(*values: datetime):
    iterator = iter(values)
    return lambda: next(iterator)


def _payload_bytes(event_id: str) -> bytes:
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "breach_opened",
            "payload": {"severity": "critical"},
            "policy_id": "us-tech-standard",
            "portfolio_id": "us-tech-equal",
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _request(endpoint: str, event_id: str) -> dict[str, Any]:
    return {
        "endpoint": endpoint,
        "payload": json.loads(_payload_bytes(event_id).decode("utf-8")),
    }


def _checklist(
    *,
    destination_id: str,
    fingerprint: str,
    authority_id: str,
    reviewed_at: datetime,
    expires_at: datetime,
) -> dict[str, Any]:
    return build_notification_activation_checklist(
        destination_id=destination_id,
        destination_fingerprint=fingerprint,
        authority_id=authority_id,
        reviewed_by=["receiver-owner", "transition-contract-reviewer"],
        reviewed_at=reviewed_at,
        review_expires_at=expires_at,
        controls={name: True for name in CONTROL_NAMES},
    )


def _controlled_receiver_record(
    *,
    checklist: dict[str, Any],
    request_id: str,
    event_id: str,
    host: str,
    started_at: datetime,
) -> dict[str, Any]:
    received_at = started_at + timedelta(seconds=1)
    receiver = ControlledNotificationReceiver(
        activation_checklist=checklist,
        allowed_hosts=[host],
        allowed_event_types=["breach_opened"],
        clock=_clock(received_at),
    )
    receiver(
        f"https://{host}/controlled",
        _payload_bytes(event_id),
        {
            "Content-Type": "application/json",
            "Idempotency-Key": event_id,
            "User-Agent": "financial-risk-data-platform/1",
        },
        5.0,
    )
    return build_controlled_receiver_rehearsal_record(
        request_id=request_id,
        terminal_status="completed",
        failure_code=None,
        activation_checklist=checklist,
        allowed_hosts=[host],
        allowed_event_types=["breach_opened"],
        response_status=204,
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=2),
        recorded_at=started_at + timedelta(seconds=3),
        attempted_request_count=1,
        receiver_summary=receiver.summary(),
    )


def _transition_evidence(now: datetime) -> tuple[dict[str, Any], dict[str, Any]]:
    planned_at = now - timedelta(hours=4)
    started_at = now - timedelta(hours=3)
    expires_at = now + timedelta(days=30)
    with tempfile.TemporaryDirectory(prefix="transition-contract-") as directory:
        root = Path(directory)
        baseline_path = _write_config(
            root,
            "baseline.yaml",
            _destination_config(
                endpoint_env=OLD_ENDPOINT_ENV,
                enabled=True,
                change_request_id="CHG-CONTRACT-BASELINE",
                reviewed_at=planned_at - timedelta(days=30),
                expires_at=expires_at,
            ),
        )
        rotated_path = _write_config(
            root,
            "rotated.yaml",
            _destination_config(
                endpoint_env=NEW_ENDPOINT_ENV,
                enabled=True,
                change_request_id="CHG-CONTRACT-ROTATE",
                reviewed_at=planned_at - timedelta(days=10),
                expires_at=expires_at,
            ),
        )
        disabled_path = _write_config(
            root,
            "disabled.yaml",
            _destination_config(
                endpoint_env=NEW_ENDPOINT_ENV,
                enabled=False,
                change_request_id="unused",
                reviewed_at=planned_at,
                expires_at=expires_at,
            ),
        )
        rollback_path = _write_config(
            root,
            "rollback.yaml",
            _destination_config(
                endpoint_env=OLD_ENDPOINT_ENV,
                enabled=True,
                change_request_id="CHG-CONTRACT-ROLLBACK",
                reviewed_at=planned_at - timedelta(days=1),
                expires_at=expires_at,
            ),
        )
        rotate_plan = build_notification_destination_transition_plan(
            operation="rotate",
            current_config_path=baseline_path,
            target_config_path=rotated_path,
            destination_id=DESTINATION_ID,
            planned_at=planned_at,
        )
        disable_plan = build_notification_destination_transition_plan(
            operation="disable",
            current_config_path=rotated_path,
            target_config_path=disabled_path,
            destination_id=DESTINATION_ID,
            planned_at=planned_at,
        )
        rollback_plan = build_notification_destination_transition_plan(
            operation="rollback",
            current_config_path=disabled_path,
            target_config_path=rollback_path,
            destination_id=DESTINATION_ID,
            planned_at=planned_at,
            prior_plan_id=disable_plan["plan_id"],
        )
        baseline_authority = resolve_notification_destination_authority(
            destination_config_path=baseline_path,
            destination_id=DESTINATION_ID,
            delivery_endpoint_env=OLD_ENDPOINT_ENV,
            evaluated_at=planned_at,
            event_types=["breach_opened"],
        )
        rotate_authority = resolve_notification_destination_authority(
            destination_config_path=rotated_path,
            destination_id=DESTINATION_ID,
            delivery_endpoint_env=NEW_ENDPOINT_ENV,
            evaluated_at=planned_at,
            event_types=["breach_opened"],
        )
        rollback_authority = resolve_notification_destination_authority(
            destination_config_path=rollback_path,
            destination_id=DESTINATION_ID,
            delivery_endpoint_env=OLD_ENDPOINT_ENV,
            evaluated_at=planned_at,
            event_types=["breach_opened"],
        )

    rotate_checklist = _checklist(
        destination_id=DESTINATION_ID,
        fingerprint=rotate_authority["destination_fingerprint"],
        authority_id=rotate_authority["authority_id"],
        reviewed_at=planned_at - timedelta(hours=2),
        expires_at=expires_at,
    )
    rollback_checklist = _checklist(
        destination_id=DESTINATION_ID,
        fingerprint=rollback_authority["destination_fingerprint"],
        authority_id=rollback_authority["authority_id"],
        reviewed_at=planned_at - timedelta(hours=1),
        expires_at=expires_at,
    )
    rotate_request = _request(
        "https://receiver-v2.test/controlled",
        "transition-contract-rotate",
    )
    rollback_request = _request(
        "https://receiver-v1.test/controlled",
        "transition-contract-rollback",
    )
    rehearsal = rehearse_notification_destination_transition(
        rotate_plan=rotate_plan,
        disable_plan=disable_plan,
        rollback_plan=rollback_plan,
        baseline_authority=baseline_authority,
        rotate_authority=rotate_authority,
        rollback_authority=rollback_authority,
        rotate_checklist=rotate_checklist,
        rollback_checklist=rollback_checklist,
        rotate_allowed_hosts=["receiver-v2.test"],
        rollback_allowed_hosts=["receiver-v1.test"],
        rotate_requests=[rotate_request, rotate_request],
        rollback_requests=[rollback_request],
        started_at=started_at,
        clock=_clock(
            started_at + timedelta(seconds=1),
            started_at + timedelta(seconds=2),
            started_at + timedelta(seconds=3),
        ),
    )
    return rehearsal, rollback_checklist


def _mutation_rejected(dsn: str, statement: str) -> bool:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Transition rehearsal contract requires psycopg") from exc
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


def _review_status(dsn: str) -> str:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Transition rehearsal contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT transition_review_status
                FROM risk_platform.current_notification_destination_transition_review
                WHERE destination_id = %s
                """,
                (DESTINATION_ID,),
            )
            row = cursor.fetchone()
    if row is None:
        raise AssertionError("transition review row was not produced")
    return str(row[0])


def run_contract_check(dsn: str) -> dict[str, Any]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rehearsal, rollback_checklist = _transition_evidence(now)
    transition_record = build_notification_destination_transition_rehearsal_record(
        request_id="TRANSITION-CONTRACT-001",
        recorded_at=now - timedelta(hours=2, minutes=59, seconds=56),
        rehearsal=rehearsal,
    )

    rollback_receiver_record = _controlled_receiver_record(
        checklist=rollback_checklist,
        request_id="TRANSITION-CONTRACT-CURRENT-RECEIVER",
        event_id="transition-current-receiver",
        host="receiver-v1.test",
        started_at=now - timedelta(hours=2),
    )
    record_controlled_receiver_rehearsal(
        dsn=dsn,
        record=rollback_receiver_record,
    )
    created = record_notification_destination_transition_rehearsal(
        dsn=dsn,
        record=transition_record,
    )
    if created["created"] is not True:
        raise AssertionError("transition rehearsal was not created")
    replay = record_notification_destination_transition_rehearsal(
        dsn=dsn,
        record=transition_record,
    )
    if replay["created"] is not False:
        raise AssertionError("exact transition rehearsal retry did not converge")
    if _review_status(dsn) != "ready":
        raise AssertionError("current transition rehearsal was not ready")

    conflicting = build_notification_destination_transition_rehearsal_record(
        request_id=transition_record["request_id"],
        recorded_at=(
            datetime.fromisoformat(transition_record["recorded_at"])
            + timedelta(seconds=1)
        ),
        rehearsal=rehearsal,
    )
    try:
        record_notification_destination_transition_rehearsal(
            dsn=dsn,
            record=conflicting,
        )
    except ValidationError:
        conflict_rejected = True
    else:
        conflict_rejected = False
    if not conflict_rejected:
        raise AssertionError("conflicting transition request identity was accepted")

    replacement_checklist = _checklist(
        destination_id=DESTINATION_ID,
        fingerprint="post-transition-destination-fingerprint",
        authority_id="post-transition-destination-authority",
        reviewed_at=now - timedelta(minutes=30),
        expires_at=now + timedelta(days=30),
    )
    replacement_receiver_record = _controlled_receiver_record(
        checklist=replacement_checklist,
        request_id="TRANSITION-CONTRACT-REPLACEMENT-RECEIVER",
        event_id="transition-replacement-receiver",
        host="receiver-v3.test",
        started_at=now - timedelta(minutes=20),
    )
    record_controlled_receiver_rehearsal(
        dsn=dsn,
        record=replacement_receiver_record,
    )
    if _review_status(dsn) != "transition_rehearsal_superseded":
        raise AssertionError("new receiver review did not supersede transition evidence")

    update_rejected = _mutation_rejected(
        dsn,
        """
        UPDATE risk_platform.notification_destination_transition_rehearsals
        SET rotate_request_count = rotate_request_count + 1
        WHERE request_id = 'TRANSITION-CONTRACT-001'
        """,
    )
    delete_rejected = _mutation_rejected(
        dsn,
        """
        DELETE FROM risk_platform.notification_destination_transition_rehearsals
        WHERE request_id = 'TRANSITION-CONTRACT-001'
        """,
    )
    if not update_rejected or not delete_rejected:
        raise AssertionError("append-only transition mutation was accepted")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Transition rehearsal contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("transition rehearsal reconciliation failed: " + names)

    return {
        "model_version": "portfolio-risk-notification-transition-contract-v1",
        "record_id": transition_record["record_id"],
        "exact_retry_converged": True,
        "conflicting_request_rejected": True,
        "initial_review_status": "ready",
        "current_review_status": "transition_rehearsal_superseded",
        "update_rejected": update_rejected,
        "delete_rejected": delete_rejected,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "socket_opened": False,
        "dns_lookup_performed": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise append-only destination transition rehearsal history "
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
        print(f"Destination transition contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
