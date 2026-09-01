from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    DestinationActivation,
    DestinationOwner,
    NotificationDestination,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    RetryExecutionPolicy,
)
from src.warehouse.controlled_receiver_rehearsal_recorder import (
    record_controlled_receiver_rehearsal,
)
from src.warehouse.notification_destination_transition_rehearsal_contract import (
    build_notification_destination_transition_rehearsal_record,
)
from src.warehouse.notification_destination_transition_rehearsal_postgres_contract_check import (
    DESTINATION_ID,
    OLD_ENDPOINT_ENV,
    _checklist,
    _controlled_receiver_record,
    _transition_evidence,
)
from src.warehouse.notification_destination_transition_rehearsal_recorder import (
    record_notification_destination_transition_rehearsal,
)
from src.warehouse.notification_execution_readiness_gate import (
    evaluate_notification_execution_readiness,
    read_notification_execution_readiness_evidence,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    build_notification_execution_readiness_record,
)
from src.warehouse.notification_execution_readiness_recorder import (
    record_notification_execution_readiness,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path("sql/notification_execution_readiness_consistency_checks.sql")


def _destination(now: datetime) -> NotificationDestination:
    planned_at = now - timedelta(hours=4)
    return NotificationDestination(
        destination_id=DESTINATION_ID,
        channel="webhook",
        endpoint_env=OLD_ENDPOINT_ENV,
        owner=DestinationOwner(
            team="risk-operations",
            contact="risk-operations-oncall",
        ),
        purpose="portfolio-risk-breach-lifecycle",
        recipient_scope="risk-operations",
        data_classification="internal",
        allowed_event_types=(
            "breach_escalated",
            "breach_opened",
            "breach_resolved",
        ),
        activation=DestinationActivation(
            enabled=True,
            change_request_id="CHG-CONTRACT-ROLLBACK",
            reviewed_by=("transition-contract-reviewer",),
            reviewed_at=planned_at - timedelta(days=1),
            review_expires_at=now + timedelta(days=30),
        ),
    )


def _evidence(dsn: str) -> dict[str, Any]:
    evidence = dict(
        read_notification_execution_readiness_evidence(
            dsn=dsn,
            destination_id=DESTINATION_ID,
        )
    )
    evidence["ambiguities"] = []
    return evidence


def _decision(
    *,
    now: datetime,
    evaluated_at: datetime,
    execution_kind: str,
    retry_enabled: bool,
    destination: NotificationDestination,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return evaluate_notification_execution_readiness(
        execution_kind=execution_kind,
        evaluated_at=evaluated_at,
        delivery_config=WebhookDeliveryConfig(
            enabled=True,
            endpoint_env=OLD_ENDPOINT_ENV,
            timeout_seconds=5,
            max_batch_events=25,
            max_attempts_per_event=3,
            initial_backoff_seconds=1,
        ),
        retry_policy_fingerprint="readiness-contract-retry-policy",
        retry_execution_policy=RetryExecutionPolicy(
            enabled=retry_enabled,
            max_plan_age_seconds=3600,
            max_events=25,
        ),
        destination=destination,
        activation_review=evidence["activation_review"],
        transition_review=evidence["transition_review"],
        ambiguities=evidence["ambiguities"],
    )


def _review_status(dsn: str, execution_kind: str) -> str:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT readiness_review_status
                FROM risk_platform.current_notification_execution_readiness_review
                WHERE destination_id = %s AND execution_kind = %s
                """,
                (DESTINATION_ID, execution_kind),
            )
            row = cursor.fetchone()
    if row is None:
        raise AssertionError("notification readiness review row was not produced")
    return str(row[0])


def _mutation_rejected(dsn: str, statement: str) -> bool:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness contract requires psycopg") from exc
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
    now = datetime.now(timezone.utc).replace(microsecond=0)
    rehearsal, rollback_checklist = _transition_evidence(now)
    transition_record = build_notification_destination_transition_rehearsal_record(
        request_id="READINESS-HISTORY-TRANSITION-001",
        recorded_at=now - timedelta(hours=2, minutes=59, seconds=56),
        rehearsal=rehearsal,
    )
    rollback_receiver = _controlled_receiver_record(
        checklist=rollback_checklist,
        request_id="READINESS-HISTORY-CURRENT-RECEIVER",
        event_id="readiness-history-current-receiver",
        host="readiness-receiver-v1.test",
        started_at=now - timedelta(hours=2),
    )
    record_controlled_receiver_rehearsal(dsn=dsn, record=rollback_receiver)
    record_notification_destination_transition_rehearsal(
        dsn=dsn,
        record=transition_record,
    )

    destination = _destination(now)
    if destination.fingerprint != rollback_checklist["destination_fingerprint"]:
        raise AssertionError("readiness destination fingerprint does not match transition")
    evidence = _evidence(dsn)
    transition = evidence["transition_review"]
    if transition is None or transition["transition_review_status"] != "ready":
        raise AssertionError("readiness fixture transition did not become ready")

    stale_decision = _decision(
        now=now,
        evaluated_at=now - timedelta(minutes=10),
        execution_kind="initial",
        retry_enabled=True,
        destination=destination,
        evidence=evidence,
    )
    stale_record = build_notification_execution_readiness_record(
        request_id="READINESS-HISTORY-STALE-INITIAL",
        recorded_at=now - timedelta(minutes=9, seconds=59),
        decision=stale_decision,
    )
    record_notification_execution_readiness(dsn=dsn, record=stale_record)
    if _review_status(dsn, "initial") != "decision_stale":
        raise AssertionError("old current readiness decision was not stale")

    current_decision = _decision(
        now=now,
        evaluated_at=now - timedelta(minutes=1),
        execution_kind="initial",
        retry_enabled=True,
        destination=destination,
        evidence=evidence,
    )
    current_record = build_notification_execution_readiness_record(
        request_id="READINESS-HISTORY-CURRENT-INITIAL",
        recorded_at=now - timedelta(seconds=59),
        decision=current_decision,
    )
    created = record_notification_execution_readiness(
        dsn=dsn,
        record=current_record,
    )
    if created["created"] is not True:
        raise AssertionError("current readiness decision was not retained")
    if _review_status(dsn, "initial") != "allowed":
        raise AssertionError("current initial readiness decision was not allowed")

    retry_decision = _decision(
        now=now,
        evaluated_at=now - timedelta(seconds=30),
        execution_kind="retry",
        retry_enabled=False,
        destination=destination,
        evidence=evidence,
    )
    retry_record = build_notification_execution_readiness_record(
        request_id="READINESS-HISTORY-CURRENT-RETRY",
        recorded_at=now - timedelta(seconds=29),
        decision=retry_decision,
    )
    record_notification_execution_readiness(dsn=dsn, record=retry_record)
    if _review_status(dsn, "retry") != "blocked":
        raise AssertionError("current retry readiness decision was not blocked")

    replay = record_notification_execution_readiness(
        dsn=dsn,
        record=current_record,
    )
    if replay["created"] is not False:
        raise AssertionError("exact readiness decision retry did not converge")

    conflicting = build_notification_execution_readiness_record(
        request_id=current_record["request_id"],
        recorded_at=datetime.fromisoformat(current_record["recorded_at"])
        + timedelta(seconds=1),
        decision=current_decision,
    )
    try:
        record_notification_execution_readiness(dsn=dsn, record=conflicting)
    except ValidationError:
        conflict_rejected = True
    else:
        conflict_rejected = False
    if not conflict_rejected:
        raise AssertionError("conflicting readiness request identity was accepted")

    replacement_checklist = _checklist(
        destination_id=DESTINATION_ID,
        fingerprint="readiness-post-transition-fingerprint",
        authority_id="readiness-post-transition-authority",
        reviewed_at=now - timedelta(minutes=20),
        expires_at=now + timedelta(days=30),
    )
    replacement_receiver = _controlled_receiver_record(
        checklist=replacement_checklist,
        request_id="READINESS-HISTORY-REPLACEMENT-RECEIVER",
        event_id="readiness-history-replacement-receiver",
        host="readiness-receiver-v2.test",
        started_at=now - timedelta(minutes=10),
    )
    record_controlled_receiver_rehearsal(dsn=dsn, record=replacement_receiver)
    if _review_status(dsn, "initial") != "decision_superseded":
        raise AssertionError("new activation evidence did not supersede initial decision")
    if _review_status(dsn, "retry") != "decision_superseded":
        raise AssertionError("new activation evidence did not supersede retry decision")

    update_rejected = _mutation_rejected(
        dsn,
        """
        UPDATE risk_platform.notification_execution_readiness_decisions
        SET decision = 'block'
        WHERE request_id = 'READINESS-HISTORY-CURRENT-INITIAL'
        """,
    )
    delete_rejected = _mutation_rejected(
        dsn,
        """
        DELETE FROM risk_platform.notification_execution_readiness_decisions
        WHERE request_id = 'READINESS-HISTORY-CURRENT-INITIAL'
        """,
    )
    if not update_rejected or not delete_rejected:
        raise AssertionError("append-only readiness mutation was accepted")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness contract requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("notification readiness reconciliation failed: " + names)

    return {
        "model_version": "portfolio-risk-notification-readiness-history-contract-v1",
        "current_initial_decision_id": current_decision["decision_id"],
        "current_retry_decision_id": retry_decision["decision_id"],
        "stale_status_proved": True,
        "allowed_status_proved": True,
        "blocked_status_proved": True,
        "superseded_status_proved": True,
        "exact_retry_converged": True,
        "conflicting_request_rejected": True,
        "update_rejected": update_rejected,
        "delete_rejected": delete_rejected,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise append-only notification execution readiness history "
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
        print(f"Notification readiness history contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
