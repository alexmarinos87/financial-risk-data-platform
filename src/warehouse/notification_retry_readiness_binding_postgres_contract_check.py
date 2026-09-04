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
from src.warehouse.notification_execution_readiness_enforcement import (
    _enforcement_evidence,
)
from src.warehouse.notification_execution_readiness_gate import (
    evaluate_notification_execution_readiness,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    build_notification_execution_readiness_record,
)
from src.warehouse.notification_execution_readiness_recorder import (
    record_notification_execution_readiness,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
)
from src.warehouse.notification_retry_execution_recorder import (
    record_notification_retry_execution,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
)
from src.warehouse.notification_retry_readiness_binding_reader import (
    read_notification_retry_readiness_binding,
)
from src.warehouse.notification_retry_readiness_binding_recorder import (
    record_notification_retry_readiness_binding,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path("sql/notification_retry_readiness_binding_consistency_checks.sql")
BASE_TIME = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)
DESTINATION_ID = "binding-history-webhook"
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
LOCK_KEY = "binding-history-lock-key"


def _destination() -> NotificationDestination:
    return NotificationDestination(
        destination_id=DESTINATION_ID,
        channel="webhook",
        endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
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
            change_request_id="CHG-BINDING-HISTORY",
            reviewed_by=("risk-control-reviewer",),
            reviewed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            review_expires_at=datetime(2027, 1, 1, tzinfo=timezone.utc),
        ),
    )


def _decision(*, evaluated_at: datetime) -> dict[str, Any]:
    destination = _destination()
    return evaluate_notification_execution_readiness(
        execution_kind="retry",
        evaluated_at=evaluated_at,
        delivery_config=WebhookDeliveryConfig(
            enabled=True,
            endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL",
            timeout_seconds=5,
            max_batch_events=25,
            max_attempts_per_event=3,
            initial_backoff_seconds=1,
        ),
        retry_policy_fingerprint="binding-history-retry-policy",
        retry_execution_policy=RetryExecutionPolicy(
            enabled=True,
            max_plan_age_seconds=3600,
            max_events=25,
        ),
        destination=destination,
        activation_review={
            "authority_id": "binding-history-authority",
            "checklist_id": "binding-history-checklist",
            "destination_fingerprint": destination.fingerprint,
            "destination_id": DESTINATION_ID,
            "operational_activation_ready": True,
            "review_status": "ready",
        },
        transition_review={
            "activation_review_status": "ready",
            "current_authority_id": "binding-history-authority",
            "current_checklist_id": "binding-history-checklist",
            "current_destination_fingerprint": destination.fingerprint,
            "destination_id": DESTINATION_ID,
            "operational_activation_ready": True,
            "rollback_authority_id": None,
            "rollback_checklist_id": None,
            "rollback_destination_fingerprint": None,
            "rollback_endpoint_environment_variable": None,
            "rollback_plan_id": None,
            "transition_matches_current_activation": True,
            "transition_ready": True,
            "transition_record_id": "binding-history-transition-record",
            "transition_rehearsal_id": "binding-history-transition-rehearsal",
            "transition_review_status": "ready",
        },
        ambiguities=[],
    )


def _readiness_record(*, request_id: str) -> dict[str, Any]:
    decision = _decision(evaluated_at=BASE_TIME)
    return build_notification_execution_readiness_record(
        request_id=request_id,
        recorded_at=BASE_TIME + timedelta(seconds=1),
        decision=decision,
    )


def _terminal(
    *,
    request_id: str,
    plan_id: str,
    started_at: datetime,
    finished_at: datetime,
    recorded_at: datetime,
    requestful: bool,
) -> dict[str, Any]:
    if requestful:
        return build_retry_execution_record(
            request_id=request_id,
            plan_id=plan_id,
            started_at=started_at,
            finished_at=finished_at,
            recorded_at=recorded_at,
            terminal_status="failed_after_request",
            failure_code="validation_error",
            request_count=1,
            attempts_persisted=1,
            succeeded_count=0,
            failed_count=1,
            attempt_ids=[f"{request_id}-attempt"],
            requested_event_ids=[f"{request_id}-event"],
            persisted_event_ids=[f"{request_id}-event"],
            execution_summary=None,
            endpoint_host="alerts.example.test",
            delivery_fingerprint="binding-history-delivery",
            retry_policy_fingerprint="binding-history-retry-policy",
            retry_execution_policy_fingerprint="binding-history-execution-policy",
            lock_model_version=LOCK_MODEL,
            lock_key_fingerprint=LOCK_KEY,
            lock_acquired=True,
            lock_released=True,
        )
    return build_retry_execution_record(
        request_id=request_id,
        plan_id=plan_id,
        started_at=started_at,
        finished_at=finished_at,
        recorded_at=recorded_at,
        terminal_status="failed_before_request",
        failure_code="validation_error",
        request_count=0,
        attempts_persisted=0,
        succeeded_count=0,
        failed_count=0,
        attempt_ids=[],
        requested_event_ids=[],
        persisted_event_ids=[],
        execution_summary=None,
    )


def _enforcement(
    *,
    record: dict[str, Any],
    enforced_at: datetime,
    suffix: str,
) -> dict[str, Any]:
    decision = record["decision"]
    return _enforcement_evidence(
        destination_id=DESTINATION_ID,
        execution_kind="retry",
        enforced_at=enforced_at,
        record={
            "record_id": record["record_id"],
            "request_id": record["request_id"],
            "decision": {
                "decision_id": decision["decision_id"],
                "evaluated_at": decision["evaluated_at"],
            },
        },
        refreshed_decision={
            "decision_id": f"binding-history-refreshed-{suffix}",
            "evaluated_at": enforced_at.isoformat(),
        },
        lock={
            "key_fingerprint": LOCK_KEY,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
        },
    )


def _mutation_rejected(dsn: str, statement: str) -> bool:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc
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


def _status(dsn: str, terminal_record_id: str) -> str:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT readiness_binding_status
                FROM risk_platform.notification_retry_readiness_binding_status
                WHERE terminal_record_id = %s
                """,
                (terminal_record_id,),
            )
            row = cursor.fetchone()
    if row is None:
        raise AssertionError("retry readiness binding status row is missing")
    return str(row[0])


def run_contract_check(dsn: str) -> dict[str, Any]:
    readiness_record = _readiness_record(
        request_id="BINDING-HISTORY-READINESS-001"
    )
    record_notification_execution_readiness(dsn=dsn, record=readiness_record)

    terminal = _terminal(
        request_id="BINDING-HISTORY-TERMINAL-001",
        plan_id="binding-history-plan-001",
        started_at=BASE_TIME + timedelta(minutes=1),
        finished_at=BASE_TIME + timedelta(minutes=3),
        recorded_at=BASE_TIME + timedelta(minutes=4),
        requestful=True,
    )
    record_notification_retry_execution(dsn=dsn, record=terminal)
    enforcement = _enforcement(
        record=readiness_record,
        enforced_at=BASE_TIME + timedelta(minutes=2),
        suffix="current",
    )
    binding = build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=enforcement,
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )
    created = record_notification_retry_readiness_binding(
        dsn=dsn,
        binding=binding,
    )
    if created["created"] is not True:
        existing = read_notification_retry_readiness_binding(
            dsn=dsn,
            terminal_record_id=terminal["record_id"],
        )
        if existing is None or existing["binding"] != binding:
            raise AssertionError("retry readiness binding was not retained")

    replay = record_notification_retry_readiness_binding(
        dsn=dsn,
        binding=binding,
    )
    if replay["created"] is not False:
        raise AssertionError("exact retry readiness binding replay did not converge")
    retained = read_notification_retry_readiness_binding(
        dsn=dsn,
        terminal_record_id=terminal["record_id"],
    )
    if retained is None or retained["binding"] != binding:
        raise AssertionError("retry readiness binding reader changed canonical evidence")
    if _status(dsn, terminal["record_id"]) != "bound":
        raise AssertionError("retained retry readiness binding was not classified bound")

    conflicting = build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=enforcement,
        recorded_at=BASE_TIME + timedelta(minutes=6),
    )
    try:
        record_notification_retry_readiness_binding(
            dsn=dsn,
            binding=conflicting,
        )
    except ValidationError:
        conflict_rejected = True
    else:
        conflict_rejected = False
    if not conflict_rejected:
        raise AssertionError("conflicting terminal readiness evidence was accepted")

    legacy_terminal = _terminal(
        request_id="BINDING-HISTORY-LEGACY-001",
        plan_id="binding-history-plan-legacy",
        started_at=BASE_TIME + timedelta(minutes=10),
        finished_at=BASE_TIME + timedelta(minutes=10, seconds=1),
        recorded_at=BASE_TIME + timedelta(minutes=10, seconds=2),
        requestful=False,
    )
    record_notification_retry_execution(dsn=dsn, record=legacy_terminal)
    if _status(dsn, legacy_terminal["record_id"]) != "binding_missing":
        raise AssertionError("legacy terminal history was not visibly unbound")

    unretained_readiness = _readiness_record(
        request_id="BINDING-HISTORY-UNRETAINED-READINESS"
    )
    missing_source_terminal = _terminal(
        request_id="BINDING-HISTORY-MISSING-SOURCE-TERMINAL",
        plan_id="binding-history-plan-missing-source",
        started_at=BASE_TIME + timedelta(minutes=1, seconds=30),
        finished_at=BASE_TIME + timedelta(minutes=2, seconds=30),
        recorded_at=BASE_TIME + timedelta(minutes=4, seconds=30),
        requestful=False,
    )
    record_notification_retry_execution(dsn=dsn, record=missing_source_terminal)
    missing_source_binding = build_notification_retry_readiness_binding(
        terminal_record=missing_source_terminal,
        readiness_enforcement=_enforcement(
            record=unretained_readiness,
            enforced_at=BASE_TIME + timedelta(minutes=2),
            suffix="missing-source",
        ),
        recorded_at=BASE_TIME + timedelta(minutes=5),
    )
    try:
        record_notification_retry_readiness_binding(
            dsn=dsn,
            binding=missing_source_binding,
        )
    except ValidationError:
        missing_source_rejected = True
    else:
        missing_source_rejected = False
    if not missing_source_rejected:
        raise AssertionError("missing readiness source was accepted")

    update_rejected = _mutation_rejected(
        dsn,
        """
        UPDATE risk_platform.notification_retry_readiness_bindings
        SET destination_id = 'changed-destination'
        WHERE terminal_record_id = (
            SELECT record_id
            FROM risk_platform.portfolio_risk_notification_retry_executions
            WHERE request_id = 'BINDING-HISTORY-TERMINAL-001'
        )
        """,
    )
    delete_rejected = _mutation_rejected(
        dsn,
        """
        DELETE FROM risk_platform.notification_retry_readiness_bindings
        WHERE terminal_record_id = (
            SELECT record_id
            FROM risk_platform.portfolio_risk_notification_retry_executions
            WHERE request_id = 'BINDING-HISTORY-TERMINAL-001'
        )
        """,
    )
    if not update_rejected or not delete_rejected:
        raise AssertionError("append-only retry readiness mutation was accepted")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification retry readiness history requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("retry readiness binding reconciliation failed: " + names)

    return {
        "model_version": "portfolio-risk-notification-retry-readiness-history-v1",
        "binding_id": binding["binding_id"],
        "terminal_record_id": terminal["record_id"],
        "readiness_record_id": readiness_record["record_id"],
        "exact_replay_converged": True,
        "conflicting_terminal_binding_rejected": conflict_rejected,
        "missing_readiness_source_rejected": missing_source_rejected,
        "legacy_binding_missing_visible": True,
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
            "Exercise append-only notification retry readiness binding history "
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
        print(f"Retry readiness binding history failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
