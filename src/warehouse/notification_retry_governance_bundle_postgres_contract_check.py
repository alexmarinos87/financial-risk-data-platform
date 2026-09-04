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
from src.warehouse.notification_retry_governance_bundle_recorder import (
    record_notification_retry_governance_bundle,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path("sql/notification_retry_readiness_binding_consistency_checks.sql")
BASE_TIME = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
DESTINATION_ID = "atomic-bundle-webhook"
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
LOCK_KEY = "atomic-bundle-lock-key"


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
            change_request_id="CHG-ATOMIC-BUNDLE",
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
        retry_policy_fingerprint="atomic-bundle-retry-policy",
        retry_execution_policy=RetryExecutionPolicy(
            enabled=True,
            max_plan_age_seconds=3600,
            max_events=25,
        ),
        destination=destination,
        activation_review={
            "authority_id": "atomic-bundle-authority",
            "checklist_id": "atomic-bundle-checklist",
            "destination_fingerprint": destination.fingerprint,
            "destination_id": DESTINATION_ID,
            "operational_activation_ready": True,
            "review_status": "ready",
        },
        transition_review={
            "activation_review_status": "ready",
            "current_authority_id": "atomic-bundle-authority",
            "current_checklist_id": "atomic-bundle-checklist",
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
            "transition_record_id": "atomic-bundle-transition-record",
            "transition_rehearsal_id": "atomic-bundle-transition-rehearsal",
            "transition_review_status": "ready",
        },
        ambiguities=[],
    )


def _readiness_record(
    *,
    request_id: str,
    evaluated_at: datetime = BASE_TIME,
) -> dict[str, Any]:
    return build_notification_execution_readiness_record(
        request_id=request_id,
        recorded_at=evaluated_at + timedelta(seconds=1),
        decision=_decision(evaluated_at=evaluated_at),
    )


def _terminal(
    *,
    request_id: str,
    plan_id: str,
    offset_seconds: int = 0,
) -> dict[str, Any]:
    started_at = BASE_TIME + timedelta(minutes=1, seconds=offset_seconds)
    return build_retry_execution_record(
        request_id=request_id,
        plan_id=plan_id,
        started_at=started_at,
        finished_at=BASE_TIME + timedelta(minutes=3, seconds=offset_seconds),
        recorded_at=BASE_TIME + timedelta(minutes=4, seconds=offset_seconds),
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
        delivery_fingerprint="atomic-bundle-delivery",
        retry_policy_fingerprint="atomic-bundle-retry-policy",
        retry_execution_policy_fingerprint="atomic-bundle-execution-policy",
        lock_model_version=LOCK_MODEL,
        lock_key_fingerprint=LOCK_KEY,
        lock_acquired=True,
        lock_released=True,
    )


def _enforcement(
    *,
    readiness_record: dict[str, Any],
    suffix: str,
    offset_seconds: int = 0,
) -> dict[str, Any]:
    enforced_at = BASE_TIME + timedelta(minutes=2, seconds=offset_seconds)
    decision = readiness_record["decision"]
    return _enforcement_evidence(
        destination_id=DESTINATION_ID,
        execution_kind="retry",
        enforced_at=enforced_at,
        record={
            "record_id": readiness_record["record_id"],
            "request_id": readiness_record["request_id"],
            "decision": {
                "decision_id": decision["decision_id"],
                "evaluated_at": decision["evaluated_at"],
            },
        },
        refreshed_decision={
            "decision_id": f"atomic-bundle-refreshed-{suffix}",
            "evaluated_at": enforced_at.isoformat(),
        },
        lock={
            "key_fingerprint": LOCK_KEY,
            "model_version": LOCK_MODEL,
            "scope": LOCK_SCOPE,
        },
    )


def _binding(
    *,
    terminal: dict[str, Any],
    readiness_record: dict[str, Any],
    suffix: str,
    offset_seconds: int = 0,
) -> dict[str, Any]:
    return build_notification_retry_readiness_binding(
        terminal_record=terminal,
        readiness_enforcement=_enforcement(
            readiness_record=readiness_record,
            suffix=suffix,
            offset_seconds=offset_seconds,
        ),
        recorded_at=BASE_TIME + timedelta(minutes=5, seconds=offset_seconds),
    )


def _row_count(dsn: str, table: str, column: str, value: str) -> int:
    allowed = {
        (
            "portfolio_risk_notification_retry_executions",
            "record_id",
        ),
        ("notification_retry_readiness_bindings", "terminal_record_id"),
    }
    if (table, column) not in allowed:
        raise AssertionError("atomic fixture query identifier is not allow-listed")
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Atomic notification retry history requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*) FROM risk_platform.{table} WHERE {column} = %s",
                (value,),
            )
            row = cursor.fetchone()
    if row is None:
        raise AssertionError("atomic fixture count is unavailable")
    return int(row[0])


def run_contract_check(dsn: str) -> dict[str, Any]:
    retained_readiness = _readiness_record(
        request_id="ATOMIC-BUNDLE-READINESS-001"
    )
    record_notification_execution_readiness(dsn=dsn, record=retained_readiness)

    terminal = _terminal(
        request_id="ATOMIC-BUNDLE-TERMINAL-001",
        plan_id="atomic-bundle-plan-001",
    )
    binding = _binding(
        terminal=terminal,
        readiness_record=retained_readiness,
        suffix="created",
    )
    created = record_notification_retry_governance_bundle(
        dsn=dsn,
        terminal_record=terminal,
        readiness_binding=binding,
    )
    if created["created"] is not True:
        raise AssertionError("fresh atomic retry governance bundle was not created")
    replay = record_notification_retry_governance_bundle(
        dsn=dsn,
        terminal_record=terminal,
        readiness_binding=binding,
    )
    if replay["created"] is not False or replay["exact_replay"] is not True:
        raise AssertionError("exact atomic retry governance replay did not converge")

    legacy_terminal = _terminal(
        request_id="ATOMIC-BUNDLE-LEGACY-001",
        plan_id="atomic-bundle-plan-legacy",
        offset_seconds=10,
    )
    record_notification_retry_execution(dsn=dsn, record=legacy_terminal)
    legacy_binding = _binding(
        terminal=legacy_terminal,
        readiness_record=retained_readiness,
        suffix="legacy",
        offset_seconds=10,
    )
    try:
        record_notification_retry_governance_bundle(
            dsn=dsn,
            terminal_record=legacy_terminal,
            readiness_binding=legacy_binding,
        )
    except ValidationError as exc:
        legacy_backfill_rejected = "cannot backfill" in str(exc)
    else:
        legacy_backfill_rejected = False
    if not legacy_backfill_rejected:
        raise AssertionError("legacy terminal history was silently backfilled")
    if _row_count(
        dsn,
        "notification_retry_readiness_bindings",
        "terminal_record_id",
        legacy_terminal["record_id"],
    ) != 0:
        raise AssertionError("legacy readiness binding was retained after rejection")

    unretained_readiness = _readiness_record(
        request_id="ATOMIC-BUNDLE-UNRETAINED-READINESS",
    )
    rollback_terminal = _terminal(
        request_id="ATOMIC-BUNDLE-ROLLBACK-001",
        plan_id="atomic-bundle-plan-rollback",
        offset_seconds=20,
    )
    rollback_binding = _binding(
        terminal=rollback_terminal,
        readiness_record=unretained_readiness,
        suffix="rollback",
        offset_seconds=20,
    )
    try:
        record_notification_retry_governance_bundle(
            dsn=dsn,
            terminal_record=rollback_terminal,
            readiness_binding=rollback_binding,
        )
    except ValidationError as exc:
        second_write_failure_observed = "readiness source is missing" in str(exc)
    else:
        second_write_failure_observed = False
    if not second_write_failure_observed:
        raise AssertionError("missing readiness source did not fail the second write")
    if _row_count(
        dsn,
        "portfolio_risk_notification_retry_executions",
        "record_id",
        rollback_terminal["record_id"],
    ) != 0:
        raise AssertionError("terminal record survived failed atomic bundle")
    if _row_count(
        dsn,
        "notification_retry_readiness_bindings",
        "terminal_record_id",
        rollback_terminal["record_id"],
    ) != 0:
        raise AssertionError("readiness binding survived failed atomic bundle")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Atomic notification retry history requires psycopg") from exc
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
    failures = [check for check in checks if check[3] != "pass"]
    if failures:
        names = ", ".join(str(check[0]) for check in failures)
        raise AssertionError("atomic retry governance reconciliation failed: " + names)

    return {
        "model_version": "portfolio-risk-notification-retry-governance-bundle-v1",
        "terminal_record_id": terminal["record_id"],
        "binding_id": binding["binding_id"],
        "fresh_bundle_created": True,
        "exact_replay_converged": True,
        "legacy_backfill_rejected": legacy_backfill_rejected,
        "second_write_failure_observed": second_write_failure_observed,
        "first_write_rolled_back": True,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prove atomic terminal and readiness-binding persistence against "
            "PostgreSQL 16."
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
        print(f"Atomic retry governance contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
