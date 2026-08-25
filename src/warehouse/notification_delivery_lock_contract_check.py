from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
    acquire_notification_delivery_lock,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
)
from src.warehouse.notification_retry_execution_reader import (
    read_notification_retry_execution_request,
)
from src.warehouse.notification_retry_execution_recorder import (
    record_notification_retry_execution,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _history_contract(dsn: str) -> dict[str, Any]:
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    record = build_retry_execution_record(
        request_id="RETRY-CONTRACT-001",
        plan_id="retry-plan-contract-001",
        started_at=started,
        finished_at=started + timedelta(seconds=1),
        recorded_at=started + timedelta(seconds=2),
        terminal_status="failed_before_request",
        failure_code="overlap_error",
        request_count=0,
        attempts_persisted=0,
        succeeded_count=0,
        failed_count=0,
        attempt_ids=[],
        requested_event_ids=[],
        persisted_event_ids=[],
        execution_summary=None,
    )
    first = record_notification_retry_execution(dsn=dsn, record=record)
    second = record_notification_retry_execution(dsn=dsn, record=record)
    if first["created"] is not True or second["created"] is not False:
        raise AssertionError("notification retry history did not converge on retry")

    retained = read_notification_retry_execution_request(
        dsn=dsn,
        request_id=record["request_id"],
    )
    if retained is None:
        raise AssertionError("notification retry history preflight lookup returned no row")
    if retained["record"] != record:
        raise AssertionError("notification retry history preflight record changed")
    retained_history = retained["history"]
    if (
        retained_history["record_id"] != record["record_id"]
        or retained_history["created"] is not False
    ):
        raise AssertionError("notification retry history preflight summary is invalid")

    conflict = build_retry_execution_record(
        request_id="RETRY-CONTRACT-001",
        plan_id="retry-plan-contract-001",
        started_at=started,
        finished_at=started + timedelta(seconds=2),
        recorded_at=started + timedelta(seconds=3),
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
    conflict_rejected = False
    try:
        record_notification_retry_execution(dsn=dsn, record=conflict)
    except ValidationError:
        conflict_rejected = True
    if not conflict_rejected:
        raise AssertionError("conflicting retry execution request was not rejected")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency required in CI.
        raise RuntimeError(
            "Notification retry history contract requires psycopg"
        ) from exc

    mutation_rejected = False
    row_count = 0
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM risk_platform.portfolio_risk_notification_retry_executions
                    WHERE request_id = %s
                    """,
                    (record["request_id"],),
                )
                row = cursor.fetchone()
                row_count = int(row[0]) if row is not None else -1
                try:
                    cursor.execute(
                        """
                        UPDATE
                            risk_platform.portfolio_risk_notification_retry_executions
                        SET recorded_at = recorded_at
                        WHERE record_id = %s
                        """,
                        (record["record_id"],),
                    )
                except Exception:
                    connection.rollback()
                    mutation_rejected = True
    except Exception as exc:
        if not mutation_rejected:
            raise StorageError(
                "Unable to verify notification retry execution history"
            ) from exc

    if row_count != 1:
        raise AssertionError(
            "notification retry history retained an unexpected row count"
        )
    if not mutation_rejected:
        raise AssertionError("notification retry history allowed direct mutation")

    return {
        "retry_history_created": True,
        "retry_history_exact_retry_converged": True,
        "retry_history_preflight_read_validated": True,
        "retry_history_conflict_rejected": True,
        "retry_history_append_only": True,
        "retry_history_rows": row_count,
        "retry_history_record_id": record["record_id"],
    }


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
        **_history_contract(dsn),
        "external_request_performed": False,
        "delivery_attempt_written": False,
    }
    if not _summary_is_secret_safe(summary):
        raise AssertionError("notification delivery contract summary is not secret-safe")
    return summary


def _summary_is_secret_safe(summary: Mapping[str, Any]) -> bool:
    rendered = json.dumps(summary, sort_keys=True, allow_nan=False)
    forbidden = ("postgresql://", "password", "secret", "dsn")
    return not any(value in rendered.casefold() for value in forbidden)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise PostgreSQL delivery locking and append-only retry execution "
            "history."
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
        print(f"Notification delivery contract failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Notification delivery contract failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
