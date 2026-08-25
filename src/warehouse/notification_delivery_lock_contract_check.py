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
    build_notification_retry_execution_record,
)
from src.warehouse.notification_retry_execution_recorder import (
    record_notification_retry_execution,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _completed_record() -> dict[str, Any]:
    started_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    finished_at = started_at + timedelta(seconds=2)
    request_id = "RETRY-HISTORY-CONTRACT-1"
    plan_id = "retry-plan-contract-1"
    execution_id = "retry-execution-contract-1"
    event_id = "retry-event-contract-1"
    attempt_id = "retry-attempt-contract-1"
    configuration = {
        "delivery_fingerprint": "delivery-contract-fingerprint",
        "retry_policy_fingerprint": "retry-policy-contract-fingerprint",
        "retry_execution_policy_fingerprint": (
            "retry-execution-policy-contract-fingerprint"
        ),
    }
    execution = {
        "execution_id": execution_id,
        "request_id": request_id,
        "plan_id": plan_id,
        "executed_at": started_at.isoformat(),
        "channel": "webhook",
        "endpoint": {
            "host": "alerts.example.test",
            "full_url_recorded": False,
        },
        "configuration": configuration,
        "revalidation": {
            "performed": True,
            "current_plan_id": plan_id,
            "events_checked": 1,
            "exact_event_evidence_unchanged": True,
        },
        "selection": {
            "planned_retryable_events": 1,
            "executed_events": 1,
            "max_events": 25,
        },
        "outcomes": [
            {
                "event_id": event_id,
                "attempt_id": attempt_id,
                "attempt_number": 2,
                "attempted_at": started_at.isoformat(),
                "error_code": None,
                "http_status": 204,
                "outcome": "succeeded",
                "payload_sha256": "a" * 64,
            }
        ],
        "outcome_counts": {"succeeded": 1, "failed": 0},
        "execution": {
            "requested": True,
            "performed": True,
            "external_requests_performed": 1,
            "delivery_attempts_written": 1,
        },
        "concurrency_control": {
            "performed": True,
            "acquired": True,
            "released": True,
            "held_through_revalidation": True,
            "held_through_attempt_persistence": True,
            "model_version": LOCK_MODEL_VERSION,
            "scope": LOCK_SCOPE,
            "key_fingerprint": LOCK_KEY_FINGERPRINT,
        },
        "response_bodies_recorded": False,
        "plan_mutated": False,
        "acknowledgement_mutated": False,
        "dead_letter_mutated": False,
    }
    return build_notification_retry_execution_record(
        request_id=request_id,
        plan_id=plan_id,
        terminal_status="completed",
        started_at=started_at,
        finished_at=finished_at,
        execution_id=execution_id,
        delivery_fingerprint=configuration["delivery_fingerprint"],
        retry_policy_fingerprint=configuration["retry_policy_fingerprint"],
        retry_execution_policy_fingerprint=(
            configuration["retry_execution_policy_fingerprint"]
        ),
        delivery_lock_model_version=LOCK_MODEL_VERSION,
        delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
        requested_event_ids=[event_id],
        persisted_event_ids=[event_id],
        persisted_attempt_ids=[attempt_id],
        execution=execution,
    )


def _ambiguous_record() -> dict[str, Any]:
    started_at = datetime(2026, 1, 10, 13, tzinfo=timezone.utc)
    return build_notification_retry_execution_record(
        request_id="RETRY-HISTORY-CONTRACT-2",
        plan_id="retry-plan-contract-2",
        terminal_status="persistence_uncertain",
        started_at=started_at,
        finished_at=started_at + timedelta(seconds=1),
        failure_stage="attempt_persistence",
        failure_code="attempt_persistence_uncertain",
        delivery_fingerprint="delivery-contract-fingerprint",
        retry_policy_fingerprint="retry-policy-contract-fingerprint",
        retry_execution_policy_fingerprint=(
            "retry-execution-policy-contract-fingerprint"
        ),
        delivery_lock_model_version=LOCK_MODEL_VERSION,
        delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
        requested_event_ids=["retry-event-contract-2"],
        persisted_event_ids=[],
        persisted_attempt_ids=[],
    )


def _assert_append_only(dsn: str, record_id: str) -> None:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("PostgreSQL contract requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE risk_platform.portfolio_risk_notification_retry_executions
                    SET failure_code = 'forbidden_update'
                    WHERE record_id = %s
                    """,
                    (record_id,),
                )
            connection.commit()
        except Exception:
            connection.rollback()
        else:  # pragma: no cover
            raise AssertionError("retry execution UPDATE was not rejected")

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM
                    risk_platform.portfolio_risk_notification_retry_executions
                    WHERE record_id = %s
                    """,
                    (record_id,),
                )
            connection.commit()
        except Exception:
            connection.rollback()
        else:  # pragma: no cover
            raise AssertionError("retry execution DELETE was not rejected")


def _exercise_retry_execution_history(dsn: str) -> dict[str, Any]:
    completed = _completed_record()
    first = record_notification_retry_execution(dsn=dsn, record=completed)
    replay = record_notification_retry_execution(dsn=dsn, record=completed)
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("retry execution exact replay did not converge")

    conflict = _completed_record()
    conflict = build_notification_retry_execution_record(
        request_id=conflict["request_id"],
        plan_id=conflict["plan_id"],
        terminal_status="failed_before_request",
        started_at=datetime(2026, 1, 10, 12, tzinfo=timezone.utc),
        finished_at=datetime(2026, 1, 10, 12, 0, 3, tzinfo=timezone.utc),
        failure_stage="pre_request",
        failure_code="validation_failed",
        delivery_fingerprint="delivery-contract-fingerprint",
        retry_policy_fingerprint="retry-policy-contract-fingerprint",
        retry_execution_policy_fingerprint=(
            "retry-execution-policy-contract-fingerprint"
        ),
        delivery_lock_model_version=LOCK_MODEL_VERSION,
        delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
    )
    try:
        record_notification_retry_execution(dsn=dsn, record=conflict)
    except ValidationError:
        conflict_rejected = True
    else:  # pragma: no cover
        conflict_rejected = False
    if not conflict_rejected:
        raise AssertionError("conflicting retry request reuse was not rejected")

    ambiguous = _ambiguous_record()
    ambiguous_result = record_notification_retry_execution(
        dsn=dsn,
        record=ambiguous,
    )
    if ambiguous_result["terminal_status"] != "persistence_uncertain":
        raise AssertionError("ambiguous retry execution status was not retained")

    _assert_append_only(dsn, completed["record_id"])
    return {
        "completed_record_created": True,
        "exact_replay_converged": True,
        "conflicting_request_rejected": True,
        "persistence_uncertain_record_created": True,
        "append_only_mutation_rejected": True,
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
        "retry_execution_history": _exercise_retry_execution_history(dsn),
        "external_request_performed": False,
        "delivery_attempt_written": False,
    }
    if not _summary_is_secret_safe(summary):
        raise AssertionError("notification delivery contract summary is not secret-safe")
    return summary


def _summary_is_secret_safe(summary: Mapping[str, Any]) -> bool:
    rendered = json.dumps(summary, sort_keys=True, allow_nan=False)
    forbidden = ("postgresql://", "password", "secret", '"dsn"')
    return not any(value in rendered.casefold() for value in forbidden)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise PostgreSQL notification delivery locking and append-only "
            "retry execution history."
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
