from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    AttemptWriter,
    Transport,
    _default_transport,
    write_delivery_attempt,
)
from src.orchestration.execute_portfolio_risk_notification_retries import (
    Clock,
    execute_portfolio_risk_notification_retries,
)
from src.orchestration.plan_portfolio_risk_notification_retries import CandidateReader
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    DeliveryLockFactory,
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    aware_utc,
    load_retry_execution_contract,
    safe_segment,
)
from src.orchestration.portfolio_risk_notification_retry_plan_contract import (
    load_retry_plan,
)
from src.warehouse.notification_retry_execution_contract import (
    build_notification_retry_execution_record,
    notification_retry_execution_document_sha256,
)
from src.warehouse.notification_retry_execution_recorder import (
    read_notification_retry_execution_by_request_id,
    record_notification_retry_execution,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

Executor = Callable[..., dict[str, Any]]
Recorder = Callable[..., dict[str, Any]]
ExistingReader = Callable[..., dict[str, Any] | None]


def _failure_code(error: Exception, *, persistence_uncertain: bool) -> str:
    if persistence_uncertain:
        return "attempt_persistence_uncertain"
    if isinstance(error, OverlapError):
        return "delivery_overlap"
    if isinstance(error, ValidationError):
        return "validation_failed"
    if isinstance(error, StorageError):
        return "storage_failed"
    if isinstance(error, RuntimeError):
        return "runtime_dependency_failed"
    return "unexpected_local_failure"


def _record_result(
    record: Mapping[str, Any],
    persistence: Mapping[str, Any],
    *,
    replayed: bool,
) -> dict[str, Any]:
    return {
        "record": dict(record),
        "persistence": dict(persistence),
        "replayed": replayed,
        "external_request_performed_this_invocation": (
            not replayed and bool(record["requested_event_ids"])
        ),
        "response_bodies_recorded": False,
        "endpoint_url_recorded": False,
        "dsn_recorded": False,
    }


def run_recorded_portfolio_risk_notification_retries(
    *,
    plan_path: Path,
    confirm_plan_id: str,
    request_id: str,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    environment: Mapping[str, str] | None = None,
    reader: CandidateReader | None = None,
    attempt_writer: AttemptWriter | None = None,
    transport: Transport | None = None,
    clock: Clock | None = None,
    lock_factory: DeliveryLockFactory | None = None,
    executor: Executor = execute_portfolio_risk_notification_retries,
    recorder: Recorder = record_notification_retry_execution,
    existing_reader: ExistingReader = read_notification_retry_execution_by_request_id,
) -> dict[str, Any]:
    if execute is not True:
        raise ValidationError("explicit --execute is required for recorded retry delivery")
    selected_request_id = cast(str, safe_segment(request_id, "request_id"))
    retained_plan = load_retry_plan(plan_path)
    plan_id = cast(str, retained_plan["plan_id"])
    if confirm_plan_id != plan_id:
        raise ValidationError("confirm_plan_id does not match the retained retry plan")

    existing = existing_reader(dsn=dsn, request_id=selected_request_id)
    if existing is not None:
        if existing["plan_id"] != plan_id:
            raise ValidationError(
                "request_id already belongs to a different notification retry plan"
            )
        digest = notification_retry_execution_document_sha256(existing)
        return _record_result(
            existing,
            {
                "record_id": existing["record_id"],
                "request_id": existing["request_id"],
                "plan_id": existing["plan_id"],
                "execution_id": existing["execution_id"],
                "terminal_status": existing["terminal_status"],
                "requested_event_count": len(existing["requested_event_ids"]),
                "persisted_event_count": len(existing["persisted_event_ids"]),
                "document_sha256": digest,
                "created": False,
            },
            replayed=True,
        )

    delivery_config, retry_policy, execution_policy = load_retry_execution_contract(
        config_path
    )
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    started_at = aware_utc(selected_clock(), "recorded execution started_at")
    first_clock_call = True

    def execution_clock() -> datetime:
        nonlocal first_clock_call
        if first_clock_call:
            first_clock_call = False
            return started_at
        return aware_utc(selected_clock(), "recorded execution clock")

    requested_event_ids: list[str] = []
    persisted_event_ids: list[str] = []
    persisted_attempt_ids: list[str] = []
    selected_transport = transport or _default_transport
    selected_attempt_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )

    def observed_transport(
        endpoint: str,
        payload: bytes,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> int:
        event_id = headers.get("Idempotency-Key")
        if not isinstance(event_id, str) or not event_id:
            raise ValidationError("retry request is missing its Idempotency-Key")
        if event_id in requested_event_ids:
            raise ValidationError("retry execution attempted one event more than once")
        requested_event_ids.append(event_id)
        return selected_transport(endpoint, payload, headers, timeout_seconds)

    def observed_writer(attempt: Mapping[str, Any]) -> None:
        event_id = attempt.get("event_id")
        attempt_id = attempt.get("attempt_id")
        if not isinstance(event_id, str) or not isinstance(attempt_id, str):
            raise ValidationError("retry attempt evidence is missing identity")
        selected_attempt_writer(attempt)
        persisted_event_ids.append(event_id)
        persisted_attempt_ids.append(attempt_id)

    execution_summary: dict[str, Any] | None = None
    terminal_status = "completed"
    failure_stage: str | None = None
    failure_code: str | None = None
    try:
        execution_summary = executor(
            plan_path=plan_path,
            confirm_plan_id=confirm_plan_id,
            request_id=selected_request_id,
            config_path=config_path,
            dsn=dsn,
            execute=True,
            environment=environment,
            reader=reader,
            attempt_writer=observed_writer,
            transport=observed_transport,
            clock=execution_clock,
            lock_factory=lock_factory,
        )
    except Exception as error:
        if not requested_event_ids:
            terminal_status = "failed_before_request"
            failure_stage = "pre_request"
        elif len(persisted_event_ids) == len(requested_event_ids):
            terminal_status = "failed_after_request"
            failure_stage = "post_request"
        else:
            terminal_status = "persistence_uncertain"
            failure_stage = "attempt_persistence"
        failure_code = _failure_code(
            error,
            persistence_uncertain=terminal_status == "persistence_uncertain",
        )

    finished_at = aware_utc(selected_clock(), "recorded execution finished_at")
    if finished_at < started_at:
        raise ValidationError("recorded execution clock moved backwards")

    if terminal_status == "completed":
        if execution_summary is None:
            raise ValidationError("completed retry execution returned no summary")
        execution_id = cast(str, execution_summary["execution_id"])
        summary_outcomes = execution_summary.get("outcomes")
        if not isinstance(summary_outcomes, list):
            raise ValidationError("completed retry execution returned invalid outcomes")
        summary_event_ids = [outcome.get("event_id") for outcome in summary_outcomes]
        if summary_event_ids != requested_event_ids:
            raise ValidationError("observed requests do not match execution outcomes")
        record = build_notification_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            terminal_status="completed",
            started_at=started_at,
            finished_at=finished_at,
            execution_id=execution_id,
            delivery_fingerprint=delivery_config.fingerprint,
            retry_policy_fingerprint=retry_policy.fingerprint,
            retry_execution_policy_fingerprint=execution_policy.fingerprint,
            delivery_lock_model_version=LOCK_MODEL_VERSION,
            delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
            requested_event_ids=requested_event_ids,
            persisted_event_ids=persisted_event_ids,
            persisted_attempt_ids=persisted_attempt_ids,
            execution=execution_summary,
        )
    else:
        record = build_notification_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            terminal_status=terminal_status,
            started_at=started_at,
            finished_at=finished_at,
            failure_stage=failure_stage,
            failure_code=failure_code,
            delivery_fingerprint=delivery_config.fingerprint,
            retry_policy_fingerprint=retry_policy.fingerprint,
            retry_execution_policy_fingerprint=execution_policy.fingerprint,
            delivery_lock_model_version=LOCK_MODEL_VERSION,
            delivery_lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
            requested_event_ids=requested_event_ids,
            persisted_event_ids=persisted_event_ids,
            persisted_attempt_ids=persisted_attempt_ids,
        )

    persistence = recorder(dsn=dsn, record=record)
    return _record_result(record, persistence, replayed=False)


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("recorded retry summary must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except (OSError, TypeError, ValueError):
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write recorded retry execution summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one exact notification retry plan and retain one terminal "
            "append-only PostgreSQL record."
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--confirm-plan-id", required=True)
    parser.add_argument("--request-id", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_recorded_portfolio_risk_notification_retries(
            plan_path=args.plan,
            confirm_plan_id=args.confirm_plan_id,
            request_id=args.request_id,
            config_path=args.config,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError as exc:
        print(f"Recorded notification retry rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Recorded notification retry failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Recorded notification retry failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0 if summary["record"]["terminal_status"] == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
