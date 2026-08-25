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
    _endpoint,
    write_delivery_attempt,
)
from src.orchestration.execute_portfolio_risk_notification_retries import (
    execute_portfolio_risk_notification_retries,
)
from src.orchestration.portfolio_risk_notification_delivery_lock import (
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
    build_retry_execution_record,
)
from src.warehouse.notification_retry_execution_recorder import (
    record_notification_retry_execution,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

Clock = Callable[[], datetime]
Executor = Callable[..., dict[str, Any]]
Recorder = Callable[..., dict[str, Any]]


class RecordedRetryExecutionError(RuntimeError):
    def __init__(
        self,
        *,
        failure_code: str,
        history: Mapping[str, Any],
    ) -> None:
        super().__init__(failure_code)
        self.failure_code = failure_code
        self.history = dict(history)


def _failure_code(exc: BaseException) -> str:
    if isinstance(exc, OverlapError):
        return "overlap_error"
    if isinstance(exc, ValidationError):
        return "validation_error"
    if isinstance(exc, StorageError):
        return "storage_error"
    return "unexpected_error"


def _terminal_status(*, requests: int, attempts_persisted: int) -> str:
    if requests == 0:
        return "failed_before_request"
    if requests > attempts_persisted:
        return "persistence_uncertain"
    return "failed_after_request"


def _next_utc(clock: Clock, *, minimum: datetime, label: str) -> datetime:
    value = aware_utc(clock(), label)
    return value if value >= minimum else minimum


def _retained_fingerprint(
    retained_plan: Mapping[str, Any],
    group: str,
) -> str | None:
    value = retained_plan.get(group)
    if not isinstance(value, Mapping):
        return None
    fingerprint = value.get("fingerprint")
    return fingerprint if isinstance(fingerprint, str) else None


def execute_and_record_portfolio_risk_notification_retries(
    *,
    plan_path: Path,
    confirm_plan_id: str,
    request_id: str,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    environment: Mapping[str, str] | None = None,
    executor: Executor | None = None,
    recorder: Recorder | None = None,
    transport: Transport | None = None,
    attempt_writer: AttemptWriter | None = None,
    clock: Clock | None = None,
    reader: Callable[..., list[dict[str, Any]]] | None = None,
    lock_factory: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    retained_plan = load_retry_plan(plan_path)
    selected_request_id = safe_segment(request_id, "request_id")
    assert selected_request_id is not None
    plan_id = retained_plan["plan_id"]
    assert isinstance(plan_id, str)

    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    started_at = aware_utc(selected_clock(), "recorded execution started_at")
    clock_calls = 0

    def executor_clock() -> datetime:
        nonlocal clock_calls
        if clock_calls == 0:
            clock_calls += 1
            return started_at
        clock_calls += 1
        return _next_utc(
            selected_clock,
            minimum=started_at,
            label="recorded execution clock",
        )

    selected_transport = transport or _default_transport
    selected_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )
    selected_environment = environment if environment is not None else os.environ
    selected_executor = executor or execute_portfolio_risk_notification_retries
    selected_recorder = recorder or record_notification_retry_execution

    requested_event_ids: list[str] = []
    persisted_attempts: list[dict[str, Any]] = []

    def tracking_transport(
        endpoint: str,
        payload: bytes,
        headers: Mapping[str, str],
        timeout_seconds: float,
    ) -> int:
        event_id = headers.get("Idempotency-Key")
        if not isinstance(event_id, str) or not event_id:
            raise ValidationError("retry transport is missing Idempotency-Key")
        requested_event_ids.append(event_id)
        return selected_transport(endpoint, payload, headers, timeout_seconds)

    def tracking_writer(attempt: Mapping[str, Any]) -> None:
        selected_writer(attempt)
        persisted_attempts.append(dict(attempt))

    endpoint_host: str | None = None
    delivery_fingerprint = _retained_fingerprint(retained_plan, "delivery_config")
    retry_policy_fingerprint = _retained_fingerprint(retained_plan, "retry_policy")
    retry_execution_policy_fingerprint: str | None = None
    execution_summary: dict[str, Any] | None = None

    try:
        delivery_config, retry_policy, execution_policy = load_retry_execution_contract(
            config_path
        )
        delivery_fingerprint = delivery_config.fingerprint
        retry_policy_fingerprint = retry_policy.fingerprint
        retry_execution_policy_fingerprint = execution_policy.fingerprint
        raw_endpoint = selected_environment.get(delivery_config.endpoint_env)
        if raw_endpoint is not None:
            _, endpoint_host = _endpoint(raw_endpoint)

        execution_summary = selected_executor(
            plan_path=plan_path,
            confirm_plan_id=confirm_plan_id,
            request_id=selected_request_id,
            config_path=config_path,
            dsn=dsn,
            execute=execute,
            environment=selected_environment,
            reader=reader,
            attempt_writer=tracking_writer,
            transport=tracking_transport,
            clock=executor_clock,
            lock_factory=lock_factory,
        )
        outcomes = execution_summary.get("outcomes")
        if not isinstance(outcomes, list):
            raise ValidationError("retry execution summary outcomes are unavailable")
        finished_at = _next_utc(
            selected_clock,
            minimum=started_at,
            label="recorded execution finished_at",
        )
        recorded_at = _next_utc(
            selected_clock,
            minimum=finished_at,
            label="recorded execution recorded_at",
        )
        attempt_ids = [str(outcome["attempt_id"]) for outcome in outcomes]
        event_ids = [str(outcome["event_id"]) for outcome in outcomes]
        succeeded = sum(outcome.get("outcome") == "succeeded" for outcome in outcomes)
        failed = sum(outcome.get("outcome") == "failed" for outcome in outcomes)
        record = build_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            started_at=started_at,
            finished_at=finished_at,
            recorded_at=recorded_at,
            terminal_status="completed",
            failure_code=None,
            request_count=len(requested_event_ids),
            attempts_persisted=len(persisted_attempts),
            succeeded_count=succeeded,
            failed_count=failed,
            attempt_ids=attempt_ids,
            requested_event_ids=event_ids,
            persisted_event_ids=event_ids,
            execution_summary=execution_summary,
        )
    except Exception as exc:
        finished_at = _next_utc(
            selected_clock,
            minimum=started_at,
            label="recorded execution finished_at",
        )
        recorded_at = _next_utc(
            selected_clock,
            minimum=finished_at,
            label="recorded execution recorded_at",
        )
        attempt_ids = [
            str(attempt["attempt_id"])
            for attempt in persisted_attempts
            if attempt.get("attempt_id") is not None
        ]
        persisted_event_ids = [
            str(attempt["event_id"])
            for attempt in persisted_attempts
            if attempt.get("event_id") is not None
        ]
        succeeded = sum(
            attempt.get("outcome") == "succeeded" for attempt in persisted_attempts
        )
        failed = sum(
            attempt.get("outcome") == "failed" for attempt in persisted_attempts
        )
        record = build_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            started_at=started_at,
            finished_at=finished_at,
            recorded_at=recorded_at,
            terminal_status=_terminal_status(
                requests=len(requested_event_ids),
                attempts_persisted=len(persisted_attempts),
            ),
            failure_code=_failure_code(exc),
            request_count=len(requested_event_ids),
            attempts_persisted=len(persisted_attempts),
            succeeded_count=succeeded,
            failed_count=failed,
            attempt_ids=attempt_ids,
            requested_event_ids=requested_event_ids,
            persisted_event_ids=persisted_event_ids,
            execution_summary=None,
            endpoint_host=endpoint_host,
            delivery_fingerprint=delivery_fingerprint,
            retry_policy_fingerprint=retry_policy_fingerprint,
            retry_execution_policy_fingerprint=(
                retry_execution_policy_fingerprint
            ),
            lock_model_version=LOCK_MODEL_VERSION,
            lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
            lock_acquired=True if requested_event_ids else None,
            lock_released=None,
        )
        history = selected_recorder(dsn=dsn, record=record)
        raise RecordedRetryExecutionError(
            failure_code=cast(str, record["failure_code"]),
            history=history,
        ) from None

    history = selected_recorder(dsn=dsn, record=record)
    return {
        "execution_summary": execution_summary,
        "history": history,
    }


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
        raise StorageError("unable to write recorded retry summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one exact notification retry plan and retain terminal "
            "append-only PostgreSQL history."
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
        result = execute_and_record_portfolio_risk_notification_retries(
            plan_path=args.plan,
            confirm_plan_id=args.confirm_plan_id,
            request_id=args.request_id,
            config_path=args.config,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, result)
    except RecordedRetryExecutionError as exc:
        print(
            "Recorded notification retry failed with terminal evidence: "
            f"{exc.failure_code}; record_id={exc.history['record_id']}",
            file=sys.stderr,
        )
        return 1
    except ValidationError as exc:
        print(f"Recorded notification retry rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Recorded notification retry history failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Recorded notification retry failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
