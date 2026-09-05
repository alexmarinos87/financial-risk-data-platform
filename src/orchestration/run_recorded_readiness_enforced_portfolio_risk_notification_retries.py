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
    DEFAULT_DESTINATION_CONFIG,
    DEFAULT_DESTINATION_ID,
    AttemptWriter,
    Transport,
    _default_transport,
    _endpoint,
    write_delivery_attempt,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    aware_utc,
    load_retry_execution_contract,
    safe_segment,
)
from src.orchestration.portfolio_risk_notification_retry_plan_contract import (
    load_retry_plan,
)
from src.orchestration.run_readiness_enforced_portfolio_risk_notification_retries import (
    MODEL_VERSION as GOVERNED_EXECUTION_MODEL_VERSION,
    execute_readiness_enforced_portfolio_risk_notification_retries,
)
from src.warehouse.notification_execution_readiness_enforcement import (
    enforce_notification_execution_readiness,
    validate_notification_execution_readiness_enforcement,
)
from src.warehouse.notification_retry_destination_binding_contract import (
    build_retry_destination_binding,
)
from src.warehouse.notification_retry_destination_binding_reader import (
    read_notification_retry_destination_binding,
)
from src.warehouse.notification_retry_destination_binding_recorder import (
    record_notification_retry_destination_binding,
)
from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
    validate_retry_execution_record,
)
from src.warehouse.notification_retry_execution_reader import (
    read_notification_retry_execution_request,
)
from src.warehouse.notification_retry_governance_bundle_recorder import (
    record_notification_retry_governance_bundle,
    validate_notification_retry_governance_bundle,
)
from src.warehouse.notification_retry_readiness_binding_contract import (
    build_notification_retry_readiness_binding,
)
from src.warehouse.notification_retry_readiness_binding_reader import (
    read_notification_retry_readiness_binding,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

Clock = Callable[[], datetime]
Executor = Callable[..., dict[str, Any]]
PlanLoader = Callable[[Path], Mapping[str, Any]]
HistoryReader = Callable[..., Mapping[str, Any] | None]
ReadinessBindingReader = Callable[..., Mapping[str, Any] | None]
BundleRecorder = Callable[..., dict[str, Any]]
DestinationRecorder = Callable[..., dict[str, Any]]
DestinationReader = Callable[..., dict[str, Any] | None]
ReadinessEnforcer = Callable[..., Mapping[str, Any]]


class RecordedReadinessRetryExecutionError(RuntimeError):
    def __init__(
        self,
        *,
        failure_code: str,
        terminal_history: Mapping[str, Any],
        readiness_history: Mapping[str, Any],
        destination_history: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(failure_code)
        self.failure_code = failure_code
        self.terminal_history = dict(terminal_history)
        self.readiness_history = dict(readiness_history)
        self.destination_history = (
            None if destination_history is None else dict(destination_history)
        )


def _failure_code(exc: Exception) -> str:
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


def _terminal_execution_summary(summary: Mapping[str, Any]) -> dict[str, Any]:
    canonical = dict(summary)
    canonical.pop("destination_authority", None)
    canonical.pop("execution_readiness", None)
    canonical.pop("governed_execution", None)
    return canonical


def _destination_authority(summary: Mapping[str, Any]) -> dict[str, Any] | None:
    value = summary.get("destination_authority")
    return dict(value) if isinstance(value, Mapping) else None


def _record_destination_binding(
    *,
    dsn: str,
    terminal_record: Mapping[str, Any],
    destination_authority: Mapping[str, Any] | None,
    recorder: DestinationRecorder | None,
) -> dict[str, Any] | None:
    if destination_authority is None or recorder is None:
        return None
    binding = build_retry_destination_binding(
        record_id=cast(str, terminal_record["record_id"]),
        request_id=cast(str, terminal_record["request_id"]),
        plan_id=cast(str, terminal_record["plan_id"]),
        execution_id=cast(str | None, terminal_record["execution_id"]),
        destination_authority=destination_authority,
        recorded_at=cast(str, terminal_record["recorded_at"]),
    )
    return recorder(dsn=dsn, binding=binding)


def _replay_summary(
    *,
    terminal_record: Mapping[str, Any],
    readiness_binding: Mapping[str, Any],
) -> dict[str, Any]:
    base = terminal_record.get("execution_summary")
    if not isinstance(base, Mapping):
        raise StorageError("completed retry execution summary is unavailable")
    readiness = readiness_binding["readiness_enforcement"]
    if not isinstance(readiness, Mapping):
        raise StorageError("retained retry readiness enforcement is unavailable")
    summary = dict(base)
    summary["execution_readiness"] = dict(readiness)
    summary["governed_execution"] = {
        "model_version": GOVERNED_EXECUTION_MODEL_VERSION,
        "plan_id": terminal_record["plan_id"],
        "destination_id": readiness["destination_id"],
        "readiness_enforcement_id": readiness["enforcement_id"],
        "single_physical_lock_acquisition": True,
        "nested_lock_reacquisition_performed": False,
        "lock_reused_by_retry_executor": True,
        "outer_lock_released": True,
    }
    return summary


def _existing_result(
    *,
    existing: Mapping[str, Any],
    readiness_evidence: Mapping[str, Any] | None,
    request_id: str,
    plan_id: str,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    record_value = existing.get("record")
    history_value = existing.get("history")
    if not isinstance(record_value, Mapping) or not isinstance(history_value, Mapping):
        raise StorageError("retained retry terminal history is invalid")
    terminal = validate_retry_execution_record(record_value)
    if terminal["request_id"] != request_id:
        raise StorageError("retained retry execution request identity changed")
    if terminal["plan_id"] != plan_id:
        raise ValidationError(
            "request_id already exists for a different notification retry plan"
        )
    if readiness_evidence is None:
        raise ValidationError(
            "retained retry terminal has no readiness binding; replay is blocked"
        )
    binding_value = readiness_evidence.get("binding")
    readiness_history = readiness_evidence.get("history")
    if not isinstance(binding_value, Mapping) or not isinstance(
        readiness_history,
        Mapping,
    ):
        raise StorageError("retained retry readiness history is invalid")
    _, binding = validate_notification_retry_governance_bundle(
        terminal_record=terminal,
        readiness_binding=binding_value,
    )
    return terminal, dict(history_value), {
        "binding": binding,
        "history": dict(readiness_history),
    }


def execute_and_record_readiness_enforced_portfolio_risk_notification_retries(
    *,
    plan_path: Path,
    confirm_plan_id: str,
    request_id: str,
    config_path: Path,
    dsn: str,
    execute: bool = False,
    destination_config_path: Path | None = None,
    destination_id: str = DEFAULT_DESTINATION_ID,
    environment: Mapping[str, str] | None = None,
    executor: Executor | None = None,
    plan_loader: PlanLoader | None = None,
    history_reader: HistoryReader | None = None,
    readiness_binding_reader: ReadinessBindingReader | None = None,
    bundle_recorder: BundleRecorder | None = None,
    destination_binding_recorder: DestinationRecorder | None = None,
    destination_binding_reader: DestinationReader | None = None,
    readiness_enforcer: ReadinessEnforcer | None = None,
    transport: Transport | None = None,
    attempt_writer: AttemptWriter | None = None,
    clock: Clock | None = None,
    reader: Callable[..., list[dict[str, Any]]] | None = None,
    lock_factory: Callable[..., Any] | None = None,
    destination_authority_resolver: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if execute is not True:
        raise ValidationError(
            "explicit --execute is required for recorded readiness-enforced retries"
        )
    selected_plan_loader = plan_loader or load_retry_plan
    retained_plan = selected_plan_loader(plan_path)
    if not isinstance(retained_plan, Mapping):
        raise ValidationError("retry plan loader returned invalid evidence")
    selected_request_id = safe_segment(request_id, "request_id")
    assert selected_request_id is not None
    plan_id = safe_segment(retained_plan.get("plan_id"), "retry plan_id")
    assert plan_id is not None
    confirmed = safe_segment(confirm_plan_id, "confirm_plan_id")
    if confirmed != plan_id:
        raise ValidationError("confirm_plan_id does not match the retained retry plan")
    selected_destination_id = safe_segment(destination_id, "destination_id")
    assert selected_destination_id is not None

    selected_history_reader = history_reader or read_notification_retry_execution_request
    selected_readiness_reader = (
        readiness_binding_reader or read_notification_retry_readiness_binding
    )
    selected_destination_reader = (
        destination_binding_reader or read_notification_retry_destination_binding
    )
    destination_history: dict[str, Any] | None
    existing = selected_history_reader(
        dsn=dsn,
        request_id=selected_request_id,
    )
    if existing is not None:
        record_value = existing.get("record")
        if not isinstance(record_value, Mapping):
            raise StorageError("retained retry terminal record is invalid")
        terminal_record_id = record_value.get("record_id")
        if not isinstance(terminal_record_id, str):
            raise StorageError("retained retry terminal record identity is invalid")
        readiness_evidence = selected_readiness_reader(
            dsn=dsn,
            terminal_record_id=terminal_record_id,
        )
        terminal, terminal_history, retained_readiness = _existing_result(
            existing=existing,
            readiness_evidence=readiness_evidence,
            request_id=selected_request_id,
            plan_id=plan_id,
        )
        destination_history = selected_destination_reader(
            dsn=dsn,
            record_id=terminal["record_id"],
        )
        result = {
            "terminal_history": terminal_history,
            "readiness_history": retained_readiness["history"],
            "destination_history": destination_history,
            "replayed": True,
            "external_request_replayed": False,
        }
        if terminal["terminal_status"] == "completed":
            result["execution_summary"] = _replay_summary(
                terminal_record=terminal,
                readiness_binding=retained_readiness["binding"],
            )
            return result
        failure_code = terminal["failure_code"]
        if not isinstance(failure_code, str):
            raise StorageError("failed retry terminal has no failure code")
        raise RecordedReadinessRetryExecutionError(
            failure_code=failure_code,
            terminal_history=terminal_history,
            readiness_history=cast(Mapping[str, Any], retained_readiness["history"]),
            destination_history=destination_history,
        )

    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    started_at = aware_utc(selected_clock(), "recorded governed retry started_at")
    clock_calls = 0

    def execution_clock() -> datetime:
        nonlocal clock_calls
        if clock_calls < 2:
            clock_calls += 1
            return started_at
        clock_calls += 1
        return _next_utc(
            selected_clock,
            minimum=started_at,
            label="recorded governed retry clock",
        )

    selected_environment = environment if environment is not None else os.environ
    selected_transport = transport or _default_transport
    selected_writer = attempt_writer or (
        lambda attempt: write_delivery_attempt(attempt, dsn=dsn)
    )
    selected_executor = (
        executor or execute_readiness_enforced_portfolio_risk_notification_retries
    )
    selected_bundle_recorder = bundle_recorder or record_notification_retry_governance_bundle
    selected_destination_recorder = (
        destination_binding_recorder or record_notification_retry_destination_binding
    )
    base_readiness_enforcer = readiness_enforcer or enforce_notification_execution_readiness

    requested_event_ids: list[str] = []
    persisted_attempts: list[dict[str, Any]] = []
    observed_readiness: dict[str, Any] | None = None
    observed_authority: dict[str, Any] | None = None

    def observing_readiness_enforcer(**kwargs: Any) -> Mapping[str, Any]:
        nonlocal observed_readiness
        candidate = validate_notification_execution_readiness_enforcement(
            base_readiness_enforcer(**kwargs)
        )
        if observed_readiness is not None and observed_readiness != candidate:
            raise ValidationError("readiness authority changed during retry execution")
        observed_readiness = candidate
        return candidate

    def observe_destination_authority(value: Mapping[str, Any]) -> None:
        nonlocal observed_authority
        candidate = dict(value)
        if observed_authority is not None and observed_authority != candidate:
            raise ValidationError("destination authority changed during retry execution")
        observed_authority = candidate

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

    delivery_config, retry_policy, execution_policy = load_retry_execution_contract(
        config_path
    )
    endpoint_host: str | None = None
    raw_endpoint = selected_environment.get(delivery_config.endpoint_env)
    if raw_endpoint is not None:
        _, endpoint_host = _endpoint(raw_endpoint)
    destination_path = (
        destination_config_path
        if destination_config_path is not None
        else (
            config_path.parent / "notification_destinations.yaml"
            if (config_path.parent / "notification_destinations.yaml").is_file()
            else DEFAULT_DESTINATION_CONFIG
        )
    )

    execution_summary: dict[str, Any] | None = None
    execution_error: Exception | None = None
    try:
        execution_summary = selected_executor(
            plan_path=plan_path,
            confirm_plan_id=confirm_plan_id,
            request_id=selected_request_id,
            config_path=config_path,
            dsn=dsn,
            execute=True,
            destination_config_path=destination_path,
            destination_id=selected_destination_id,
            environment=selected_environment,
            reader=reader,
            attempt_writer=tracking_writer,
            transport=tracking_transport,
            clock=execution_clock,
            lock_factory=lock_factory,
            destination_authority_resolver=destination_authority_resolver,
            destination_authority_observer=observe_destination_authority,
            readiness_enforcer=observing_readiness_enforcer,
            readiness_validator=validate_notification_execution_readiness_enforcement,
        )
        if not isinstance(execution_summary, Mapping):
            raise StorageError("readiness-enforced retry returned invalid evidence")
        execution_summary = dict(execution_summary)
        summary_readiness = execution_summary.get("execution_readiness")
        if not isinstance(summary_readiness, Mapping):
            raise ValidationError("retry execution summary lacks readiness evidence")
        summary_readiness_validated = (
            validate_notification_execution_readiness_enforcement(summary_readiness)
        )
        if observed_readiness is None:
            observed_readiness = summary_readiness_validated
        elif observed_readiness != summary_readiness_validated:
            raise ValidationError("retry summary readiness authority changed")
        summary_authority = _destination_authority(execution_summary)
        if summary_authority is not None:
            observe_destination_authority(summary_authority)
    except Exception as exc:
        if observed_readiness is None:
            raise
        execution_error = exc

    finished_at = _next_utc(
        selected_clock,
        minimum=started_at,
        label="recorded governed retry finished_at",
    )
    terminal_recorded_at = _next_utc(
        selected_clock,
        minimum=finished_at,
        label="recorded governed retry recorded_at",
    )
    binding_recorded_at = _next_utc(
        selected_clock,
        minimum=terminal_recorded_at,
        label="recorded governed retry binding recorded_at",
    )

    if execution_error is None:
        assert execution_summary is not None
        outcomes = execution_summary.get("outcomes")
        if not isinstance(outcomes, list):
            raise ValidationError("retry execution summary outcomes are unavailable")
        attempt_ids = [str(outcome["attempt_id"]) for outcome in outcomes]
        event_ids = [str(outcome["event_id"]) for outcome in outcomes]
        succeeded = sum(outcome.get("outcome") == "succeeded" for outcome in outcomes)
        failed = sum(outcome.get("outcome") == "failed" for outcome in outcomes)
        terminal_record = build_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            started_at=started_at,
            finished_at=finished_at,
            recorded_at=terminal_recorded_at,
            terminal_status="completed",
            failure_code=None,
            request_count=len(requested_event_ids),
            attempts_persisted=len(persisted_attempts),
            succeeded_count=succeeded,
            failed_count=failed,
            attempt_ids=attempt_ids,
            requested_event_ids=event_ids,
            persisted_event_ids=event_ids,
            execution_summary=_terminal_execution_summary(execution_summary),
        )
    else:
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
        lock = observed_readiness["lock"]
        terminal_record = build_retry_execution_record(
            request_id=selected_request_id,
            plan_id=plan_id,
            started_at=started_at,
            finished_at=finished_at,
            recorded_at=terminal_recorded_at,
            terminal_status=_terminal_status(
                requests=len(requested_event_ids),
                attempts_persisted=len(persisted_attempts),
            ),
            failure_code=_failure_code(execution_error),
            request_count=len(requested_event_ids),
            attempts_persisted=len(persisted_attempts),
            succeeded_count=succeeded,
            failed_count=failed,
            attempt_ids=attempt_ids,
            requested_event_ids=requested_event_ids,
            persisted_event_ids=persisted_event_ids,
            execution_summary=None,
            endpoint_host=endpoint_host,
            delivery_fingerprint=delivery_config.fingerprint,
            retry_policy_fingerprint=retry_policy.fingerprint,
            retry_execution_policy_fingerprint=execution_policy.fingerprint,
            lock_model_version=lock["model_version"],
            lock_key_fingerprint=lock["key_fingerprint"],
            lock_acquired=True,
            lock_released=True,
        )

    assert observed_readiness is not None
    readiness_binding = build_notification_retry_readiness_binding(
        terminal_record=terminal_record,
        readiness_enforcement=observed_readiness,
        recorded_at=binding_recorded_at,
    )
    bundle_history = selected_bundle_recorder(
        dsn=dsn,
        terminal_record=terminal_record,
        readiness_binding=readiness_binding,
    )
    terminal_history = bundle_history.get("terminal_history")
    readiness_history = bundle_history.get("readiness_history")
    if not isinstance(terminal_history, Mapping) or not isinstance(
        readiness_history,
        Mapping,
    ):
        raise StorageError("atomic retry governance history result is invalid")
    destination_history = _record_destination_binding(
        dsn=dsn,
        terminal_record=terminal_record,
        destination_authority=observed_authority,
        recorder=selected_destination_recorder,
    )

    if execution_error is not None:
        raise RecordedReadinessRetryExecutionError(
            failure_code=_failure_code(execution_error),
            terminal_history=terminal_history,
            readiness_history=readiness_history,
            destination_history=destination_history,
        ) from None

    return {
        "execution_summary": execution_summary,
        "terminal_history": dict(terminal_history),
        "readiness_history": dict(readiness_history),
        "destination_history": destination_history,
        "replayed": False,
        "atomic_commit": True,
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("recorded governed retry summary must not be a symbolic link")
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
        raise StorageError("unable to write recorded governed retry summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Execute one readiness-enforced notification retry plan and atomically "
            "retain terminal and readiness evidence."
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
        "--destination-config",
        type=Path,
        default=DEFAULT_DESTINATION_CONFIG,
    )
    parser.add_argument("--destination-id", default=DEFAULT_DESTINATION_ID)
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
        result = execute_and_record_readiness_enforced_portfolio_risk_notification_retries(
            plan_path=args.plan,
            confirm_plan_id=args.confirm_plan_id,
            request_id=args.request_id,
            config_path=args.config,
            destination_config_path=args.destination_config,
            destination_id=args.destination_id,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, result)
    except RecordedReadinessRetryExecutionError as exc:
        print(
            "Recorded readiness-enforced retry failed with terminal evidence: "
            f"{exc.failure_code}; "
            f"record_id={exc.terminal_history['record_id']}; "
            f"binding_id={exc.readiness_history['binding_id']}",
            file=sys.stderr,
        )
        return 1
    except ValidationError as exc:
        print(f"Recorded readiness-enforced retry rejected: {exc}", file=sys.stderr)
        return 1
    except (StorageError, RuntimeError) as exc:
        print(f"Recorded readiness-enforced retry history failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Recorded readiness-enforced retry failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
