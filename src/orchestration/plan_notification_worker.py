from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    NotificationDestination,
    evaluate_destination_activation,
    load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    RetryExecutionPolicy,
    aware_utc,
    bounded_integer,
    exact_mapping,
    load_retry_execution_contract,
    safe_segment,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    RetryPlanningPolicy,
)
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)

CONFIG_MODEL_VERSION = "portfolio-risk-notification-worker-config-v1"
PLAN_MODEL_VERSION = "portfolio-risk-notification-worker-plan-v1"
MAX_CONFIG_BYTES = 1_048_576
MAX_WORKERS = 20
MAX_INTERVAL_SECONDS = 86_400
MAX_JITTER_SECONDS = 3_600
MAX_EXECUTION_TIMEOUT_SECONDS = 3_600
MAX_READINESS_AGE_SECONDS = 300
MAX_CONSECUTIVE_FAILURES = 20
MAX_COOLDOWN_SECONDS = 86_400
EXECUTION_KINDS = frozenset({"initial", "retry"})
BLOCKING_REASON_ORDER = (
    "worker_disabled",
    "delivery_disabled",
    "retry_execution_disabled",
    "destination_not_active",
    "endpoint_environment_mismatch",
)
INITIAL_ENTRYPOINT = (
    "src.orchestration.deliver_portfolio_risk_notifications"
)
RETRY_ENTRYPOINT = (
    "src.orchestration.run_recorded_readiness_enforced_"
    "portfolio_risk_notification_retries"
)


@dataclass(frozen=True, slots=True)
class WorkerSchedule:
    mode: str
    interval_seconds: int
    jitter_seconds: int
    timezone_name: str


@dataclass(frozen=True, slots=True)
class WorkerLimits:
    max_initial_events: int
    max_retry_events: int
    max_concurrency: int
    execution_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class WorkerReadiness:
    required_status: str
    max_age_seconds: int


@dataclass(frozen=True, slots=True)
class WorkerSuspension:
    block_on_readiness_failure: bool
    block_on_persistence_ambiguity: bool
    block_on_expired_review: bool
    max_consecutive_failures: int
    cooldown_seconds: int


@dataclass(frozen=True, slots=True)
class NotificationWorker:
    worker_id: str
    enabled: bool
    destination_id: str
    execution_kinds: tuple[str, ...]
    schedule: WorkerSchedule
    limits: WorkerLimits
    readiness: WorkerReadiness
    suspension: WorkerSuspension

    @property
    def fingerprint(self) -> str:
        payload = _worker_document(self)
        digest = hashlib.sha256(_canonical_bytes(payload, "worker configuration")).hexdigest()
        return f"{CONFIG_MODEL_VERSION}-worker-{digest[:24]}"


def _canonical_bytes(value: Mapping[str, Any], label: str) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError(f"{label} is not canonical JSON") from None


def _read_yaml(path: Path, label: str) -> Mapping[str, Any]:
    if path.is_symlink():
        raise ValidationError(f"{label} must not be a symbolic link")
    if not path.is_file():
        raise ValidationError(f"{label} must be a regular file")
    try:
        if path.stat().st_size > MAX_CONFIG_BYTES:
            raise ValidationError(f"{label} exceeds 1 MB")
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ValidationError(f"{label} could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    return payload


def _require_regular_file(path: Path, label: str) -> None:
    if path.is_symlink():
        raise ValidationError(f"{label} must not be a symbolic link")
    if not path.is_file():
        raise ValidationError(f"{label} must be a regular file")
    try:
        size = path.stat().st_size
    except OSError:
        raise ValidationError(f"{label} could not be inspected") from None
    if size > MAX_CONFIG_BYTES:
        raise ValidationError(f"{label} exceeds 1 MB")


def _boolean(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean")
    return bool(value)


def _execution_kinds(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValidationError("execution_kinds must be an array")
    parsed = tuple(safe_segment(item, "execution_kinds item") for item in value)
    if any(item is None for item in parsed):
        raise ValidationError("execution_kinds contains an invalid value")
    selected = tuple(str(item) for item in parsed)
    if not selected or not set(selected).issubset(EXECUTION_KINDS):
        raise ValidationError("execution_kinds contains an unsupported kind")
    if list(selected) != sorted(selected) or len(selected) != len(set(selected)):
        raise ValidationError("execution_kinds must be sorted and unique")
    return selected


def _parse_schedule(value: Any) -> WorkerSchedule:
    schedule = exact_mapping(
        value,
        frozenset({"mode", "interval_seconds", "jitter_seconds", "timezone"}),
        "worker schedule",
    )
    if schedule["mode"] != "fixed_interval":
        raise ValidationError("worker schedule mode must be fixed_interval")
    if schedule["timezone"] != "UTC":
        raise ValidationError("worker schedule timezone must be UTC")
    interval = bounded_integer(
        schedule["interval_seconds"],
        "interval_seconds",
        minimum=60,
        maximum=MAX_INTERVAL_SECONDS,
    )
    jitter = bounded_integer(
        schedule["jitter_seconds"],
        "jitter_seconds",
        minimum=0,
        maximum=MAX_JITTER_SECONDS,
    )
    if jitter > interval // 2:
        raise ValidationError("jitter_seconds may not exceed half the interval")
    return WorkerSchedule(
        mode="fixed_interval",
        interval_seconds=interval,
        jitter_seconds=jitter,
        timezone_name="UTC",
    )


def _parse_limits(value: Any) -> WorkerLimits:
    limits = exact_mapping(
        value,
        frozenset(
            {
                "max_initial_events",
                "max_retry_events",
                "max_concurrency",
                "execution_timeout_seconds",
            }
        ),
        "worker limits",
    )
    max_concurrency = bounded_integer(
        limits["max_concurrency"],
        "max_concurrency",
        minimum=1,
        maximum=1,
    )
    return WorkerLimits(
        max_initial_events=bounded_integer(
            limits["max_initial_events"],
            "max_initial_events",
            minimum=1,
            maximum=100,
        ),
        max_retry_events=bounded_integer(
            limits["max_retry_events"],
            "max_retry_events",
            minimum=1,
            maximum=100,
        ),
        max_concurrency=max_concurrency,
        execution_timeout_seconds=bounded_integer(
            limits["execution_timeout_seconds"],
            "execution_timeout_seconds",
            minimum=1,
            maximum=MAX_EXECUTION_TIMEOUT_SECONDS,
        ),
    )


def _parse_readiness(value: Any) -> WorkerReadiness:
    readiness = exact_mapping(
        value,
        frozenset({"required_status", "max_age_seconds"}),
        "worker readiness",
    )
    if readiness["required_status"] != "allowed":
        raise ValidationError("worker readiness required_status must be allowed")
    return WorkerReadiness(
        required_status="allowed",
        max_age_seconds=bounded_integer(
            readiness["max_age_seconds"],
            "readiness max_age_seconds",
            minimum=1,
            maximum=MAX_READINESS_AGE_SECONDS,
        ),
    )


def _parse_suspension(value: Any) -> WorkerSuspension:
    suspension = exact_mapping(
        value,
        frozenset(
            {
                "block_on_readiness_failure",
                "block_on_persistence_ambiguity",
                "block_on_expired_review",
                "max_consecutive_failures",
                "cooldown_seconds",
            }
        ),
        "worker suspension",
    )
    readiness = _boolean(
        suspension["block_on_readiness_failure"],
        "block_on_readiness_failure",
    )
    ambiguity = _boolean(
        suspension["block_on_persistence_ambiguity"],
        "block_on_persistence_ambiguity",
    )
    expired = _boolean(
        suspension["block_on_expired_review"],
        "block_on_expired_review",
    )
    if not readiness or not ambiguity or not expired:
        raise ValidationError(
            "worker suspension must block on readiness, ambiguity, and expired review"
        )
    return WorkerSuspension(
        block_on_readiness_failure=True,
        block_on_persistence_ambiguity=True,
        block_on_expired_review=True,
        max_consecutive_failures=bounded_integer(
            suspension["max_consecutive_failures"],
            "max_consecutive_failures",
            minimum=1,
            maximum=MAX_CONSECUTIVE_FAILURES,
        ),
        cooldown_seconds=bounded_integer(
            suspension["cooldown_seconds"],
            "cooldown_seconds",
            minimum=0,
            maximum=MAX_COOLDOWN_SECONDS,
        ),
    )


def _parse_worker(worker_id: str, value: Any) -> NotificationWorker:
    worker = exact_mapping(
        value,
        frozenset(
            {
                "enabled",
                "destination_id",
                "execution_kinds",
                "schedule",
                "limits",
                "readiness",
                "suspension",
            }
        ),
        f"worker {worker_id}",
    )
    selected_worker_id = safe_segment(worker_id, "worker_id")
    destination_id = safe_segment(worker["destination_id"], "destination_id")
    assert selected_worker_id is not None and destination_id is not None
    return NotificationWorker(
        worker_id=selected_worker_id,
        enabled=_boolean(worker["enabled"], "worker enabled"),
        destination_id=destination_id,
        execution_kinds=_execution_kinds(worker["execution_kinds"]),
        schedule=_parse_schedule(worker["schedule"]),
        limits=_parse_limits(worker["limits"]),
        readiness=_parse_readiness(worker["readiness"]),
        suspension=_parse_suspension(worker["suspension"]),
    )


def load_notification_workers(path: Path) -> dict[str, NotificationWorker]:
    payload = _read_yaml(path, "notification worker configuration")
    root = exact_mapping(
        payload,
        frozenset({"model_version", "workers"}),
        "notification worker configuration",
    )
    if root["model_version"] != CONFIG_MODEL_VERSION:
        raise ValidationError("notification worker model_version is unsupported")
    workers = root["workers"]
    if not isinstance(workers, Mapping) or not workers:
        raise ValidationError("workers must be a non-empty mapping")
    if len(workers) > MAX_WORKERS:
        raise ValidationError("worker count exceeds the reviewed maximum")
    parsed = {
        str(worker_id): _parse_worker(str(worker_id), value)
        for worker_id, value in workers.items()
    }
    if list(parsed) != sorted(parsed):
        raise ValidationError("worker IDs must be sorted")
    return parsed


def _worker_document(worker: NotificationWorker) -> dict[str, Any]:
    return {
        "destination_id": worker.destination_id,
        "enabled": worker.enabled,
        "execution_kinds": list(worker.execution_kinds),
        "limits": {
            "execution_timeout_seconds": worker.limits.execution_timeout_seconds,
            "max_concurrency": worker.limits.max_concurrency,
            "max_initial_events": worker.limits.max_initial_events,
            "max_retry_events": worker.limits.max_retry_events,
        },
        "model_version": CONFIG_MODEL_VERSION,
        "readiness": {
            "max_age_seconds": worker.readiness.max_age_seconds,
            "required_status": worker.readiness.required_status,
        },
        "schedule": {
            "interval_seconds": worker.schedule.interval_seconds,
            "jitter_seconds": worker.schedule.jitter_seconds,
            "mode": worker.schedule.mode,
            "timezone": worker.schedule.timezone_name,
        },
        "suspension": {
            "block_on_expired_review": (
                worker.suspension.block_on_expired_review
            ),
            "block_on_persistence_ambiguity": (
                worker.suspension.block_on_persistence_ambiguity
            ),
            "block_on_readiness_failure": (
                worker.suspension.block_on_readiness_failure
            ),
            "cooldown_seconds": worker.suspension.cooldown_seconds,
            "max_consecutive_failures": (
                worker.suspension.max_consecutive_failures
            ),
        },
        "worker_id": worker.worker_id,
    }


def _validate_limits_against_delivery(
    worker: NotificationWorker,
    delivery: WebhookDeliveryConfig,
    retry_policy: RetryPlanningPolicy,
    retry_execution: RetryExecutionPolicy,
) -> None:
    if worker.limits.max_initial_events > delivery.max_batch_events:
        raise ValidationError("worker max_initial_events exceeds webhook batch limit")
    if worker.limits.max_retry_events > delivery.max_batch_events:
        raise ValidationError("worker max_retry_events exceeds webhook batch limit")
    if worker.limits.max_retry_events > retry_policy.max_plan_events:
        raise ValidationError("worker max_retry_events exceeds retry planning limit")
    if worker.limits.max_retry_events > retry_execution.max_events:
        raise ValidationError("worker max_retry_events exceeds retry execution limit")
    if worker.limits.execution_timeout_seconds < delivery.timeout_seconds:
        raise ValidationError(
            "worker execution timeout is shorter than webhook request timeout"
        )


def _next_schedule(
    worker: NotificationWorker,
    planned_at: datetime,
) -> tuple[datetime, int, int]:
    epoch = int(planned_at.timestamp())
    interval = worker.schedule.interval_seconds
    boundary_epoch = ((epoch // interval) + 1) * interval
    seed = hashlib.sha256(
        f"{worker.fingerprint}:{boundary_epoch}".encode("utf-8")
    ).digest()
    jitter_limit = worker.schedule.jitter_seconds
    jitter = 0 if jitter_limit == 0 else int.from_bytes(seed[:8], "big") % (
        jitter_limit + 1
    )
    scheduled_for = datetime.fromtimestamp(
        boundary_epoch + jitter,
        tz=timezone.utc,
    )
    return scheduled_for, boundary_epoch, jitter


def _work_items(worker: NotificationWorker) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if "initial" in worker.execution_kinds:
        items.append(
            {
                "entrypoint": INITIAL_ENTRYPOINT,
                "execution_kind": "initial",
                "max_events": worker.limits.max_initial_events,
            }
        )
    if "retry" in worker.execution_kinds:
        items.append(
            {
                "entrypoint": RETRY_ENTRYPOINT,
                "execution_kind": "retry",
                "max_events": worker.limits.max_retry_events,
            }
        )
    return items


def _blocking_reasons(
    *,
    worker: NotificationWorker,
    delivery: WebhookDeliveryConfig,
    retry_execution: RetryExecutionPolicy,
    activation_status: str,
    destination: NotificationDestination,
) -> list[str]:
    active = {
        "worker_disabled": not worker.enabled,
        "delivery_disabled": not delivery.enabled,
        "retry_execution_disabled": (
            "retry" in worker.execution_kinds and not retry_execution.enabled
        ),
        "destination_not_active": activation_status != "active",
        "endpoint_environment_mismatch": (
            destination.endpoint_env != delivery.endpoint_env
        ),
    }
    return [reason for reason in BLOCKING_REASON_ORDER if active[reason]]


def _plan_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        _canonical_bytes(identity, "notification worker plan")
    ).hexdigest()[:24]
    return f"{PLAN_MODEL_VERSION}-plan-{digest}"


def build_notification_worker_plan(
    *,
    worker: NotificationWorker,
    delivery: WebhookDeliveryConfig,
    retry_policy: RetryPlanningPolicy,
    retry_execution: RetryExecutionPolicy,
    destination: NotificationDestination,
    planned_at: datetime | str,
) -> dict[str, Any]:
    planned = aware_utc(planned_at, "planned_at")
    if destination.destination_id != worker.destination_id:
        raise ValidationError("worker and destination identities differ")
    _validate_limits_against_delivery(
        worker,
        delivery,
        retry_policy,
        retry_execution,
    )
    activation = evaluate_destination_activation(
        destination,
        evaluated_at=planned,
    )
    activation_status = activation["activation"]["status"]
    scheduled_for, boundary_epoch, deterministic_jitter = _next_schedule(
        worker,
        planned,
    )
    blockers = _blocking_reasons(
        worker=worker,
        delivery=delivery,
        retry_execution=retry_execution,
        activation_status=str(activation_status),
        destination=destination,
    )
    if not worker.enabled:
        status = "disabled"
    elif blockers:
        status = "blocked"
    else:
        status = "would_schedule"
    identity = {
        "blocking_reasons": blockers,
        "concurrency_control": {
            "key_fingerprint": LOCK_KEY_FINGERPRINT,
            "lock_acquired": False,
            "max_concurrency": worker.limits.max_concurrency,
            "model_version": LOCK_MODEL_VERSION,
            "scope": LOCK_SCOPE,
        },
        "delivery": {
            "delivery_fingerprint": delivery.fingerprint,
            "max_batch_events": delivery.max_batch_events,
            "retry_execution_enabled": retry_execution.enabled,
            "retry_execution_policy_fingerprint": retry_execution.fingerprint,
            "retry_planning_policy_fingerprint": retry_policy.fingerprint,
            "webhook_enabled": delivery.enabled,
        },
        "destination": {
            "activation_status": activation_status,
            "allowed_event_types": list(destination.allowed_event_types),
            "destination_id": destination.destination_id,
            "endpoint_environment_variable": destination.endpoint_env,
            "endpoint_value_recorded": False,
            "fingerprint": destination.fingerprint,
        },
        "execution": {
            "execution_timeout_seconds": worker.limits.execution_timeout_seconds,
            "work_items": _work_items(worker),
        },
        "model_version": PLAN_MODEL_VERSION,
        "planned_at": planned.isoformat(),
        "readiness": {
            "max_age_seconds": worker.readiness.max_age_seconds,
            "refresh_under_shared_lock": True,
            "required_status": worker.readiness.required_status,
            "source_view": (
                "risk_platform.current_notification_execution_readiness_review"
            ),
        },
        "schedule": {
            "activation_action": (
                "would_create" if status == "would_schedule" else "none"
            ),
            "boundary_epoch": boundary_epoch,
            "deterministic_jitter_seconds": deterministic_jitter,
            "interval_seconds": worker.schedule.interval_seconds,
            "jitter_seconds": worker.schedule.jitter_seconds,
            "mode": worker.schedule.mode,
            "scheduled_for": scheduled_for.isoformat(),
            "timezone": worker.schedule.timezone_name,
        },
        "side_effects": {
            "acknowledgement_mutated": False,
            "cloud_schedule_activated": False,
            "database_read_performed": False,
            "delivery_attempt_written": False,
            "external_request_performed": False,
            "infrastructure_deployed": False,
            "outbox_mutated": False,
            "terraform_apply_performed": False,
        },
        "status": status,
        "suspension": {
            "conditions": [
                "expired_review",
                "persistence_ambiguity",
                "readiness_failure",
                "repeated_delivery_failure",
            ],
            "cooldown_seconds": worker.suspension.cooldown_seconds,
            "max_consecutive_failures": (
                worker.suspension.max_consecutive_failures
            ),
        },
        "worker": {
            "enabled": worker.enabled,
            "fingerprint": worker.fingerprint,
            "worker_id": worker.worker_id,
        },
    }
    return {"plan_id": _plan_id(identity), **identity}


def plan_notification_worker(
    *,
    worker_id: str,
    planned_at: datetime | str,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    selected_worker_id = safe_segment(worker_id, "worker_id")
    assert selected_worker_id is not None
    workers = load_notification_workers(worker_config_path)
    worker = workers.get(selected_worker_id)
    if worker is None:
        raise ValidationError("notification worker does not exist")
    _require_regular_file(
        delivery_config_path,
        "notification delivery configuration",
    )
    _require_regular_file(
        destination_config_path,
        "notification destination configuration",
    )
    delivery, retry_policy, retry_execution = load_retry_execution_contract(
        delivery_config_path
    )
    destinations = load_notification_destinations(destination_config_path)
    destination = destinations.get(worker.destination_id)
    if destination is None:
        raise ValidationError("notification worker destination does not exist")
    return build_notification_worker_plan(
        worker=worker,
        delivery=delivery,
        retry_policy=retry_policy,
        retry_execution=retry_execution,
        destination=destination,
        planned_at=planned_at,
    )


def validate_notification_worker_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, Mapping):
        raise ValidationError("notification worker plan must be a mapping")
    expected = {
        "plan_id",
        "blocking_reasons",
        "concurrency_control",
        "delivery",
        "destination",
        "execution",
        "model_version",
        "planned_at",
        "readiness",
        "schedule",
        "side_effects",
        "status",
        "suspension",
        "worker",
    }
    if set(plan) != expected:
        raise ValidationError("notification worker plan fields are invalid")
    if plan["model_version"] != PLAN_MODEL_VERSION:
        raise ValidationError("notification worker plan model_version is unsupported")
    if plan["status"] not in {"disabled", "blocked", "would_schedule"}:
        raise ValidationError("notification worker plan status is invalid")
    reasons = plan["blocking_reasons"]
    if not isinstance(reasons, list):
        raise ValidationError("blocking_reasons must be an array")
    if reasons != [reason for reason in BLOCKING_REASON_ORDER if reason in reasons]:
        raise ValidationError("blocking_reasons are not canonical")
    if len(reasons) != len(set(reasons)):
        raise ValidationError("blocking_reasons contain duplicates")
    side_effects = exact_mapping(
        plan["side_effects"],
        frozenset(
            {
                "acknowledgement_mutated",
                "cloud_schedule_activated",
                "database_read_performed",
                "delivery_attempt_written",
                "external_request_performed",
                "infrastructure_deployed",
                "outbox_mutated",
                "terraform_apply_performed",
            }
        ),
        "worker plan side effects",
    )
    if any(value is not False for value in side_effects.values()):
        raise ValidationError("notification worker plan reports a side effect")
    planned = aware_utc(plan["planned_at"], "planned_at")
    schedule = plan["schedule"]
    if not isinstance(schedule, Mapping):
        raise ValidationError("worker plan schedule must be a mapping")
    scheduled_for = aware_utc(schedule.get("scheduled_for"), "scheduled_for")
    if scheduled_for <= planned:
        raise ValidationError("scheduled_for must follow planned_at")
    worker = plan["worker"]
    if not isinstance(worker, Mapping):
        raise ValidationError("worker plan identity must be a mapping")
    enabled = worker.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("worker plan enabled state is invalid")
    if plan["status"] == "disabled" and enabled is not False:
        raise ValidationError("disabled plan requires a disabled worker")
    if plan["status"] == "would_schedule" and reasons:
        raise ValidationError("would_schedule plan must have no blockers")
    if plan["status"] == "blocked" and (not reasons or enabled is not True):
        raise ValidationError("blocked plan must have an enabled worker and blockers")
    identity = dict(plan)
    supplied_plan_id = identity.pop("plan_id")
    if supplied_plan_id != _plan_id(identity):
        raise ValidationError("notification worker plan_id does not match content")
    return dict(plan)


def _timestamp(value: str) -> datetime:
    try:
        return aware_utc(value, "planned_at")
    except ValidationError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    from src.orchestration.notification_worker_summary import (
        write_notification_worker_summary,
    )

    write_notification_worker_summary(path, summary)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a deterministic plan for a managed notification worker "
            "without activating a scheduler or executing delivery."
        )
    )
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--planned-at", required=True, type=_timestamp)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_workers.yaml"),
    )
    parser.add_argument(
        "--delivery-config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--destination-config",
        type=Path,
        default=Path("config/notification_destinations.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        plan = plan_notification_worker(
            worker_id=args.worker_id,
            planned_at=args.planned_at,
            worker_config_path=args.config,
            delivery_config_path=args.delivery_config,
            destination_config_path=args.destination_config,
        )
        validate_notification_worker_plan(plan)
        if args.summary_json is not None:
            _write_summary(args.summary_json, plan)
    except ValidationError as exc:
        print(f"Notification worker plan rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError as exc:
        print(f"Notification worker planning failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(plan, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
