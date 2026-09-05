from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.plan_notification_worker import (
    BLOCKING_REASON_ORDER,
    CONFIG_MODEL_VERSION,
    INITIAL_ENTRYPOINT,
    MAX_CONFIG_BYTES,
    MAX_CONSECUTIVE_FAILURES,
    MAX_COOLDOWN_SECONDS,
    MAX_EXECUTION_TIMEOUT_SECONDS,
    PLAN_MODEL_VERSION,
    RETRY_ENTRYPOINT,
    _canonical_bytes,
    _parse_readiness,
    _parse_schedule,
    _plan_id,
)
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    ENVIRONMENT_NAME,
    EVENT_TYPES,
    MODEL_VERSION as DESTINATION_MODEL_VERSION,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    POLICY_MODEL_VERSION as RETRY_EXECUTION_POLICY_VERSION,
    aware_utc,
    bounded_integer,
    safe_segment,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    POLICY_MODEL_VERSION as RETRY_PLANNING_POLICY_VERSION,
)

PLAN_FIELDS = frozenset({
    "plan_id", "blocking_reasons", "concurrency_control", "delivery",
    "destination", "execution", "model_version", "planned_at", "readiness",
    "schedule", "side_effects", "status", "suspension", "worker",
})
SIDE_EFFECT_FIELDS = frozenset({
    "acknowledgement_mutated", "cloud_schedule_activated",
    "database_read_performed", "delivery_attempt_written",
    "external_request_performed", "infrastructure_deployed", "outbox_mutated",
    "terraform_apply_performed",
})
SUSPENSION_CONDITIONS = (
    "expired_review", "persistence_ambiguity", "readiness_failure",
    "repeated_delivery_failure",
)
READINESS_VIEW = "risk_platform.current_notification_execution_readiness_review"


def exact_object(value: Any, fields: frozenset[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValidationError(f"{label} fields are invalid")
    return dict(value)


def canonical_timestamp(value: Any, label: str) -> datetime:
    try:
        parsed = aware_utc(value, label)
    except (OverflowError, ValueError):
        raise ValidationError(f"{label} is outside datetime bounds") from None
    if not isinstance(value, str) or value != parsed.isoformat():
        raise ValidationError(f"{label} must use canonical UTC text")
    return parsed


def _fingerprint(value: Any, prefix: str, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(
        re.escape(prefix) + r"-[0-9a-f]{24}", value
    ) is None:
        raise ValidationError(f"{label} fingerprint is invalid")
    return value


def _boolean(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean")
    return bool(value)


def deterministic_schedule(
    worker_fingerprint: str, planned: datetime, interval: int, jitter_limit: int
) -> tuple[datetime, int, int]:
    """Compute the first future slot using integer UTC arithmetic."""
    bounds = _parse_schedule({
        "mode": "fixed_interval", "timezone": "UTC",
        "interval_seconds": interval, "jitter_seconds": jitter_limit,
    })
    planned = aware_utc(planned, "planned_at")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    elapsed = planned - epoch
    whole_seconds = elapsed.days * 86_400 + elapsed.seconds
    boundary = (whole_seconds // bounds.interval_seconds + 1) * bounds.interval_seconds
    seed = hashlib.sha256(f"{worker_fingerprint}:{boundary}".encode()).digest()
    jitter = int.from_bytes(seed[:8], "big") % (bounds.jitter_seconds + 1)
    try:
        scheduled = epoch + timedelta(seconds=boundary + jitter)
    except OverflowError:
        raise ValidationError("next worker schedule is outside datetime bounds") from None
    return scheduled, boundary, jitter


def validate_retained_notification_worker_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    """Validate internal semantics, not authenticity or current configuration."""
    source = exact_object(plan, PLAN_FIELDS, "notification worker plan")
    try:
        encoded = _canonical_bytes(source, "notification worker plan")
    except (RecursionError, UnicodeError):
        raise ValidationError("notification worker plan is not canonical JSON") from None
    if len(encoded) > MAX_CONFIG_BYTES:
        raise ValidationError("notification worker plan exceeds 1 MB")
    parsed = json.loads(encoded)
    assert isinstance(parsed, dict)
    # A detached JSON snapshot avoids aliasing the caller's nested containers.
    plan = parsed
    if plan["model_version"] != PLAN_MODEL_VERSION:
        raise ValidationError("notification worker plan model_version is unsupported")
    status = plan["status"]
    if not isinstance(status, str) or status not in {"disabled", "blocked", "would_schedule"}:
        raise ValidationError("notification worker plan status is invalid")
    planned = canonical_timestamp(plan["planned_at"], "planned_at")

    worker = exact_object(
        plan["worker"], frozenset({"enabled", "fingerprint", "worker_id"}), "worker"
    )
    enabled = _boolean(worker["enabled"], "worker enabled")
    safe_segment(worker["worker_id"], "worker_id")
    fingerprint = _fingerprint(
        worker["fingerprint"], f"{CONFIG_MODEL_VERSION}-worker", "worker"
    )

    lock = exact_object(plan["concurrency_control"], frozenset({
        "key_fingerprint", "lock_acquired", "max_concurrency", "model_version", "scope"
    }), "worker concurrency control")
    bounded_integer(lock["max_concurrency"], "max_concurrency", minimum=1, maximum=1)
    if (lock["lock_acquired"] is not False or lock["model_version"] != LOCK_MODEL_VERSION
            or lock["scope"] != LOCK_SCOPE or lock["key_fingerprint"] != LOCK_KEY_FINGERPRINT):
        raise ValidationError("worker concurrency control differs from the shared lock")

    delivery = exact_object(plan["delivery"], frozenset({
        "delivery_fingerprint", "max_batch_events", "retry_execution_enabled",
        "retry_execution_policy_fingerprint", "retry_planning_policy_fingerprint", "webhook_enabled"
    }), "worker delivery")
    delivery_enabled = _boolean(delivery["webhook_enabled"], "webhook_enabled")
    retry_enabled = _boolean(delivery["retry_execution_enabled"], "retry_execution_enabled")
    batch_limit = bounded_integer(
        delivery["max_batch_events"], "max_batch_events", minimum=1, maximum=100
    )
    _fingerprint(delivery["delivery_fingerprint"], "webhook-delivery", "delivery")
    _fingerprint(
        delivery["retry_execution_policy_fingerprint"],
        f"{RETRY_EXECUTION_POLICY_VERSION}-policy", "retry execution"
    )
    _fingerprint(
        delivery["retry_planning_policy_fingerprint"],
        f"{RETRY_PLANNING_POLICY_VERSION}-policy", "retry planning"
    )

    destination = exact_object(plan["destination"], frozenset({
        "activation_status", "allowed_event_types", "destination_id",
        "endpoint_environment_variable", "endpoint_value_recorded", "fingerprint"
    }), "worker destination")
    safe_segment(destination["destination_id"], "destination_id")
    _fingerprint(
        destination["fingerprint"], f"{DESTINATION_MODEL_VERSION}-destination", "destination"
    )
    activation = destination["activation_status"]
    if not isinstance(activation, str) or activation not in {
        "disabled", "not_yet_reviewed", "review_expired", "active"
    }:
        raise ValidationError("destination activation status is invalid")
    environment = destination["endpoint_environment_variable"]
    if not isinstance(environment, str) or not ENVIRONMENT_NAME.fullmatch(environment):
        raise ValidationError("destination environment-variable name is invalid")
    if destination["endpoint_value_recorded"] is not False:
        raise ValidationError("worker plan must not record an endpoint value")
    events = destination["allowed_event_types"]
    if (not isinstance(events, list) or not events
            or any(not isinstance(item, str) for item in events)
            or events != sorted(set(events)) or not set(events).issubset(EVENT_TYPES)):
        raise ValidationError("destination event types are invalid")

    execution = exact_object(
        plan["execution"], frozenset({"execution_timeout_seconds", "work_items"}),
        "worker execution"
    )
    bounded_integer(
        execution["execution_timeout_seconds"], "execution timeout",
        minimum=1, maximum=MAX_EXECUTION_TIMEOUT_SECONDS
    )
    items = execution["work_items"]
    if not isinstance(items, list) or not 1 <= len(items) <= 2:
        raise ValidationError("worker work_items must contain one or two entries")
    kinds: list[str] = []
    entrypoints = {"initial": INITIAL_ENTRYPOINT, "retry": RETRY_ENTRYPOINT}
    for item in items:
        work = exact_object(
            item, frozenset({"entrypoint", "execution_kind", "max_events"}), "worker work item"
        )
        kind = work["execution_kind"]
        if not isinstance(kind, str) or kind not in entrypoints:
            raise ValidationError("worker execution kind is invalid")
        if work["entrypoint"] != entrypoints[kind]:
            raise ValidationError("worker execution entrypoint is not reviewed")
        bounded_integer(work["max_events"], "work max_events", minimum=1, maximum=batch_limit)
        kinds.append(kind)
    if kinds != sorted(set(kinds)):
        raise ValidationError("worker execution kinds must be sorted and unique")

    readiness = exact_object(plan["readiness"], frozenset({
        "max_age_seconds", "refresh_under_shared_lock", "required_status", "source_view"
    }), "worker readiness")
    _parse_readiness({key: readiness[key] for key in ("required_status", "max_age_seconds")})
    if readiness["refresh_under_shared_lock"] is not True or readiness["source_view"] != READINESS_VIEW:
        raise ValidationError("worker readiness must refresh under the shared lock")
    suspension = exact_object(plan["suspension"], frozenset({
        "conditions", "cooldown_seconds", "max_consecutive_failures"
    }), "worker suspension")
    if suspension["conditions"] != list(SUSPENSION_CONDITIONS):
        raise ValidationError("worker suspension conditions are invalid")
    bounded_integer(
        suspension["cooldown_seconds"], "cooldown_seconds",
        minimum=0, maximum=MAX_COOLDOWN_SECONDS
    )
    bounded_integer(
        suspension["max_consecutive_failures"], "max_consecutive_failures",
        minimum=1, maximum=MAX_CONSECUTIVE_FAILURES
    )

    reasons = plan["blocking_reasons"]
    if (not isinstance(reasons, list) or any(not isinstance(item, str) for item in reasons)
            or reasons != [reason for reason in BLOCKING_REASON_ORDER if reason in reasons]):
        raise ValidationError("worker blocking reasons are not canonical")
    known_blockers = {
        "worker_disabled": not enabled,
        "delivery_disabled": not delivery_enabled,
        "retry_execution_disabled": "retry" in kinds and not retry_enabled,
        "destination_not_active": activation != "active",
    }
    if any((reason in reasons) != required for reason, required in known_blockers.items()):
        raise ValidationError("worker blocking reasons contradict the retained configuration")
    expected_status = "disabled" if not enabled else "blocked" if reasons else "would_schedule"
    if status != expected_status:
        raise ValidationError("worker status contradicts its enabled state or blockers")

    schedule = exact_object(plan["schedule"], frozenset({
        "activation_action", "boundary_epoch", "deterministic_jitter_seconds",
        "interval_seconds", "jitter_seconds", "mode", "scheduled_for", "timezone"
    }), "worker schedule")
    bounds = _parse_schedule({
        key: schedule[key] for key in ("mode", "interval_seconds", "jitter_seconds", "timezone")
    })
    scheduled, boundary, jitter = deterministic_schedule(
        fingerprint, planned, bounds.interval_seconds, bounds.jitter_seconds
    )
    if (type(schedule["boundary_epoch"]) is not int or schedule["boundary_epoch"] != boundary
            or type(schedule["deterministic_jitter_seconds"]) is not int
            or schedule["deterministic_jitter_seconds"] != jitter
            or canonical_timestamp(schedule["scheduled_for"], "scheduled_for") != scheduled
            or scheduled <= planned):
        raise ValidationError("worker schedule is not the deterministic next slot")
    expected_action = "would_create" if status == "would_schedule" else "none"
    if schedule["activation_action"] != expected_action:
        raise ValidationError("worker schedule activation action contradicts status")
    side_effects = exact_object(plan["side_effects"], SIDE_EFFECT_FIELDS, "worker plan side effects")
    if any(value is not False for value in side_effects.values()):
        raise ValidationError("notification worker plan reports a side effect")
    identity = dict(plan)
    supplied_id = identity.pop("plan_id")
    if supplied_id != _plan_id(identity):
        raise ValidationError("notification worker plan_id does not match content")
    return parsed
