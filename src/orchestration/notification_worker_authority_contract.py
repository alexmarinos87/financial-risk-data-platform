"""Pure, single-slot worker authority evidence; never a scheduler or delivery grant."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

from src.common.exceptions import ValidationError

MODEL_VERSION = "portfolio-risk-notification-worker-authority-transition-v1"
PLAN_MODEL_VERSION = "portfolio-risk-notification-worker-plan-v1"
LOCK_MODEL = "portfolio-risk-notification-delivery-lock-v1"
LOCK_SCOPE = "portfolio-risk-notification-delivery"
SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
REASONS = (
    "expired_review", "operator_request", "persistence_ambiguity",
    "readiness_failure", "repeated_delivery_failure",
)
CONDITIONS = [reason for reason in REASONS if reason != "operator_request"]
ENTRYPOINTS = {
    "initial": "src.orchestration.deliver_portfolio_risk_notifications",
    "retry": "src.orchestration.run_recorded_readiness_enforced_portfolio_risk_notification_retries",
}
ACTIONS = {
    "activate": ({"inactive", "disabled", "expired"}, "active"),
    "suspend": ({"active"}, "suspended"),
    "resume": ({"suspended"}, "active"),
    "disable": ({"active", "suspended", "expired"}, "disabled"),
}
PLAN_KEYS = frozenset({
    "plan_id", "blocking_reasons", "concurrency_control", "delivery", "destination",
    "execution", "model_version", "planned_at", "readiness", "schedule", "side_effects",
    "status", "suspension", "worker",
})
TRANSITION_KEYS = frozenset({
    "transition_id", "model_version", "request_id", "operator_id", "reviewed_by", "action",
    "requested_at", "effective_at", "expires_at", "previous_transition_id", "from_state",
    "to_state", "reason_codes", "plan", "plan_sha256", "scheduler_mutated",
    "external_request_performed",
})


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            dict(value), sort_keys=True, separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("worker authority must be canonical JSON") from None


def utc(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(
            value, str
        ) else value
    except ValueError:
        raise ValidationError(f"{label} must be ISO-8601") from None
    if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_ID.fullmatch(value):
        raise ValidationError(f"{label} must be one bounded safe identifier")
    return value


def _exact(value: Any, keys: Sequence[str] | frozenset[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(keys):
        raise ValidationError(f"{label} fields are not exact")
    return value


def _integer(value: Any, minimum: int, maximum: int, label: str) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(f"{label} is outside its integer bounds")
    return value


def _boolean(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean")
    return bool(value)


def validate_authority_plan(value: Mapping[str, Any]) -> dict[str, Any]:
    """Check plan semantics, not just its unkeyed content hash.

    This validates a retained planning snapshot, not current configuration or
    reviewer authentication. Execution still needs fresh under-lock readiness.
    """
    p = _exact(value, PLAN_KEYS, "worker plan")
    if p["model_version"] != PLAN_MODEL_VERSION:
        raise ValidationError("unsupported worker plan model")
    identity = dict(p)
    supplied_id = identity.pop("plan_id")
    expected_id = f"{PLAN_MODEL_VERSION}-plan-{hashlib.sha256(canonical_bytes(identity)).hexdigest()[:24]}"
    if supplied_id != expected_id:
        raise ValidationError("worker plan_id does not match content")
    worker = _exact(p["worker"], ("worker_id", "fingerprint", "enabled"), "worker identity")
    identifier(worker["worker_id"], "worker_id")
    fingerprint = identifier(worker["fingerprint"], "worker fingerprint")
    enabled = _boolean(worker["enabled"], "worker enabled")
    destination = _exact(p["destination"], (
        "activation_status", "allowed_event_types", "destination_id",
        "endpoint_environment_variable", "endpoint_value_recorded", "fingerprint",
    ), "plan destination")
    identifier(destination["destination_id"], "destination_id")
    identifier(destination["fingerprint"], "destination fingerprint")
    env = destination["endpoint_environment_variable"]
    if not isinstance(env, str) or not ENV_NAME.fullmatch(env):
        raise ValidationError("plan destination must name an environment variable")
    if destination["endpoint_value_recorded"] is not False:
        raise ValidationError("endpoint values must not be retained")
    if destination["activation_status"] not in {
        "active", "disabled", "not_yet_reviewed", "review_expired",
    }:
        raise ValidationError("invalid destination activation status")
    events = destination["allowed_event_types"]
    allowed_events = {"breach_opened", "breach_escalated", "breach_deescalated", "breach_resolved"}
    if not isinstance(events, list) or not events or any(
        not isinstance(event, str) or event not in allowed_events for event in events
    ) or events != sorted(set(events)):
        raise ValidationError("invalid plan event allow-list")
    delivery = _exact(p["delivery"], (
        "delivery_fingerprint", "max_batch_events", "retry_execution_enabled",
        "retry_execution_policy_fingerprint", "retry_planning_policy_fingerprint", "webhook_enabled",
    ), "plan delivery")
    for key in ("delivery_fingerprint", "retry_execution_policy_fingerprint", "retry_planning_policy_fingerprint"):
        identifier(delivery[key], key)
    webhook_enabled = _boolean(delivery["webhook_enabled"], "webhook enabled")
    retry_enabled = _boolean(delivery["retry_execution_enabled"], "retry enabled")
    batch = _integer(delivery["max_batch_events"], 1, 100, "delivery batch")
    execution = _exact(p["execution"], ("execution_timeout_seconds", "work_items"), "execution")
    _integer(execution["execution_timeout_seconds"], 1, 3600, "execution timeout")
    items = execution["work_items"]
    if not isinstance(items, list) or not 1 <= len(items) <= 2:
        raise ValidationError("plan must contain one or two work items")
    kinds: list[str] = []
    for item_value in items:
        item = _exact(item_value, ("entrypoint", "execution_kind", "max_events"), "work item")
        kind = identifier(item["execution_kind"], "execution kind")
        if kind not in ENTRYPOINTS or item["entrypoint"] != ENTRYPOINTS[kind]:
            raise ValidationError("unreviewed worker entrypoint")
        _integer(item["max_events"], 1, batch, "work item batch")
        kinds.append(kind)
    if kinds != sorted(set(kinds)):
        raise ValidationError("work items must be sorted and unique")
    readiness = _exact(p["readiness"], (
        "max_age_seconds", "refresh_under_shared_lock", "required_status", "source_view",
    ), "readiness")
    _integer(readiness["max_age_seconds"], 1, 300, "readiness age")
    if readiness["refresh_under_shared_lock"] is not True or readiness["required_status"] != "allowed" or readiness["source_view"] != "risk_platform.current_notification_execution_readiness_review":
        raise ValidationError("worker readiness controls cannot be weakened")
    lock = _exact(p["concurrency_control"], (
        "key_fingerprint", "lock_acquired", "max_concurrency", "model_version", "scope",
    ), "concurrency control")
    unsigned_key = int.from_bytes(hashlib.sha256(f"{LOCK_MODEL}:{LOCK_SCOPE}".encode()).digest()[:8], "big")
    signed_key = unsigned_key if unsigned_key < 2**63 else unsigned_key - 2**64
    key_fingerprint = hashlib.sha256(str(signed_key).encode("ascii")).hexdigest()[:24]
    if lock["model_version"] != LOCK_MODEL or lock["scope"] != LOCK_SCOPE or lock["key_fingerprint"] != key_fingerprint or lock["lock_acquired"] is not False:
        raise ValidationError("worker must retain the shared delivery lock identity")
    _integer(lock["max_concurrency"], 1, 1, "max_concurrency")
    suspension = _exact(p["suspension"], (
        "conditions", "cooldown_seconds", "max_consecutive_failures",
    ), "suspension")
    if suspension["conditions"] != CONDITIONS:
        raise ValidationError("worker suspension conditions cannot be weakened")
    _integer(suspension["cooldown_seconds"], 0, 86400, "cooldown")
    _integer(suspension["max_consecutive_failures"], 1, 20, "failure threshold")
    effects = _exact(p["side_effects"], (
        "acknowledgement_mutated", "cloud_schedule_activated", "database_read_performed",
        "delivery_attempt_written", "external_request_performed", "infrastructure_deployed",
        "outbox_mutated", "terraform_apply_performed",
    ), "plan side effects")
    if any(effect is not False for effect in effects.values()):
        raise ValidationError("worker plan must have no side effects")
    reasons = p["blocking_reasons"]
    if not isinstance(reasons, list):
        raise ValidationError("plan blocking reasons must be an array")
    # The endpoint value is deliberately absent. Preserve the planner's mismatch
    # evidence; current source configuration must be re-read by the execution gate.
    expected_reasons = [reason for reason, applies in (
        ("worker_disabled", not enabled), ("delivery_disabled", not webhook_enabled),
        ("retry_execution_disabled", "retry" in kinds and not retry_enabled),
        ("destination_not_active", destination["activation_status"] != "active"),
        ("endpoint_environment_mismatch", "endpoint_environment_mismatch" in reasons),
    ) if applies]
    status = "disabled" if not enabled else "blocked" if expected_reasons else "would_schedule"
    if reasons != expected_reasons or p["status"] != status:
        raise ValidationError("plan status or blockers contradict its dependencies")
    schedule = _exact(p["schedule"], (
        "activation_action", "boundary_epoch", "deterministic_jitter_seconds", "interval_seconds",
        "jitter_seconds", "mode", "scheduled_for", "timezone",
    ), "schedule")
    interval = _integer(schedule["interval_seconds"], 60, 86400, "interval")
    jitter_limit = _integer(schedule["jitter_seconds"], 0, min(3600, interval - 1), "jitter")
    planned = utc(p["planned_at"], "planned_at")
    boundary = ((int(planned.timestamp()) // interval) + 1) * interval
    seed = hashlib.sha256(f"{fingerprint}:{boundary}".encode()).digest()
    jitter = int.from_bytes(seed[:8], "big") % (jitter_limit + 1)
    _integer(schedule["boundary_epoch"], boundary, boundary, "schedule boundary")
    _integer(schedule["deterministic_jitter_seconds"], jitter, jitter, "deterministic jitter")
    if schedule["mode"] != "fixed_interval" or schedule["timezone"] != "UTC" or schedule["activation_action"] != ("would_create" if status == "would_schedule" else "none"):
        raise ValidationError("schedule contradicts the worker plan")
    if utc(schedule["scheduled_for"], "scheduled_for").timestamp() != boundary + jitter:
        raise ValidationError("schedule slot is not deterministic")
    return json.loads(canonical_bytes(p))


def authority_state(transition: Mapping[str, Any] | None, *, as_of: datetime | str) -> str:
    instant = utc(as_of, "as_of")
    if transition is None:
        return "inactive"
    document = validate_worker_authority_transition(transition)
    if instant < utc(document["effective_at"], "effective_at"):
        return "inactive"
    if document["to_state"] == "active" and instant >= utc(document["expires_at"], "expires_at"):
        return "expired"
    return str(document["to_state"])


def _build(
    *, plan: Mapping[str, Any], request_id: str, operator_id: str, reviewed_by: Sequence[str],
    action: str, requested_at: datetime | str, effective_at: datetime | str,
    expires_at: datetime | str | None, previous_transition_id: str | None,
    from_state: str, reason_codes: Sequence[str],
) -> dict[str, Any]:
    p = validate_authority_plan(plan)
    request = identifier(request_id, "request_id")
    operator = identifier(operator_id, "operator_id")
    if not isinstance(action, str) or action not in ACTIONS:
        raise ValidationError("unsupported worker authority action")
    states, target = ACTIONS[action]
    if from_state not in states:
        raise ValidationError("worker authority transition is not legal from current state")
    if previous_transition_id is None:
        if from_state != "inactive" or action != "activate":
            raise ValidationError("only initial activation may start an authority chain")
    else:
        identifier(previous_transition_id, "previous_transition_id")
        if from_state == "inactive":
            raise ValidationError("a retained authority cannot become an inactive root")
    requested = utc(requested_at, "requested_at")
    effective = utc(effective_at, "effective_at")
    if requested > effective or requested < utc(p["planned_at"], "planned_at"):
        raise ValidationError("authority timestamps precede their request or plan")
    if not isinstance(reviewed_by, (list, tuple)) or len(reviewed_by) > 10:
        raise ValidationError("reviewed_by must be a bounded reviewer array")
    reviewers = [identifier(item, "reviewer") for item in reviewed_by]
    if reviewers != sorted(reviewers) or len({item.casefold() for item in reviewers}) != len(reviewers):
        raise ValidationError("reviewer identities must be sorted and unique")
    if not isinstance(reason_codes, (list, tuple)):
        raise ValidationError("reason_codes must be an array")
    reasons = list(reason_codes)
    if reasons != [reason for reason in REASONS if reason in reasons]:
        raise ValidationError("reason_codes must be canonical and unique")
    expiry: datetime | None = None
    if target == "active":
        if p["status"] != "would_schedule":
            raise ValidationError("disabled or blocked plans cannot grant worker authority")
        if not reviewers or operator.casefold() in {item.casefold() for item in reviewers}:
            raise ValidationError("activation requires independent reviewer identities")
        if reasons:
            raise ValidationError("activation cannot retain suspension reasons")
        expiry = utc(expires_at, "expires_at")
        slot = utc(p["schedule"]["scheduled_for"], "scheduled_for")
        window_end = slot + timedelta(seconds=p["execution"]["execution_timeout_seconds"])
        if not effective <= slot < expiry <= window_end:
            raise ValidationError("active authority must cover only the exact bounded schedule slot")
    else:
        if expires_at is not None or reviewers or not reasons:
            raise ValidationError("stop actions require reasons, no grant expiry, and no reviewers")
        if action == "disable" and reasons != ["operator_request"]:
            raise ValidationError("disable requires an explicit operator_request reason")
    identity = {
        "model_version": MODEL_VERSION, "request_id": request, "operator_id": operator,
        "reviewed_by": reviewers, "action": action, "requested_at": requested.isoformat(),
        "effective_at": effective.isoformat(), "expires_at": None if expiry is None else expiry.isoformat(),
        "previous_transition_id": previous_transition_id, "from_state": from_state,
        "to_state": target, "reason_codes": reasons, "plan": p,
        "plan_sha256": hashlib.sha256(canonical_bytes(p)).hexdigest(),
        "scheduler_mutated": False, "external_request_performed": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    return {"transition_id": f"{MODEL_VERSION}-{digest}", **identity}


def validate_worker_authority_transition(value: Mapping[str, Any]) -> dict[str, Any]:
    p = _exact(value, TRANSITION_KEYS, "worker authority transition")
    rebuilt = _build(**{key: p[key] for key in (
        "plan", "request_id", "operator_id", "reviewed_by", "action", "requested_at",
        "effective_at", "expires_at", "previous_transition_id", "from_state", "reason_codes",
    )})
    if canonical_bytes(p) != canonical_bytes(rebuilt):
        raise ValidationError("worker authority transition differs from canonical evidence")
    return rebuilt


def validate_worker_authority_chain(
    transition: Mapping[str, Any], previous: Mapping[str, Any] | None,
) -> dict[str, Any]:
    current = validate_worker_authority_transition(transition)
    prior = None if previous is None else validate_worker_authority_transition(previous)
    expected_id = None if prior is None else prior["transition_id"]
    if current["previous_transition_id"] != expected_id:
        raise ValidationError("worker authority predecessor is not the exact current head")
    effective = utc(current["effective_at"], "effective_at")
    if current["from_state"] != authority_state(prior, as_of=effective):
        raise ValidationError("worker authority from_state contradicts its predecessor")
    if prior is not None:
        if effective <= utc(prior["effective_at"], "previous effective_at"):
            raise ValidationError("authority transitions must be strictly time ordered")
        if utc(current["requested_at"], "requested_at") < utc(prior["effective_at"], "previous effective_at"):
            raise ValidationError("authority request predates its predecessor")
        for group, key in (("worker", "worker_id"), ("destination", "destination_id")):
            if current["plan"][group][key] != prior["plan"][group][key]:
                raise ValidationError("authority chain cannot change worker or destination identity")
        if current["action"] in {"suspend", "disable"} and current["plan"] != prior["plan"]:
            raise ValidationError("stop actions must reference the exact previously governed plan")
        if current["action"] == "resume" and effective < (
            utc(prior["effective_at"], "suspended_at")
            + timedelta(seconds=prior["plan"]["suspension"]["cooldown_seconds"])
        ):
            raise ValidationError("resume requires the prior suspension cooldown to elapse")
        if current["request_id"] == prior["request_id"]:
            raise ValidationError("a new authority transition requires a new request_id")
    return current


def build_worker_authority_transition(
    *, plan: Mapping[str, Any], request_id: str, operator_id: str, action: str,
    requested_at: datetime | str, effective_at: datetime | str,
    reviewed_by: Sequence[str] = (), expires_at: datetime | str | None = None,
    reason_codes: Sequence[str] = (), previous: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    prior = None if previous is None else validate_worker_authority_transition(previous)
    result = _build(
        plan=plan, request_id=request_id, operator_id=operator_id, reviewed_by=reviewed_by,
        action=action, requested_at=requested_at, effective_at=effective_at, expires_at=expires_at,
        previous_transition_id=None if prior is None else prior["transition_id"],
        from_state=authority_state(prior, as_of=effective_at), reason_codes=reason_codes,
    )
    return validate_worker_authority_chain(result, prior)
