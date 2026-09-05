"""Pure worker assessment over supplied observations; never runtime permission."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, canonical_bytes, identifier, validate_worker_authority_transition,
)
from src.orchestration.notification_worker_plan_validation import (
    canonical_timestamp, exact_object,
)
from src.orchestration.plan_notification_worker import validate_notification_worker_plan
from src.orchestration.portfolio_risk_notification_retry_execution_policy import bounded_integer

MODEL_VERSION = "portfolio-risk-notification-worker-readiness-assessment-v1"
MAX_EVIDENCE_BYTES = 1_048_576
EVIDENCE_FIELDS = frozenset({
    "worker_id", "evaluated_at", "observed_at", "scheduled_for",
    "selected_authority", "current_authority", "configuration_plan",
    "destination_review_expires_at", "readiness", "health",
})
READINESS_FIELDS = frozenset({
    "execution_kind", "record_id", "decision_id", "evaluated_at", "status",
    "decision", "matches_current_evidence", "destination_id", "destination_fingerprint",
    "delivery_fingerprint", "retry_policy_fingerprint", "retry_execution_policy_fingerprint",
    "endpoint_environment_variable",
})
HEALTH_FIELDS = frozenset({
    "execution_kind", "worker_id", "destination_id", "observed_at", "evidence_id",
    "evidence_complete", "consecutive_failures", "persistence_ambiguity",
})
READINESS_STATUSES = frozenset({"allowed", "blocked", "decision_stale", "decision_superseded"})


def _snapshot(value: Mapping[str, Any]) -> dict[str, Any]:
    source = exact_object(value, EVIDENCE_FIELDS, "worker evidence")
    encoded = canonical_bytes(source)
    if len(encoded) > MAX_EVIDENCE_BYTES:
        raise ValidationError("worker evidence exceeds 1 MB")
    result = json.loads(encoded)
    assert isinstance(result, dict)
    return result


def _boolean(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean")
    return bool(value)


def _transition(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or not isinstance(value.get("plan"), Mapping):
        raise ValidationError("worker authority must contain a plan object")
    validate_notification_worker_plan(value["plan"])
    return validate_worker_authority_transition(value)


def _rows(value: Any, fields: frozenset[str], kinds: list[str], label: str) -> dict[str, dict[str, Any]]:
    if not isinstance(value, list) or len(value) > 2:
        raise ValidationError(f"{label} must contain at most two observations")
    result: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for item in value:
        row = exact_object(item, fields, label)
        kind = identifier(row["execution_kind"], "execution_kind")
        if kind not in kinds:
            raise ValidationError(f"{label} contains an unconfigured execution kind")
        order.append(kind)
        result[kind] = row
    if order != sorted(set(order)):
        raise ValidationError(f"{label} kinds must be sorted and unique")
    return result


def _age_reasons(
    instant: datetime, *, observed: datetime, evaluated: datetime,
    max_age: int, label: str,
) -> list[str]:
    reasons = []
    if instant > observed or instant > evaluated:
        reasons.append(f"{label}_future")
    if (evaluated - instant).total_seconds() > max_age:
        reasons.append(f"{label}_stale")
    return reasons


def _readiness_reasons(
    row: Mapping[str, Any], *, kind: str, plan: Mapping[str, Any],
    observed: datetime, evaluated: datetime, max_age: int,
) -> list[str]:
    for key in ("record_id", "decision_id", "destination_id", "destination_fingerprint",
                "delivery_fingerprint", "retry_policy_fingerprint",
                "retry_execution_policy_fingerprint", "endpoint_environment_variable"):
        identifier(row[key], key)
    status = row["status"]
    if not isinstance(status, str) or status not in READINESS_STATUSES:
        raise ValidationError("readiness status is invalid")
    decision = row["decision"]
    if not isinstance(decision, str) or decision not in {"allow", "block"}:
        raise ValidationError("readiness decision is invalid")
    matches = _boolean(row["matches_current_evidence"], "matches_current_evidence")
    if (status == "allowed" and (decision != "allow" or not matches)
            or status == "blocked" and decision != "block"):
        raise ValidationError("readiness status contradicts its decision or current evidence")
    reasons = _age_reasons(
        canonical_timestamp(row["evaluated_at"], "readiness evaluated_at"),
        observed=observed, evaluated=evaluated, max_age=max_age, label="readiness",
    )
    if status != "allowed":
        reasons.append(f"readiness_{status}")
    if not matches and status != "decision_superseded":
        reasons.append("readiness_evidence_mismatch")
    expected = {
        "destination_id": plan["destination"]["destination_id"],
        "destination_fingerprint": plan["destination"]["fingerprint"],
        "endpoint_environment_variable": plan["destination"]["endpoint_environment_variable"],
        "delivery_fingerprint": plan["delivery"]["delivery_fingerprint"],
        "retry_policy_fingerprint": plan["delivery"]["retry_planning_policy_fingerprint"],
        "retry_execution_policy_fingerprint": plan["delivery"]["retry_execution_policy_fingerprint"],
    }
    if any(row[key] != value for key, value in expected.items()):
        reasons.append("readiness_configuration_mismatch")
    return [f"{reason}:{kind}" for reason in reasons]


def _health_reasons(
    row: Mapping[str, Any], *, kind: str, plan: Mapping[str, Any],
    observed: datetime, evaluated: datetime, max_age: int,
) -> list[str]:
    for key in ("worker_id", "destination_id", "evidence_id"):
        identifier(row[key], key)
    complete = _boolean(row["evidence_complete"], "health evidence_complete")
    ambiguous = _boolean(row["persistence_ambiguity"], "health persistence_ambiguity")
    failures = bounded_integer(
        row["consecutive_failures"], "consecutive_failures", minimum=0, maximum=1_000_000,
    )
    reasons = _age_reasons(
        canonical_timestamp(row["observed_at"], "health observed_at"),
        observed=observed, evaluated=evaluated, max_age=max_age, label="health",
    )
    if (row["worker_id"] != plan["worker"]["worker_id"]
            or row["destination_id"] != plan["destination"]["destination_id"]):
        reasons.append("health_scope_mismatch")
    if not complete:
        reasons.append("health_incomplete")
    if ambiguous:
        reasons.append("persistence_ambiguity")
    if failures >= plan["suspension"]["max_consecutive_failures"]:
        reasons.append("repeated_delivery_failure")
    return [f"{reason}:{kind}" for reason in reasons]


def _assess(evidence: Mapping[str, Any]) -> dict[str, Any]:
    supplied = _snapshot(evidence)
    worker_id = identifier(supplied["worker_id"], "worker_id")
    evaluated = canonical_timestamp(supplied["evaluated_at"], "evaluated_at")
    observed = canonical_timestamp(supplied["observed_at"], "observed_at")
    requested_slot = canonical_timestamp(supplied["scheduled_for"], "scheduled_for")
    selected = _transition(supplied["selected_authority"])
    current = _transition(supplied["current_authority"])
    configuration = validate_notification_worker_plan(supplied["configuration_plan"])
    plan = configuration if selected is None else selected["plan"]
    max_age = plan["readiness"]["max_age_seconds"]
    kinds = [item["execution_kind"] for item in plan["execution"]["work_items"]]
    state = authority_state(current, as_of=evaluated)
    reasons: list[str] = []
    if selected is None:
        reasons.append("authority_missing")
    if selected != current:
        reasons.append("authority_superseded")
    if state != "active":
        reasons.append(f"authority_{state}")
    if (configuration["worker"]["worker_id"] != worker_id
            or any(item is not None and item["plan"]["worker"]["worker_id"] != worker_id
                   for item in (selected, current))):
        reasons.append("worker_scope_mismatch")
    if any(item is not None and canonical_timestamp(item["effective_at"], "effective_at") > observed
           for item in (selected, current)):
        reasons.append("authority_not_yet_observed")
    if configuration != plan:
        reasons.append("configuration_mismatch")
    if configuration["status"] != "would_schedule":
        reasons.append("configuration_blocked")
    if canonical_timestamp(configuration["planned_at"], "planned_at") > observed:
        reasons.append("configuration_future")
    reasons.extend(_age_reasons(
        observed, observed=observed, evaluated=evaluated, max_age=max_age, label="observation",
    ))
    review_expiry = supplied["destination_review_expires_at"]
    if review_expiry is None:
        reasons.append("destination_review_missing")
    else:
        expiry = canonical_timestamp(review_expiry, "destination_review_expires_at")
        if evaluated >= expiry:
            reasons.append("destination_review_expired")
        if selected is not None and selected["expires_at"] is not None:
            if canonical_timestamp(selected["expires_at"], "authority expires_at") > expiry:
                reasons.append("authority_exceeds_destination_review")
    slot = canonical_timestamp(plan["schedule"]["scheduled_for"], "plan scheduled_for")
    if requested_slot != slot:
        reasons.append("schedule_slot_mismatch")
    if evaluated < slot:
        reasons.append("slot_not_due")
    readiness = _rows(supplied["readiness"], READINESS_FIELDS, kinds, "readiness")
    health = _rows(supplied["health"], HEALTH_FIELDS, kinds, "health")
    for kind in kinds:
        if kind not in readiness:
            reasons.append(f"readiness_missing:{kind}")
        else:
            reasons.extend(_readiness_reasons(
                readiness[kind], kind=kind, plan=plan, observed=observed,
                evaluated=evaluated, max_age=max_age,
            ))
        if kind not in health:
            reasons.append(f"health_missing:{kind}")
        else:
            reasons.extend(_health_reasons(
                health[kind], kind=kind, plan=plan, observed=observed,
                evaluated=evaluated, max_age=max_age,
            ))
    assessment = "may_run" if not reasons else "wait" if reasons == ["slot_not_due"] else "must_suspend"
    identity = {
        "model_version": MODEL_VERSION, "worker_id": worker_id,
        "evaluated_at": evaluated.isoformat(), "scheduled_for": requested_slot.isoformat(),
        "plan_id": plan["plan_id"], "authority_state": state,
        "selected_transition_id": None if selected is None else selected["transition_id"],
        "current_transition_id": None if current is None else current["transition_id"],
        "evidence_sha256": hashlib.sha256(canonical_bytes(supplied)).hexdigest(),
        "assessment": assessment, "reasons": reasons, "read_only": True,
        "runtime_permission_granted": False, "scheduler_mutated": False,
        "shared_lock_acquired": False, "external_request_performed": False,
        "database_read_performed": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    return {"assessment_id": f"{MODEL_VERSION}-{digest}", **identity}


def assess_worker_readiness(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Assess supplied snapshots only; the caller must establish their provenance.

    No file, database, environment, transport or scheduler operation occurs here.
    A later runtime must obtain consistent current evidence and recheck it under
    the shared lock. Even may_run is not execution permission or a slot claim.
    """
    try:
        return _assess(evidence)
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness evidence is malformed") from None


def validate_worker_readiness_assessment(
    assessment: Mapping[str, Any], *, evidence: Mapping[str, Any],
) -> dict[str, Any]:
    rebuilt = assess_worker_readiness(evidence)
    checked = exact_object(assessment, frozenset(rebuilt), "worker readiness assessment")
    try:
        encoded = canonical_bytes(checked)
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness assessment is malformed") from None
    if encoded != canonical_bytes(rebuilt):
        raise ValidationError("worker readiness assessment differs from supplied evidence")
    return rebuilt
