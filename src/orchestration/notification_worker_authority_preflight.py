"""Pure authority/slot preflight, separate from health and suspension policy."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, canonical_bytes, identifier, validate_worker_authority_transition,
)
from src.orchestration.notification_worker_plan_validation import (
    canonical_timestamp, exact_object,
)
from src.orchestration.plan_notification_worker import validate_notification_worker_plan

MODEL_VERSION = "portfolio-risk-notification-worker-authority-preflight-v1"
MAX_EVIDENCE_BYTES = 1_048_576
EVIDENCE_FIELDS = frozenset({
    "worker_id", "selected_transition_id", "evaluated_at", "observed_at",
    "scheduled_for", "current_authority", "configuration_plan",
    "destination_review_expires_at",
})


def _evaluate(evidence: Mapping[str, Any]) -> dict[str, Any]:
    source = exact_object(evidence, EVIDENCE_FIELDS, "worker preflight evidence")
    encoded = canonical_bytes(source)
    if len(encoded) > MAX_EVIDENCE_BYTES:
        raise ValidationError("worker preflight evidence exceeds 1 MB")
    snapshot = json.loads(encoded)
    worker_id = identifier(snapshot["worker_id"], "worker_id")
    selected_id = identifier(snapshot["selected_transition_id"], "selected_transition_id")
    evaluated = canonical_timestamp(snapshot["evaluated_at"], "evaluated_at")
    observed = canonical_timestamp(snapshot["observed_at"], "observed_at")
    selected_slot = canonical_timestamp(snapshot["scheduled_for"], "scheduled_for")
    current = snapshot["current_authority"]
    if current is not None:
        if not isinstance(current, Mapping) or not isinstance(current.get("plan"), Mapping):
            raise ValidationError("current worker authority must contain a plan object")
        validate_notification_worker_plan(current["plan"])
        current = validate_worker_authority_transition(current)
    configuration = snapshot["configuration_plan"]
    if configuration is not None:
        configuration = validate_notification_worker_plan(configuration)
    plan = configuration if current is None else current["plan"]
    max_age = 300 if plan is None else plan["readiness"]["max_age_seconds"]
    state = authority_state(current, as_of=evaluated)
    reasons: list[str] = []
    if current is None:
        reasons.append("authority_missing")
    else:
        if selected_id != current["transition_id"]:
            reasons.append("authority_superseded")
        if current["plan"]["worker"]["worker_id"] != worker_id:
            reasons.append("worker_scope_mismatch")
        if canonical_timestamp(current["effective_at"], "effective_at") > observed:
            reasons.append("authority_not_yet_observed")
    if state != "active":
        reasons.append(f"authority_{state}")
    if configuration is None:
        reasons.append("configuration_unverified")
    else:
        if configuration["worker"]["worker_id"] != worker_id:
            reasons.append("configuration_scope_mismatch")
        if current is not None and configuration != current["plan"]:
            reasons.append("configuration_mismatch")
        if configuration["status"] != "would_schedule":
            reasons.append("configuration_blocked")
        if canonical_timestamp(configuration["planned_at"], "planned_at") > observed:
            reasons.append("configuration_future")
    if observed > evaluated:
        reasons.append("observation_future")
    if (evaluated - observed).total_seconds() > max_age:
        reasons.append("observation_stale")
    review_expiry = snapshot["destination_review_expires_at"]
    if review_expiry is None:
        reasons.append("destination_review_missing")
    else:
        expiry = canonical_timestamp(review_expiry, "destination_review_expires_at")
        if evaluated >= expiry:
            reasons.append("destination_review_expired")
        if current is not None and current["expires_at"] is not None:
            if canonical_timestamp(current["expires_at"], "authority expires_at") > expiry:
                reasons.append("authority_exceeds_destination_review")
    if plan is not None:
        slot = canonical_timestamp(plan["schedule"]["scheduled_for"], "plan scheduled_for")
        if selected_slot != slot:
            reasons.append("schedule_slot_mismatch")
        if evaluated < slot:
            reasons.append("slot_not_due")
    outcome = "eligible_for_health_review" if not reasons else "wait" if reasons == ["slot_not_due"] else "blocked"
    identity = {
        "model_version": MODEL_VERSION, "worker_id": worker_id,
        "evaluated_at": evaluated.isoformat(), "scheduled_for": selected_slot.isoformat(),
        "selected_transition_id": selected_id,
        "current_transition_id": None if current is None else current["transition_id"],
        "plan_id": None if plan is None else plan["plan_id"],
        "authority_state": state, "outcome": outcome, "reasons": reasons,
        "evidence_sha256": hashlib.sha256(encoded).hexdigest(),
        "readiness_evaluated": False, "runtime_permission_granted": False,
        "scheduler_mutated": False, "shared_lock_acquired": False,
        "external_request_performed": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    return {"preflight_id": f"{MODEL_VERSION}-{digest}", **identity}


def evaluate_worker_authority_preflight(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Check supplied authority/configuration snapshots without I/O.

    The producer must select the actual current head and reconstruct the plan
    and review expiry from one immutable reviewed configuration snapshot. This
    function verifies their agreement, not their provenance. A passing result
    is only eligible for separate health evaluation, never permission to run.
    """
    try:
        return _evaluate(evidence)
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker preflight evidence is malformed") from None


def validate_worker_authority_preflight(
    value: Mapping[str, Any], *, evidence: Mapping[str, Any],
) -> dict[str, Any]:
    rebuilt = evaluate_worker_authority_preflight(evidence)
    checked = exact_object(value, frozenset(rebuilt), "worker authority preflight")
    try:
        encoded = canonical_bytes(checked)
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker authority preflight is malformed") from None
    if encoded != canonical_bytes(rebuilt):
        raise ValidationError("worker authority preflight differs from supplied evidence")
    return rebuilt
