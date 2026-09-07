"""Attribution context for one worker kind and slot; never execution permission."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    canonical_bytes, identifier, utc, validate_worker_authority_transition,
)

MODEL_VERSION = "portfolio-risk-worker-execution-context-v1"
MAX_AUTHORITY_BYTES = 1_048_576
MAX_CONTEXT_BYTES = 16_384


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def build_worker_execution_context(
    *, authority: Mapping[str, Any], request_id: str, execution_kind: str,
    started_at: datetime | str,
) -> dict[str, Any]:
    """Derive attribution from a validated snapshot, not current database authority.

    Request identity and start time describe a caller's proposed invocation. They
    do not claim a slot, prove readiness, authenticate the caller or execute work.
    """
    try:
        if not isinstance(authority, Mapping) or len(canonical_bytes(authority)) > MAX_AUTHORITY_BYTES:
            raise ValidationError("execution context authority exceeds its document bound")
        prior = validate_worker_authority_transition(authority)
        request = identifier(request_id, "execution request_id")
        kind = identifier(execution_kind, "execution_kind")
        plan = prior["plan"]
        items = {item["execution_kind"]: item for item in plan["execution"]["work_items"]}
        if kind not in items:
            raise ValidationError("execution context kind is not selected by its plan")
        if prior["to_state"] != "active":
            raise ValidationError("execution context requires an active authority snapshot")
        started = utc(started_at, "started_at")
        slot = utc(plan["schedule"]["scheduled_for"], "scheduled_for")
        expiry = utc(prior["expires_at"], "expires_at")
        if not slot <= started < expiry:
            raise ValidationError("execution context start is outside its exact authority slot")
        scope = {
            "worker_id": plan["worker"]["worker_id"],
            "authority_transition_id": prior["transition_id"],
            "scheduled_for": slot.isoformat(), "execution_kind": kind,
        }
        identity = {
            "model_version": MODEL_VERSION, **scope,
            "scope_id": f"{MODEL_VERSION}-scope-{_digest(scope)}",
            "request_id": request, "started_at": started.isoformat(),
            "expires_at": expiry.isoformat(), "authority_sha256": _digest(prior),
            "plan_id": plan["plan_id"], "worker_fingerprint": plan["worker"]["fingerprint"],
            "destination_id": plan["destination"]["destination_id"],
            "destination_fingerprint": plan["destination"]["fingerprint"],
            "delivery_fingerprint": plan["delivery"]["delivery_fingerprint"],
            "retry_planning_policy_fingerprint": plan["delivery"]["retry_planning_policy_fingerprint"],
            "retry_execution_policy_fingerprint": plan["delivery"]["retry_execution_policy_fingerprint"],
            "entrypoint": items[kind]["entrypoint"], "max_events": items[kind]["max_events"],
            "current_authority_verified": False, "readiness_verified": False,
            "slot_claimed": False, "runtime_permission_granted": False,
        }
        result = {"context_id": f"{MODEL_VERSION}-{_digest(identity)}", **identity}
        if len(canonical_bytes(result)) > MAX_CONTEXT_BYTES:
            raise ValidationError("execution context exceeds its document bound")
        return result
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker execution context is malformed") from None


def validate_worker_execution_context(
    value: Mapping[str, Any], *, authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Reconstruct every projected field against the exact supplied authority."""
    try:
        if not isinstance(value, Mapping):
            raise ValidationError("worker execution context must be an object")
        raw = canonical_bytes(value)
        if len(raw) > MAX_CONTEXT_BYTES:
            raise ValidationError("execution context exceeds its document bound")
        rebuilt = build_worker_execution_context(
            authority=authority, request_id=value["request_id"],
            execution_kind=value["execution_kind"], started_at=value["started_at"],
        )
        if raw != canonical_bytes(rebuilt):
            raise ValidationError("execution context differs from canonical authority evidence")
        return rebuilt
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker execution context is malformed") from None
