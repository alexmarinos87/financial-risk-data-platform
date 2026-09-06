"""Reconcile verified readiness records with a worker plan and current review evidence."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    authority_state, validate_worker_authority_transition,
)
from src.warehouse.notification_worker_readiness_source import (
    source_bytes, source_identifier, source_time, verify_worker_readiness_record,
)

MODEL_VERSION = "portfolio-risk-worker-readiness-snapshot-v1"
REVIEW_FIELDS = (
    "destination_id", "execution_kind", "readiness_record_id",
    "current_destination_fingerprint", "current_authority_id", "current_checklist_id",
    "activation_review_status", "operational_activation_ready",
    "current_transition_record_id", "current_transition_rehearsal_id",
    "current_transition_review_status", "current_transition_ready",
    "current_endpoint_environment_variable",
)
SOURCE_FIELDS = {"execution_kind", "record", "document_sha256", "review"}


def _review(value: Any, *, destination_id: str, kind: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(REVIEW_FIELDS):
        raise ValidationError("worker readiness review fields are not exact")
    result = dict(value)
    if result["destination_id"] != destination_id or result["execution_kind"] != kind:
        raise ValidationError("worker readiness review scope differs from selected source")
    for field in REVIEW_FIELDS:
        if field in {"operational_activation_ready", "current_transition_ready"}:
            if type(result[field]) is not bool:
                raise ValidationError("worker readiness review flags must be boolean")
        elif result[field] is not None:
            source_identifier(result[field])
    env = result["current_endpoint_environment_variable"]
    if env is not None and re.fullmatch(r"[A-Z][A-Z0-9_]{2,127}", env) is None:
        raise ValidationError("worker readiness review must name an environment variable")
    return result


def _matches_current(decision: Mapping[str, Any], review: Mapping[str, Any]) -> bool:
    activation = decision["activation_review"] or {}
    transition = decision["transition_review"] or {}
    comparisons = (
        (review["current_destination_fingerprint"], decision["destination"]["fingerprint"]),
        (review["current_authority_id"], activation.get("authority_id")),
        (review["current_checklist_id"], activation.get("checklist_id")),
        (review["activation_review_status"], activation.get("review_status")),
        (review["operational_activation_ready"], activation.get("operational_activation_ready")),
        (review["current_transition_record_id"], transition.get("transition_record_id")),
        (review["current_transition_rehearsal_id"], transition.get("transition_rehearsal_id")),
        (review["current_transition_review_status"], transition.get("transition_review_status")),
        (review["current_transition_ready"], transition.get("transition_ready")),
        (review["current_endpoint_environment_variable"], transition.get("rollback_endpoint_environment_variable")),
    )
    return all(actual == expected for actual, expected in comparisons)


def _matches_plan(decision: Mapping[str, Any], plan: Mapping[str, Any]) -> bool:
    configuration = decision["configuration"]
    destination = decision["destination"]
    comparisons = (
        (destination["fingerprint"], plan["destination"]["fingerprint"]),
        (destination["endpoint_environment_variable"], plan["destination"]["endpoint_environment_variable"]),
        (configuration["endpoint_environment_variable"], plan["destination"]["endpoint_environment_variable"]),
        (configuration["delivery_fingerprint"], plan["delivery"]["delivery_fingerprint"]),
        (configuration["retry_policy_fingerprint"], plan["delivery"]["retry_planning_policy_fingerprint"]),
        (configuration["retry_execution_policy_fingerprint"], plan["delivery"]["retry_execution_policy_fingerprint"]),
        (configuration["delivery_enabled"], plan["delivery"]["webhook_enabled"]),
        (configuration["retry_execution_enabled"], plan["delivery"]["retry_execution_enabled"]),
    )
    return all(actual == expected for actual, expected in comparisons)


def build_worker_readiness_snapshot(
    *, authority: Mapping[str, Any], sources: list[Mapping[str, Any]],
    observed_at: datetime | str,
) -> dict[str, Any]:
    """Reconcile supplied sources; database selection and failure verification are separate.

    Missing selected kinds block. Current-view labels are not accepted as proof:
    source records are reopened and review/configuration agreement is recomputed.
    """
    try:
        if not isinstance(sources, list) or len(sources) > 2:
            raise ValidationError("worker readiness source inventory must contain at most two rows")
        detached = json.loads(source_bytes({"authority": authority, "sources": sources}))
        prior = validate_worker_authority_transition(detached["authority"])
        plan = prior["plan"]
        instant = source_time(observed_at)
        kinds = [item["execution_kind"] for item in plan["execution"]["work_items"]]
        inventory: dict[str, dict[str, Any]] = {}
        for source in detached["sources"]:
            if not isinstance(source, dict) or set(source) != SOURCE_FIELDS:
                raise ValidationError("worker readiness source fields are not exact")
            kind = source["execution_kind"]
            if not isinstance(kind, str) or kind not in kinds or kind in inventory:
                raise ValidationError("worker readiness source kind is unselected or duplicated")
            inventory[kind] = source
        destination_id = plan["destination"]["destination_id"]
        missing: list[str] = []
        reasons: list[str] = []
        rows: list[dict[str, Any]] = []
        if authority_state(prior, as_of=instant) != "active":
            reasons.append("worker_authority_not_active")
        for kind in kinds:
            source = inventory.get(kind)
            if source is None:
                missing.append(kind)
                reasons.append(f"readiness_missing:{kind}")
                continue
            review_value = source["review"]
            review = None if review_value is None else _review(review_value, destination_id=destination_id, kind=kind)
            selected_id = None if review is None else review["readiness_record_id"]
            if selected_id is None:
                if source["record"] is not None or source["document_sha256"] is not None:
                    raise ValidationError("worker readiness document has no current selected record")
                missing.append(kind)
                reasons.append(f"readiness_missing:{kind}")
                continue
            verified = verify_worker_readiness_record(
                record=source["record"], document_sha256=source["document_sha256"],
                expected_record_id=selected_id, destination_id=destination_id, execution_kind=kind,
                observed_at=instant, max_age_seconds=plan["readiness"]["max_age_seconds"],
            )
            decision = verified["record"]["decision"]
            assert review is not None
            status = verified["retained_status"]
            if not _matches_current(decision, review):
                status = "superseded"
                reasons.append(f"readiness_review_changed:{kind}")
            if not _matches_plan(decision, plan):
                status = "superseded"
                reasons.append(f"readiness_plan_mismatch:{kind}")
            if status != "allowed":
                reasons.append(f"readiness_not_allowed:{kind}")
            rows.append({
                "execution_kind": kind, "record_id": selected_id,
                "document_sha256": verified["document_sha256"],
                "destination_id": destination_id,
                "destination_fingerprint": decision["destination"]["fingerprint"],
                "delivery_fingerprint": decision["configuration"]["delivery_fingerprint"],
                "evaluated_at": decision["evaluated_at"], "status": status,
            })
        ordered_sources = [inventory[kind] for kind in kinds if kind in inventory]
        identity = {
            "model_version": MODEL_VERSION, "worker_id": plan["worker"]["worker_id"],
            "destination_id": destination_id, "authority_transition_id": prior["transition_id"],
            "authority_sha256": hashlib.sha256(source_bytes(prior)).hexdigest(),
            "sources_sha256": hashlib.sha256(source_bytes({"sources": ordered_sources})).hexdigest(),
            "observed_at": instant.isoformat(), "readiness": rows,
            "missing_execution_kinds": missing, "blocking_reasons": sorted(reasons),
            "outcome": "blocked" if reasons else "ready_sources",
            "current_authority_verified": False, "failure_history_verified": False,
            "runtime_permission_granted": False,
        }
        return {"snapshot_id": f"{MODEL_VERSION}-{hashlib.sha256(source_bytes(identity)).hexdigest()}", **identity}
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness source snapshot is malformed") from None


def validate_worker_readiness_snapshot(
    value: Mapping[str, Any], *, authority: Mapping[str, Any], sources: list[Mapping[str, Any]],
) -> dict[str, Any]:
    try:
        raw = source_bytes(value)
        rebuilt = build_worker_readiness_snapshot(authority=authority, sources=sources, observed_at=value["observed_at"])
        if raw != source_bytes(rebuilt):
            raise ValidationError("worker readiness snapshot differs from its source evidence")
        return rebuilt
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker readiness snapshot is malformed") from None
