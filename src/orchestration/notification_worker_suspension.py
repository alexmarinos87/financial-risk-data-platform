"""Deterministic suspension evidence over one authority and bounded health snapshot."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    CONDITIONS, authority_state, canonical_bytes, identifier, utc,
    validate_worker_authority_transition,
)

MODEL_VERSION = "portfolio-risk-notification-worker-suspension-decision-v1"
MAX_DOCUMENT_BYTES = 1_048_576
OBSERVATION_KEYS = {
    "observation_id", "authority_transition_id", "observed_at", "review_expires_at",
    "worker_fingerprint", "readiness", "failures",
}
READINESS_KEYS = {
    "execution_kind", "record_id", "document_sha256", "destination_id",
    "destination_fingerprint", "delivery_fingerprint", "evaluated_at", "status",
}
FAILURE_KEYS = {
    "execution_kind", "history_id", "history_sha256", "observed_at",
    "consecutive_failures", "unresolved_persistence_ambiguity",
}


def _exact(value: Any, fields: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValidationError(f"worker suspension {label} fields are not exact")
    return dict(value)


def _digest(value: Any) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValidationError("worker suspension evidence requires a SHA-256 digest")
    return value


def _bounded(value: Mapping[str, Any]) -> bytes:
    raw = canonical_bytes(value)
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise ValidationError("worker suspension document exceeds 1 MB")
    return raw


def _rows(value: Any, *, fields: set[str], kinds: list[str]) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) > len(kinds):
        raise ValidationError("worker suspension rows must be bounded arrays")
    rows = [_exact(row, fields, "row") for row in value]
    names = [identifier(row["execution_kind"], "execution_kind") for row in rows]
    if names != sorted(set(names)) or any(name not in kinds for name in names):
        raise ValidationError("worker suspension kinds must be selected, sorted and unique")
    return rows


def _observation(
    value: Mapping[str, Any] | None, authority: dict[str, Any], instant: datetime,
) -> dict[str, Any] | None:
    if value is None:
        return None
    _bounded(value)
    result = _exact(value, OBSERVATION_KEYS, "observation")
    identifier(result["observation_id"], "observation_id")
    identifier(result["worker_fingerprint"], "worker_fingerprint")
    if result["authority_transition_id"] != authority["transition_id"]:
        raise ValidationError("worker suspension observation references different authority")
    observed = utc(result["observed_at"], "observed_at")
    if observed > instant:
        raise ValidationError("worker suspension observation is future-dated")
    result["observed_at"] = observed.isoformat()
    if result["review_expires_at"] is not None:
        result["review_expires_at"] = utc(result["review_expires_at"], "review_expires_at").isoformat()
    kinds = [item["execution_kind"] for item in authority["plan"]["execution"]["work_items"]]
    readiness = _rows(result["readiness"], fields=READINESS_KEYS, kinds=kinds)
    for row in readiness:
        for key in ("record_id", "destination_id", "destination_fingerprint", "delivery_fingerprint"):
            identifier(row[key], key)
        _digest(row["document_sha256"])
        if row["status"] not in {"allowed", "blocked", "stale", "superseded"}:
            raise ValidationError("worker suspension readiness status is invalid")
        evaluated = utc(row["evaluated_at"], "readiness evaluated_at")
        if evaluated > observed:
            raise ValidationError("worker suspension readiness postdates its observation")
        row["evaluated_at"] = evaluated.isoformat()
    if len({row["record_id"] for row in readiness}) != len(readiness):
        raise ValidationError("worker suspension readiness record reuse is ambiguous")
    failures = _rows(result["failures"], fields=FAILURE_KEYS, kinds=kinds)
    for row in failures:
        identifier(row["history_id"], "history_id")
        _digest(row["history_sha256"])
        count = row["consecutive_failures"]
        if type(count) is not int or not 0 <= count <= 1_000_000:
            raise ValidationError("worker suspension failure count must be a bounded integer")
        if type(row["unresolved_persistence_ambiguity"]) is not bool:
            raise ValidationError("worker suspension ambiguity must be boolean")
        evaluated = utc(row["observed_at"], "failure observed_at")
        if evaluated > observed:
            raise ValidationError("worker suspension history postdates its observation")
        row["observed_at"] = evaluated.isoformat()
    result["readiness"], result["failures"] = readiness, failures
    return json.loads(_bounded(result))


def evaluate_worker_suspension(
    *, authority: Mapping[str, Any], observation: Mapping[str, Any] | None,
    evaluated_at: datetime | str,
) -> dict[str, Any]:
    """Inspect supplied evidence; never authenticate it, grant runtime permission or resume.

    A trusted caller must select current retained authority, independently verify
    source records and produce complete per-kind history summaries. Missing
    evidence is unsafe; malformed evidence raises instead of producing approval.
    """
    try:
        _bounded(authority)
        prior = validate_worker_authority_transition(authority)
        instant = utc(evaluated_at, "evaluated_at")
        snapshot = _observation(observation, prior, instant)
        plan = prior["plan"]
        state = authority_state(prior, as_of=instant)
        reasons: set[str] = set()
        blockers: set[str] = set()

        def block(reason: str, detail: str) -> None:
            reasons.add(reason)
            blockers.add(detail)

        if state == "expired":
            block("expired_review", "authority_expired")
        if snapshot is None:
            for reason in ("expired_review", "persistence_ambiguity", "readiness_failure"):
                block(reason, "observation_missing")
        else:
            max_age = plan["readiness"]["max_age_seconds"]
            if (instant - utc(snapshot["observed_at"], "observed_at")).total_seconds() > max_age:
                block("readiness_failure", "observation_stale")
                block("persistence_ambiguity", "observation_stale")
            expiry = snapshot["review_expires_at"]
            if expiry is None or instant >= utc(expiry, "review_expires_at"):
                block("expired_review", "destination_review_missing_or_expired")
            if snapshot["worker_fingerprint"] != plan["worker"]["fingerprint"]:
                block("readiness_failure", "worker_configuration_mismatch")
            readiness = {row["execution_kind"]: row for row in snapshot["readiness"]}
            failures = {row["execution_kind"]: row for row in snapshot["failures"]}
            for item in plan["execution"]["work_items"]:
                kind = item["execution_kind"]
                row = readiness.get(kind)
                if row is None:
                    block("readiness_failure", f"readiness_missing:{kind}")
                else:
                    age = (instant - utc(row["evaluated_at"], "evaluated_at")).total_seconds()
                    if row["status"] != "allowed" or age > max_age:
                        block("readiness_failure", f"readiness_not_current_allowed:{kind}")
                    if any(row[field] != expected for field, expected in (
                        ("destination_id", plan["destination"]["destination_id"]),
                        ("destination_fingerprint", plan["destination"]["fingerprint"]),
                        ("delivery_fingerprint", plan["delivery"]["delivery_fingerprint"]),
                    )):
                        block("readiness_failure", f"readiness_configuration_mismatch:{kind}")
                history = failures.get(kind)
                if history is None:
                    block("persistence_ambiguity", f"failure_history_missing:{kind}")
                else:
                    age = (instant - utc(history["observed_at"], "observed_at")).total_seconds()
                    if history["unresolved_persistence_ambiguity"] or age > max_age:
                        block("persistence_ambiguity", f"failure_history_uncertain:{kind}")
                    if history["consecutive_failures"] >= plan["suspension"]["max_consecutive_failures"]:
                        block("repeated_delivery_failure", f"failure_threshold_reached:{kind}")
        outcome = "inactive" if state != "active" else "suspend" if reasons else "no_suspension_required"
        resume_not_before = None
        if outcome == "suspend" or state == "suspended":
            origin = instant if outcome == "suspend" else utc(prior["effective_at"], "effective_at")
            resume_not_before = (origin + timedelta(seconds=plan["suspension"]["cooldown_seconds"])).isoformat()
        identity = {
            "model_version": MODEL_VERSION, "authority_transition_id": prior["transition_id"],
            "authority_sha256": hashlib.sha256(canonical_bytes(prior)).hexdigest(),
            "worker_id": plan["worker"]["worker_id"],
            "destination_id": plan["destination"]["destination_id"],
            "evaluated_at": instant.isoformat(), "authority_state": state, "outcome": outcome,
            "reason_codes": [reason for reason in CONDITIONS if reason in reasons],
            "blocking_reasons": sorted(blockers), "observation": snapshot,
            "resume_not_before": resume_not_before, "runtime_permission_granted": False,
            "scheduler_mutated": False, "external_request_performed": False,
        }
        digest = hashlib.sha256(_bounded(identity)).hexdigest()
        result = {"decision_id": f"{MODEL_VERSION}-{digest}", **identity}
        _bounded(result)
        return result
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker suspension evidence is malformed") from None


def validate_worker_suspension_decision(
    value: Mapping[str, Any], *, authority: Mapping[str, Any],
) -> dict[str, Any]:
    """Recompute the decision from its bound inputs, not merely its content hash."""
    try:
        supplied = _bounded(value)
        rebuilt = evaluate_worker_suspension(
            authority=authority, observation=value["observation"], evaluated_at=value["evaluated_at"],
        )
        if supplied != canonical_bytes(rebuilt):
            raise ValidationError("worker suspension decision differs from canonical evidence")
        return rebuilt
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker suspension decision is malformed") from None
