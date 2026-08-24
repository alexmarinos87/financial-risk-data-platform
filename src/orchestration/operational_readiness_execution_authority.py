from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError

MODEL_VERSION = "operational-readiness-execution-authority-v1"
PLAN_MODEL_VERSION = "readiness-aware-schedule-plan-v1"
AUTHORITY_ID_PATTERN = re.compile(
    r"^operational-readiness-execution-authority-v1-authority-[0-9a-f]{24}$"
)
PLAN_ID_PATTERN = re.compile(
    r"^readiness-aware-schedule-plan-v1-plan-[0-9a-f]{24}$"
)
DECISION_ID_PATTERN = re.compile(
    r"^operational-readiness-gate-v1-decision-[0-9a-f]{24}$"
)
OVERRIDE_ID_PATTERN = re.compile(
    r"^operational-readiness-override-v1-[0-9a-f]{24}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
AUTHORITY_KEYS = frozenset(
    {
        "authority_id",
        "model_version",
        "authority_type",
        "authorized_at",
        "plan_id",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "calendar_fingerprint",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_id",
        "mandate_fingerprint",
        "as_of_date",
        "latest_expected_session",
        "session_dates",
        "gate_id",
        "gate_fingerprint",
        "operational_policy_id",
        "operational_policy_fingerprint",
        "readiness_decision_id",
        "readiness_document_sha256",
        "readiness_decision",
        "readiness_reasons",
        "override_id",
        "override_expires_at",
    }
)


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _bounded_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def _calendar_date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if value != parsed.isoformat():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def _string_array(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ValidationError(f"{label} must be an array of text values")
    if len(value) != len(set(value)):
        raise ValidationError(f"{label} must not contain duplicates")
    return list(value)


def _session_dates(value: Any) -> list[str]:
    values = _string_array(value, "session_dates")
    parsed = [_calendar_date(item, "session_date").isoformat() for item in values]
    if parsed != sorted(parsed):
        raise ValidationError("session_dates must use ascending event-date order")
    if not parsed:
        raise ValidationError("execution authority requires at least one session")
    return parsed


def _required_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    return value


def _canonical_authority_payload(authority: Mapping[str, Any]) -> dict[str, Any]:
    return {key: authority[key] for key in sorted(AUTHORITY_KEYS - {"authority_id"})}


def _authority_id(authority: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            _canonical_authority_payload(authority),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-authority-{digest}"


def _validate_override_contract(
    override: Mapping[str, Any],
    *,
    authorized_at: datetime,
    readiness: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> tuple[str, str]:
    override_id = override.get("override_id")
    if not isinstance(override_id, str) or not OVERRIDE_ID_PATTERN.fullmatch(
        override_id
    ):
        raise ValidationError("active override ID is incompatible")
    if override.get("active") is not True:
        raise ValidationError("readiness override is not active")
    if override.get("decision_id") != readiness.get("decision_id"):
        raise ValidationError("readiness override targets another decision")
    if override.get("decision_document_sha256") != readiness.get(
        "document_sha256"
    ):
        raise ValidationError("readiness override targets another decision digest")

    schedule_plan = _required_mapping(plan.get("schedule_plan"), "schedule_plan")
    calendar = _required_mapping(schedule_plan.get("calendar"), "schedule calendar")
    expected = {
        "gate_id": readiness.get("gate_id"),
        "gate_fingerprint": readiness.get("gate_fingerprint"),
        "operational_policy_id": readiness.get("operational_policy_id"),
        "operational_policy_fingerprint": readiness.get(
            "operational_policy_fingerprint"
        ),
        "schedule_id": plan.get("schedule_id"),
        "schedule_fingerprint": plan.get("schedule_fingerprint"),
        "calendar_id": calendar.get("calendar_id"),
        "portfolio_id": plan.get("portfolio_id"),
        "risk_limit_policy_id": plan.get("risk_limit_policy_id"),
        "mandate_fingerprint": plan.get("mandate_fingerprint"),
        "latest_expected_session": plan.get("latest_expected_session"),
    }
    for key, value in expected.items():
        if override.get(key) != value:
            raise ValidationError(f"readiness override {key} does not match the plan")
    evaluated_at = _aware_utc(override.get("evaluated_at"), "override.evaluated_at")
    if evaluated_at != authorized_at:
        raise ValidationError("readiness override was evaluated at another instant")
    expires_at = _aware_utc(override.get("expires_at"), "override.expires_at")
    if expires_at <= authorized_at:
        raise ValidationError("readiness override is expired")
    return override_id, expires_at.isoformat()


def build_operational_readiness_execution_authority(
    *,
    plan: Mapping[str, Any],
    authorized_at: datetime | str,
    active_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if plan.get("model_version") != PLAN_MODEL_VERSION:
        raise ValidationError("readiness-aware plan model version is unsupported")
    plan_id = plan.get("plan_id")
    if not isinstance(plan_id, str) or not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise ValidationError("readiness-aware plan ID is incompatible")
    authorization_time = _aware_utc(authorized_at, "authorized_at")
    schedule_effect = _required_mapping(
        plan.get("schedule_effect"),
        "schedule_effect",
    )
    if schedule_effect.get("decision") not in {"would_run", "would_block"}:
        raise ValidationError("execution authority requires selected schedule work")
    session_dates = _session_dates(schedule_effect.get("session_dates"))
    if schedule_effect.get("sessions_selected") != len(session_dates):
        raise ValidationError("schedule effect session count is incompatible")

    readiness = _required_mapping(plan.get("readiness"), "readiness")
    decision_id = readiness.get("decision_id")
    if not isinstance(decision_id, str) or not DECISION_ID_PATTERN.fullmatch(
        decision_id
    ):
        raise ValidationError("execution requires a retained readiness decision")
    document_sha256 = readiness.get("document_sha256")
    if not isinstance(document_sha256, str) or not SHA256_PATTERN.fullmatch(
        document_sha256
    ):
        raise ValidationError("readiness decision digest is incompatible")
    reasons = _string_array(readiness.get("reasons"), "readiness_reasons")
    readiness_decision = readiness.get("decision")

    override_id: str | None = None
    override_expires_at: str | None = None
    if schedule_effect.get("decision") == "would_run":
        if readiness.get("status") != "current" or readiness_decision != "allow":
            raise ValidationError("would_run plan does not contain a current allow decision")
        if reasons:
            raise ValidationError("allow decision must not contain blocking reasons")
        if active_override is not None:
            raise ValidationError("allow decision must not use override authority")
        authority_type = "gate_allow"
    else:
        if readiness.get("status") != "current" or readiness_decision != "block":
            raise ValidationError("missing readiness decisions cannot be overridden")
        if not reasons:
            raise ValidationError("blocked readiness decision must contain reasons")
        if active_override is None:
            raise ValidationError("blocked readiness decision requires an active override")
        authority_type = "active_override"
        override_id, override_expires_at = _validate_override_contract(
            active_override,
            authorized_at=authorization_time,
            readiness=readiness,
            plan=plan,
        )

    schedule_plan = _required_mapping(plan.get("schedule_plan"), "schedule_plan")
    calendar = _required_mapping(schedule_plan.get("calendar"), "schedule calendar")
    authority: dict[str, Any] = {
        "authority_id": "",
        "model_version": MODEL_VERSION,
        "authority_type": authority_type,
        "authorized_at": authorization_time.isoformat(),
        "plan_id": plan_id,
        "schedule_id": _safe_segment(plan.get("schedule_id"), "schedule_id"),
        "schedule_fingerprint": _bounded_text(
            plan.get("schedule_fingerprint"),
            "schedule_fingerprint",
        ),
        "calendar_id": _safe_segment(calendar.get("calendar_id"), "calendar_id"),
        "calendar_fingerprint": _bounded_text(
            calendar.get("calendar_fingerprint"),
            "calendar_fingerprint",
        ),
        "portfolio_id": _safe_segment(plan.get("portfolio_id"), "portfolio_id"),
        "risk_limit_policy_id": _safe_segment(
            plan.get("risk_limit_policy_id"),
            "risk_limit_policy_id",
        ),
        "mandate_id": _safe_segment(plan.get("mandate_id"), "mandate_id"),
        "mandate_fingerprint": _bounded_text(
            plan.get("mandate_fingerprint"),
            "mandate_fingerprint",
        ),
        "as_of_date": _calendar_date(plan.get("as_of_date"), "as_of_date").isoformat(),
        "latest_expected_session": _calendar_date(
            plan.get("latest_expected_session"),
            "latest_expected_session",
        ).isoformat(),
        "session_dates": session_dates,
        "gate_id": _safe_segment(readiness.get("gate_id"), "gate_id"),
        "gate_fingerprint": _bounded_text(
            readiness.get("gate_fingerprint"),
            "gate_fingerprint",
        ),
        "operational_policy_id": _safe_segment(
            readiness.get("operational_policy_id"),
            "operational_policy_id",
        ),
        "operational_policy_fingerprint": _bounded_text(
            readiness.get("operational_policy_fingerprint"),
            "operational_policy_fingerprint",
        ),
        "readiness_decision_id": decision_id,
        "readiness_document_sha256": document_sha256,
        "readiness_decision": readiness_decision,
        "readiness_reasons": reasons,
        "override_id": override_id,
        "override_expires_at": override_expires_at,
    }
    authority["authority_id"] = _authority_id(authority)
    return authority


def validate_operational_readiness_execution_authority(
    authority: Mapping[str, Any] | None,
    *,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    calendar_fingerprint: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    as_of_date: date,
    latest_expected_session: date,
    session_dates: Sequence[date],
    mandate_fingerprints: Sequence[str],
) -> dict[str, Any]:
    if not isinstance(authority, Mapping) or set(authority) != AUTHORITY_KEYS:
        raise ValidationError(
            "local schedule execution requires exact operational readiness authority"
        )
    if authority.get("model_version") != MODEL_VERSION:
        raise ValidationError("execution authority model version is unsupported")
    authority_id = authority.get("authority_id")
    if not isinstance(authority_id, str) or not AUTHORITY_ID_PATTERN.fullmatch(
        authority_id
    ):
        raise ValidationError("execution authority ID is incompatible")
    plan_id = authority.get("plan_id")
    if not isinstance(plan_id, str) or not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise ValidationError("execution authority plan ID is incompatible")
    _aware_utc(authority.get("authorized_at"), "authorized_at")

    expected_sessions = [value.isoformat() for value in session_dates]
    if authority.get("session_dates") != expected_sessions:
        raise ValidationError("execution authority sessions do not match current plan")
    if authority.get("schedule_id") != schedule_id:
        raise ValidationError("execution authority belongs to another schedule")
    if authority.get("schedule_fingerprint") != schedule_fingerprint:
        raise ValidationError("execution authority schedule fingerprint is stale")
    if authority.get("calendar_id") != calendar_id:
        raise ValidationError("execution authority belongs to another calendar")
    if authority.get("calendar_fingerprint") != calendar_fingerprint:
        raise ValidationError("execution authority calendar fingerprint is stale")
    if authority.get("portfolio_id") != portfolio_id:
        raise ValidationError("execution authority belongs to another portfolio")
    if authority.get("risk_limit_policy_id") != risk_limit_policy_id:
        raise ValidationError("execution authority uses another risk-limit policy")
    if authority.get("as_of_date") != as_of_date.isoformat():
        raise ValidationError("execution authority uses another as_of_date")
    if authority.get("latest_expected_session") != latest_expected_session.isoformat():
        raise ValidationError("execution authority expected session is stale")

    expected_mandates = list(mandate_fingerprints)
    if not expected_mandates or len(set(expected_mandates)) != 1:
        raise ValidationError(
            "readiness authority currently requires one mandate across selected sessions"
        )
    if authority.get("mandate_fingerprint") != expected_mandates[0]:
        raise ValidationError("execution authority mandate fingerprint is stale")
    _safe_segment(authority.get("mandate_id"), "mandate_id")
    _safe_segment(authority.get("gate_id"), "gate_id")
    _bounded_text(authority.get("gate_fingerprint"), "gate_fingerprint")
    _safe_segment(authority.get("operational_policy_id"), "operational_policy_id")
    _bounded_text(
        authority.get("operational_policy_fingerprint"),
        "operational_policy_fingerprint",
    )
    decision_id = authority.get("readiness_decision_id")
    if not isinstance(decision_id, str) or not DECISION_ID_PATTERN.fullmatch(
        decision_id
    ):
        raise ValidationError("execution authority readiness decision is incompatible")
    digest = authority.get("readiness_document_sha256")
    if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
        raise ValidationError("execution authority readiness digest is incompatible")
    reasons = _string_array(authority.get("readiness_reasons"), "readiness_reasons")

    authority_type = authority.get("authority_type")
    if authority_type == "gate_allow":
        if authority.get("readiness_decision") != "allow" or reasons:
            raise ValidationError("gate allow authority has invalid readiness evidence")
        if authority.get("override_id") is not None or authority.get(
            "override_expires_at"
        ) is not None:
            raise ValidationError("gate allow authority must not contain override evidence")
    elif authority_type == "active_override":
        if authority.get("readiness_decision") != "block" or not reasons:
            raise ValidationError("override authority has invalid readiness evidence")
        override_id = authority.get("override_id")
        if not isinstance(override_id, str) or not OVERRIDE_ID_PATTERN.fullmatch(
            override_id
        ):
            raise ValidationError("override authority ID is incompatible")
        expires_at = _aware_utc(
            authority.get("override_expires_at"),
            "override_expires_at",
        )
        authorized_at = _aware_utc(authority.get("authorized_at"), "authorized_at")
        if expires_at <= authorized_at:
            raise ValidationError("override authority is expired")
    else:
        raise ValidationError("execution authority type is unsupported")

    canonical = dict(authority)
    if canonical["authority_id"] != _authority_id(canonical):
        raise ValidationError("execution authority ID does not match its evidence")
    return canonical
