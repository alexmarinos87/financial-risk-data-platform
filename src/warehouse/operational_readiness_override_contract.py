from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from src.common.exceptions import ValidationError

OVERRIDE_MODEL_VERSION = "operational-readiness-override-v1"
REVOCATION_MODEL_VERSION = "operational-readiness-override-revocation-v1"
MAX_OVERRIDE_DURATION_SECONDS = 86_400
MAX_REASON_LENGTH = 2_000
MAX_ACTOR_LENGTH = 320
MAX_REQUEST_ID_LENGTH = 128
DECISION_ID_PATTERN = re.compile(
    r"^operational-readiness-gate-v1-decision-[0-9a-f]{24}$"
)
OVERRIDE_ID_PATTERN = re.compile(
    r"^operational-readiness-override-v1-[0-9a-f]{24}$"
)
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class OperationalReadinessOverride:
    override_id: str
    model_version: str
    decision_id: str
    decision_document_sha256: str
    gate_id: str
    gate_fingerprint: str
    operational_policy_id: str
    operational_policy_fingerprint: str
    schedule_id: str
    schedule_fingerprint: str
    calendar_id: str
    portfolio_id: str
    risk_limit_policy_id: str
    mandate_fingerprint: str
    latest_expected_session: str
    request_id: str
    approved_at: datetime
    expires_at: datetime
    approved_by: str
    reason: str


@dataclass(frozen=True, slots=True)
class OperationalReadinessOverrideRevocation:
    revocation_id: str
    model_version: str
    override_id: str
    request_id: str
    revoked_at: datetime
    revoked_by: str
    reason: str


def aware_utc(value: datetime | str, label: str) -> datetime:
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


def safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def bounded_text(value: Any, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if len(parsed) > maximum or any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} must be bounded printable text")
    return parsed


def request_id(value: Any) -> str:
    return bounded_text(value, "request_id", MAX_REQUEST_ID_LENGTH)


def _bounded_fingerprint(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    return value


def _decision_text(decision: dict[str, Any], key: str) -> str:
    value = decision.get(key)
    if key in {
        "gate_id",
        "operational_policy_id",
        "schedule_id",
        "calendar_id",
        "portfolio_id",
        "risk_limit_policy_id",
    }:
        return safe_segment(value, key)
    return _bounded_fingerprint(value, key)


def build_operational_readiness_override(
    *,
    decision: dict[str, Any],
    decision_document_sha256: str,
    request_identifier: str,
    approved_at: datetime | str,
    expires_at: datetime | str,
    approved_by: str,
    reason: str,
) -> OperationalReadinessOverride:
    decision_id = decision.get("decision_id")
    if not isinstance(decision_id, str) or not DECISION_ID_PATTERN.fullmatch(
        decision_id
    ):
        raise ValidationError("decision_id is incompatible")
    if decision.get("decision") != "block":
        raise ValidationError("operational readiness overrides require a block decision")
    if not isinstance(decision_document_sha256, str) or not re.fullmatch(
        r"[0-9a-f]{64}", decision_document_sha256
    ):
        raise ValidationError("decision_document_sha256 is incompatible")

    raw_decision_evaluated_at = decision.get("evaluated_at")
    if not isinstance(raw_decision_evaluated_at, (datetime, str)):
        raise ValidationError(
            "decision.evaluated_at must be a timezone-aware timestamp"
        )
    decision_evaluated_at = aware_utc(
        raw_decision_evaluated_at,
        "decision.evaluated_at",
    )
    approval_time = aware_utc(approved_at, "approved_at")
    expiry_time = aware_utc(expires_at, "expires_at")
    if approval_time < decision_evaluated_at:
        raise ValidationError("approved_at must not predate the blocked decision")
    if expiry_time <= approval_time:
        raise ValidationError("expires_at must be after approved_at")
    if expiry_time - approval_time > timedelta(
        seconds=MAX_OVERRIDE_DURATION_SECONDS
    ):
        raise ValidationError(
            "override duration exceeds the maximum supported duration"
        )

    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(approved_by, "approved_by", MAX_ACTOR_LENGTH)
    canonical_reason = bounded_text(reason, "reason", MAX_REASON_LENGTH)
    fields = {
        "decision_id": decision_id,
        "decision_document_sha256": decision_document_sha256,
        "gate_id": _decision_text(decision, "gate_id"),
        "gate_fingerprint": _decision_text(decision, "gate_fingerprint"),
        "operational_policy_id": _decision_text(
            decision,
            "operational_policy_id",
        ),
        "operational_policy_fingerprint": _decision_text(
            decision,
            "operational_policy_fingerprint",
        ),
        "schedule_id": _decision_text(decision, "schedule_id"),
        "schedule_fingerprint": _decision_text(
            decision,
            "schedule_fingerprint",
        ),
        "calendar_id": _decision_text(decision, "calendar_id"),
        "portfolio_id": _decision_text(decision, "portfolio_id"),
        "risk_limit_policy_id": _decision_text(
            decision,
            "risk_limit_policy_id",
        ),
        "mandate_fingerprint": _decision_text(
            decision,
            "mandate_fingerprint",
        ),
        "latest_expected_session": safe_segment(
            decision.get("latest_expected_session"),
            "latest_expected_session",
        ),
        "request_id": canonical_request_id,
        "approved_at": approval_time.isoformat(),
        "expires_at": expiry_time.isoformat(),
        "approved_by": canonical_actor,
        "reason": canonical_reason,
        "model_version": OVERRIDE_MODEL_VERSION,
    }
    digest = hashlib.sha256(
        json.dumps(fields, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return OperationalReadinessOverride(
        override_id=f"{OVERRIDE_MODEL_VERSION}-{digest}",
        model_version=OVERRIDE_MODEL_VERSION,
        decision_id=decision_id,
        decision_document_sha256=decision_document_sha256,
        gate_id=fields["gate_id"],
        gate_fingerprint=fields["gate_fingerprint"],
        operational_policy_id=fields["operational_policy_id"],
        operational_policy_fingerprint=fields[
            "operational_policy_fingerprint"
        ],
        schedule_id=fields["schedule_id"],
        schedule_fingerprint=fields["schedule_fingerprint"],
        calendar_id=fields["calendar_id"],
        portfolio_id=fields["portfolio_id"],
        risk_limit_policy_id=fields["risk_limit_policy_id"],
        mandate_fingerprint=fields["mandate_fingerprint"],
        latest_expected_session=fields["latest_expected_session"],
        request_id=canonical_request_id,
        approved_at=approval_time,
        expires_at=expiry_time,
        approved_by=canonical_actor,
        reason=canonical_reason,
    )


def build_operational_readiness_override_revocation(
    *,
    override: OperationalReadinessOverride,
    request_identifier: str,
    revoked_at: datetime | str,
    revoked_by: str,
    reason: str,
) -> OperationalReadinessOverrideRevocation:
    if not OVERRIDE_ID_PATTERN.fullmatch(override.override_id):
        raise ValidationError("override_id is incompatible")
    revocation_time = aware_utc(revoked_at, "revoked_at")
    if revocation_time < override.approved_at:
        raise ValidationError("revoked_at must not predate the override")
    canonical_request_id = request_id(request_identifier)
    canonical_actor = bounded_text(revoked_by, "revoked_by", MAX_ACTOR_LENGTH)
    canonical_reason = bounded_text(reason, "reason", MAX_REASON_LENGTH)
    payload = {
        "model_version": REVOCATION_MODEL_VERSION,
        "override_id": override.override_id,
        "request_id": canonical_request_id,
        "revoked_at": revocation_time.isoformat(),
        "revoked_by": canonical_actor,
        "reason": canonical_reason,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return OperationalReadinessOverrideRevocation(
        revocation_id=f"{REVOCATION_MODEL_VERSION}-{digest}",
        model_version=REVOCATION_MODEL_VERSION,
        override_id=override.override_id,
        request_id=canonical_request_id,
        revoked_at=revocation_time,
        revoked_by=canonical_actor,
        reason=canonical_reason,
    )
