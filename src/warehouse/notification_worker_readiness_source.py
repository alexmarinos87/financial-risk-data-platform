"""Reopen retained readiness records without confusing integrity with current authority."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.notification_worker_readiness_json import (
    MAX_SOURCE_BYTES as MAX_SOURCE_BYTES,
    bounded_source_bytes,
)

SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")


def source_identifier(value: Any) -> str:
    if not isinstance(value, str) or SAFE_ID.fullmatch(value) is None:
        raise ValidationError("readiness source identity must be bounded text")
    return value


def source_time(value: Any) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
        if not isinstance(parsed, datetime) or parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError):
        raise ValidationError("readiness source time must be timezone-aware") from None


def source_bytes(value: Mapping[str, Any]) -> bytes:
    return bounded_source_bytes(value)


def verify_worker_readiness_record(
    *, record: Mapping[str, Any], document_sha256: str, expected_record_id: str,
    destination_id: str, execution_kind: str, observed_at: datetime | str,
    max_age_seconds: int = 300,
) -> dict[str, Any]:
    """Validate the real persisted record, its digest, scope and observation-time age.

    This function does not select current history or authenticate its producer.
    Even a retained allow decision is not a current execution permission.
    """
    selected_id = source_identifier(expected_record_id)
    selected_destination = source_identifier(destination_id)
    if execution_kind not in ("initial", "retry"):
        raise ValidationError("readiness source execution kind is unsupported")
    if type(max_age_seconds) is not int or not 1 <= max_age_seconds <= 300:
        raise ValidationError("readiness source maximum age must be an integer from 1 to 300")
    if not isinstance(document_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", document_sha256) is None:
        raise ValidationError("readiness source requires a SHA-256 digest")
    instant = source_time(observed_at)
    detached = json.loads(source_bytes(record))
    # Import the existing semantic validator; do not invent a second readiness policy.
    from src.warehouse.notification_execution_readiness_history_contract import (
        canonical_notification_execution_readiness_record_bytes,
        validate_notification_execution_readiness_record,
    )

    try:
        validated = validate_notification_execution_readiness_record(detached)
        canonical = canonical_notification_execution_readiness_record_bytes(validated)
        digest = hashlib.sha256(canonical).hexdigest()
        if digest != document_sha256:
            raise ValidationError("readiness source digest differs from its retained document")
        decision = validated["decision"]
        if (validated["record_id"] != selected_id
                or decision["destination"]["destination_id"] != selected_destination
                or decision["execution_kind"] != execution_kind):
            raise ValidationError("readiness source does not match its selected identity")
        evaluated = source_time(decision["evaluated_at"])
        recorded = source_time(validated["recorded_at"])
        if evaluated > instant or recorded > instant:
            raise ValidationError("readiness source postdates its observation")
        age = (instant - evaluated).total_seconds()
        status = "stale" if age > max_age_seconds else "allowed" if decision["decision"] == "allow" else "blocked"
        return {
            "record": json.loads(canonical), "document_sha256": digest,
            "observed_at": instant.isoformat(), "age_seconds": age,
            "retained_status": status, "current_evidence_verified": False,
            "runtime_permission_granted": False,
        }
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("retained readiness source is malformed") from None
