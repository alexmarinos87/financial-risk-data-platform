"""Bind suspension decisions to the existing lifecycle, without recording or executing."""
from __future__ import annotations

import hashlib
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition, canonical_bytes,
    validate_worker_authority_transition,
)
from src.orchestration.notification_worker_suspension import (
    MAX_DOCUMENT_BYTES, validate_worker_suspension_decision,
)


def build_worker_suspension_bundle(
    *, authority: Mapping[str, Any], decision: Mapping[str, Any], operator_id: str,
) -> dict[str, Any]:
    """Prepare one exact stop and its evidence. This is not a new authority model.

    Caller identity authentication and current retained-head selection are outside
    this pure boundary. The recorder must recheck the predecessor transactionally.
    """
    try:
        if len(canonical_bytes(authority)) + len(canonical_bytes(decision)) > MAX_DOCUMENT_BYTES:
            raise ValidationError("worker suspension bundle exceeds 1 MB")
        prior = validate_worker_authority_transition(authority)
        evaluated = validate_worker_suspension_decision(decision, authority=prior)
        if evaluated["outcome"] != "suspend":
            raise ValidationError("only a suspend decision may create a stop transition")
        request_id = "worker-suspension:" + hashlib.sha256(canonical_bytes(evaluated)).hexdigest()
        transition = build_worker_authority_transition(
            plan=prior["plan"], request_id=request_id, operator_id=operator_id, action="suspend",
            requested_at=evaluated["evaluated_at"], effective_at=evaluated["evaluated_at"],
            reason_codes=evaluated["reason_codes"], previous=prior,
        )
        result = {"authority": prior, "decision": evaluated, "transition": transition}
        if len(canonical_bytes(result)) > MAX_DOCUMENT_BYTES:
            raise ValidationError("worker suspension bundle exceeds 1 MB")
        return result
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker suspension bundle is malformed") from None


def validate_worker_suspension_bundle(value: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild the decision and stop, rejecting altered identities, reasons or timing."""
    try:
        if not isinstance(value, Mapping) or set(value) != {"authority", "decision", "transition"}:
            raise ValidationError("worker suspension bundle fields are not exact")
        raw = canonical_bytes(value)
        if len(raw) > MAX_DOCUMENT_BYTES:
            raise ValidationError("worker suspension bundle exceeds 1 MB")
        if not isinstance(value["transition"], Mapping):
            raise ValidationError("worker suspension transition must be an object")
        rebuilt = build_worker_suspension_bundle(
            authority=value["authority"], decision=value["decision"],
            operator_id=value["transition"]["operator_id"],
        )
        if raw != canonical_bytes(rebuilt):
            raise ValidationError("worker suspension bundle differs from canonical evidence")
        return rebuilt
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker suspension bundle is malformed") from None
