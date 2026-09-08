"""Canonical attempt attribution for storage; no source or execution authority claim."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_attempt_observer import _receipt
from src.orchestration.notification_worker_authority_contract import (
    canonical_bytes, utc, validate_worker_authority_transition,
)
from src.orchestration.notification_worker_execution_context import validate_worker_execution_context

MODEL_VERSION = "portfolio-risk-worker-attempt-attribution-v1"
MAX_RECORD_BYTES = 1_048_576
MAX_ATTEMPT_BYTES = 8_192


def _storage_text(value: Any) -> None:
    if isinstance(value, str):
        if "\x00" in value:
            raise ValidationError("attempt attribution contains unsupported database text")
        value.encode("utf-8", errors="strict")
    elif isinstance(value, dict):
        for key, item in value.items():
            _storage_text(key)
            _storage_text(item)
    elif isinstance(value, list):
        for item in value:
            _storage_text(item)


def build_worker_attempt_attribution(
    *, authority: Mapping[str, Any], context: Mapping[str, Any], attempt: Mapping[str, Any],
) -> dict[str, Any]:
    """Bind exact sources to a reconstructed receipt, without asserting they were stored.

    Full authority/context validation and producer-shaped attempt validation are
    reused. All source hashes remain integrity evidence, not authentication.
    """
    try:
        if not isinstance(attempt, Mapping):
            raise ValidationError("attempt attribution source must be an object")
        prior = validate_worker_authority_transition(authority)
        selected = validate_worker_execution_context(context, authority=prior)
        source = {**attempt, "attempted_at": utc(attempt["attempted_at"], "attempted_at").isoformat()}
        raw = canonical_bytes(source)
        if len(raw) > MAX_ATTEMPT_BYTES:
            raise ValidationError("attempt attribution source exceeds its byte limit")
        source = json.loads(raw)
        receipt = _receipt(selected, source)
        identity = {
            "model_version": MODEL_VERSION, "authority": prior, "context": selected,
            "attempt": source, "receipt": receipt,
            "failure_history_complete": False, "runtime_permission_granted": False,
        }
        encoded = canonical_bytes(identity)
        if len(encoded) > MAX_RECORD_BYTES:
            raise ValidationError("attempt attribution record exceeds its byte limit")
        _storage_text(identity)
        digest = hashlib.sha256(encoded).hexdigest()
        result = {"attribution_id": f"{MODEL_VERSION}-{digest}", **identity}
        encoded = canonical_bytes(result)
        if len(encoded) > MAX_RECORD_BYTES:
            raise ValidationError("attempt attribution record exceeds its byte limit")
        return json.loads(encoded)
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker attempt attribution is malformed") from None


def validate_worker_attempt_attribution(value: Mapping[str, Any]) -> dict[str, Any]:
    """Reconstruct source relationships, not just a content-derived identifier."""
    try:
        if not isinstance(value, Mapping):
            raise ValidationError("attempt attribution record must be an object")
        raw = canonical_bytes(value)
        if len(raw) > MAX_RECORD_BYTES:
            raise ValidationError("attempt attribution record exceeds its byte limit")
        rebuilt = build_worker_attempt_attribution(
            authority=value["authority"], context=value["context"], attempt=value["attempt"],
        )
        if raw != canonical_bytes(rebuilt):
            raise ValidationError("attempt attribution differs from canonical source evidence")
        return rebuilt
    except (ValueError, TypeError, KeyError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker attempt attribution is malformed") from None
