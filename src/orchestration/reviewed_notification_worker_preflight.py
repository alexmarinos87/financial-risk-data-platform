"""Bind captured authority preflight to reviewed immutable configuration files."""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import canonical_bytes, identifier, utc
from src.orchestration.notification_worker_authority_preflight import (
    EVIDENCE_FIELDS, evaluate_worker_authority_preflight,
)
from src.orchestration.reviewed_notification_worker_authority import _bind_reviewed_plan
from src.warehouse.notification_worker_authority_snapshot import validate_worker_authority_snapshot

MODEL_VERSION = "portfolio-risk-reviewed-worker-preflight-v1"
MAX_BUNDLE_BYTES = 1_048_576
BUNDLE_FIELDS = frozenset({
    "bundle_id", "model_version", "snapshot", "evidence", "preflight",
    "evaluation_scope", "configuration_validated", "runtime_permission_granted",
})


def build_reviewed_worker_preflight(
    *, snapshot: Mapping[str, Any], selected_transition_id: str, scheduled_for: str,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Evaluate the captured database instant; do not claim it is still current.

    Files must be immutable reviewed snapshots in trusted directories. Missing
    authority produces a blocked result without reading configuration. A changed
    configuration raises ValidationError before producing any passing result.
    """
    captured = validate_worker_authority_snapshot(snapshot)
    selected = identifier(selected_transition_id, "selected_transition_id")
    slot = utc(scheduled_for, "scheduled_for").isoformat()
    current = captured["transition"]
    plan = None
    review_expiry = None
    if current is not None:
        plan, destination = _bind_reviewed_plan(
            current["plan"], worker_config_path=worker_config_path,
            delivery_config_path=delivery_config_path,
            destination_config_path=destination_config_path,
        )
        expiry = destination.activation.review_expires_at
        review_expiry = None if expiry is None else expiry.isoformat()
    evidence = {
        "worker_id": captured["worker_id"], "selected_transition_id": selected,
        "evaluated_at": captured["observed_at"], "observed_at": captured["observed_at"],
        "scheduled_for": slot, "current_authority": current,
        "configuration_plan": plan, "destination_review_expires_at": review_expiry,
    }
    identity = {
        "model_version": MODEL_VERSION, "snapshot": captured, "evidence": evidence,
        "preflight": evaluate_worker_authority_preflight(evidence),
        "evaluation_scope": "captured_database_instant",
        "configuration_validated": current is not None, "runtime_permission_granted": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    result = {"bundle_id": f"{MODEL_VERSION}-{digest}", **identity}
    encoded = canonical_bytes(result)
    if len(encoded) > MAX_BUNDLE_BYTES:
        raise ValidationError("reviewed worker preflight bundle exceeds 1 MB")
    return dict(json.loads(encoded))


def validate_reviewed_worker_preflight(
    value: Mapping[str, Any], *,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Reopen supplied reviewed files and rebuild every retained output field."""
    try:
        if not isinstance(value, Mapping) or set(value) != BUNDLE_FIELDS:
            raise ValidationError("reviewed worker preflight fields are not exact")
        encoded = canonical_bytes(value)
        if len(encoded) > MAX_BUNDLE_BYTES:
            raise ValidationError("reviewed worker preflight bundle exceeds 1 MB")
        source = json.loads(encoded)
        evidence = source["evidence"]
        if not isinstance(evidence, Mapping) or set(evidence) != EVIDENCE_FIELDS:
            raise ValidationError("reviewed worker preflight evidence fields are not exact")
        rebuilt = build_reviewed_worker_preflight(
            snapshot=source["snapshot"], selected_transition_id=evidence["selected_transition_id"],
            scheduled_for=evidence["scheduled_for"], worker_config_path=worker_config_path,
            delivery_config_path=delivery_config_path,
            destination_config_path=destination_config_path,
        )
        if encoded != canonical_bytes(rebuilt):
            raise ValidationError("reviewed worker preflight differs from bound source evidence")
        return rebuilt
    except (KeyError, TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("reviewed worker preflight is malformed") from None
