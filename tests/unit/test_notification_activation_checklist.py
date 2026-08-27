from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.notification_activation_checklist import (
    CONTROL_NAMES,
    MODEL_VERSION,
    build_notification_activation_checklist,
    validate_notification_activation_checklist,
)

REVIEWED_AT = datetime(2026, 8, 27, 22, tzinfo=timezone.utc)
EXPIRES_AT = datetime(2026, 9, 27, 22, tzinfo=timezone.utc)


def _controls(**overrides: bool) -> dict[str, bool]:
    values = {name: True for name in CONTROL_NAMES}
    values.update(overrides)
    return values


def _build(**overrides: Any) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "destination_id": "risk-operations-webhook",
        "destination_fingerprint": "destination-fingerprint-v1",
        "authority_id": "destination-authority-v1",
        "reviewed_by": ["reviewer-b", "reviewer-a"],
        "reviewed_at": REVIEWED_AT,
        "review_expires_at": EXPIRES_AT,
        "controls": _controls(),
    }
    parameters.update(overrides)
    return build_notification_activation_checklist(**parameters)


def test_ready_checklist_is_deterministic_canonical_and_secret_free() -> None:
    first = _build()
    second = _build(reviewed_by=["reviewer-a", "reviewer-b"])

    assert first == second
    assert first["model_version"] == MODEL_VERSION
    assert first["activation_ready"] is True
    assert first["reviewed_by"] == ["reviewer-a", "reviewer-b"]
    assert validate_notification_activation_checklist(first) == first
    assert first["credential_recorded"] is False
    assert first["endpoint_value_recorded"] is False
    assert first["external_request_performed"] is False
    assert first["infrastructure_deployed"] is False

    rendered = json.dumps(first, sort_keys=True)
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "secret" not in rendered.casefold()


def test_incomplete_control_is_visible_and_changes_identity() -> None:
    ready = _build()
    incomplete = _build(
        controls=_controls(receiver_idempotency_confirmed=False),
    )

    assert incomplete["activation_ready"] is False
    assert incomplete["controls"]["receiver_idempotency_confirmed"] is False
    assert incomplete["checklist_id"] != ready["checklist_id"]
    assert validate_notification_activation_checklist(incomplete) == incomplete


def test_control_and_reviewer_contracts_fail_closed() -> None:
    missing = _controls()
    missing.pop("rollback_tested")
    with pytest.raises(ValidationError, match="missing"):
        _build(controls=missing)

    unknown = _controls()
    unknown["unreviewed_control"] = True
    with pytest.raises(ValidationError, match="unknown"):
        _build(controls=unknown)

    invalid_type: dict[str, Any] = _controls()
    invalid_type["rollback_tested"] = "yes"
    with pytest.raises(ValidationError, match="must be boolean"):
        _build(controls=invalid_type)

    with pytest.raises(ValidationError, match="no duplicates"):
        _build(reviewed_by=["reviewer-a", "reviewer-a"])

    with pytest.raises(ValidationError, match="between 1"):
        _build(reviewed_by=[])


def test_time_and_canonical_evidence_fail_closed() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        _build(reviewed_at=datetime(2026, 8, 27, 22))

    with pytest.raises(ValidationError, match="after reviewed_at"):
        _build(review_expires_at=REVIEWED_AT)

    checklist = _build()
    tampered = dict(checklist)
    tampered["activation_ready"] = False
    with pytest.raises(ValidationError, match="not canonical"):
        validate_notification_activation_checklist(tampered)

    unknown = dict(checklist)
    unknown["endpoint_url"] = "https://example.test/secret"
    with pytest.raises(ValidationError, match="unknown"):
        validate_notification_activation_checklist(unknown)
