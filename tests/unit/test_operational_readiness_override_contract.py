from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_readiness_override_contract import (
    MAX_OVERRIDE_DURATION_SECONDS,
    build_operational_readiness_override,
    build_operational_readiness_override_revocation,
)
from src.warehouse.operational_readiness_override_registry import (
    read_active_operational_readiness_override,
)


def _decision(*, result: str = "block") -> dict[str, Any]:
    return {
        "decision_id": "operational-readiness-gate-v1-decision-" + "a" * 24,
        "decision": result,
        "evaluated_at": "2026-04-01T12:00:00+00:00",
        "gate_id": "us-tech-local",
        "gate_fingerprint": "operational-readiness-gate-" + "b" * 24,
        "operational_policy_id": "us-tech-local",
        "operational_policy_fingerprint": "operational-slo-policy-" + "c" * 24,
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "calendar_id": "XNYS",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_fingerprint": "portfolio-mandate-example",
        "latest_expected_session": "2026-03-31",
    }


def test_override_identity_is_deterministic_and_time_bounded() -> None:
    first = build_operational_readiness_override(
        decision=_decision(),
        decision_document_sha256="d" * 64,
        request_identifier="OVERRIDE-001",
        approved_at="2026-04-01T12:05:00Z",
        expires_at="2026-04-01T13:05:00Z",
        approved_by="operator@example.test",
        reason="Reviewed one bounded local execution exception.",
    )
    second = build_operational_readiness_override(
        decision=_decision(),
        decision_document_sha256="d" * 64,
        request_identifier="OVERRIDE-001",
        approved_at=datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc),
        expires_at=datetime(2026, 4, 1, 13, 5, tzinfo=timezone.utc),
        approved_by="operator@example.test",
        reason="Reviewed one bounded local execution exception.",
    )

    assert first == second
    assert first.override_id.startswith("operational-readiness-override-v1-")
    assert first.expires_at > first.approved_at


def test_override_rejects_allow_predating_and_excess_duration() -> None:
    common = {
        "decision_document_sha256": "d" * 64,
        "request_identifier": "OVERRIDE-001",
        "approved_by": "operator@example.test",
        "reason": "Reviewed one bounded local execution exception.",
    }
    with pytest.raises(ValidationError, match="block decision"):
        build_operational_readiness_override(
            decision=_decision(result="allow"),
            approved_at="2026-04-01T12:05:00Z",
            expires_at="2026-04-01T13:05:00Z",
            **common,
        )
    with pytest.raises(ValidationError, match="predate"):
        build_operational_readiness_override(
            decision=_decision(),
            approved_at="2026-04-01T11:59:00Z",
            expires_at="2026-04-01T12:30:00Z",
            **common,
        )
    with pytest.raises(ValidationError, match="maximum"):
        build_operational_readiness_override(
            decision=_decision(),
            approved_at="2026-04-01T12:05:00Z",
            expires_at=(
                datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc)
                + timedelta(seconds=MAX_OVERRIDE_DURATION_SECONDS + 1)
            ),
            **common,
        )


def test_revocation_identity_is_deterministic_and_cannot_predate_override() -> None:
    override = build_operational_readiness_override(
        decision=_decision(),
        decision_document_sha256="d" * 64,
        request_identifier="OVERRIDE-001",
        approved_at="2026-04-01T12:05:00Z",
        expires_at="2026-04-01T13:05:00Z",
        approved_by="operator@example.test",
        reason="Reviewed one bounded local execution exception.",
    )
    revocation = build_operational_readiness_override_revocation(
        override=override,
        request_identifier="OVERRIDE-REVOKE-001",
        revoked_at="2026-04-01T12:30:00Z",
        revoked_by="operator@example.test",
        reason="Override withdrawn after review.",
    )
    assert revocation.revocation_id.startswith(
        "operational-readiness-override-revocation-v1-"
    )
    with pytest.raises(ValidationError, match="predate"):
        build_operational_readiness_override_revocation(
            override=override,
            request_identifier="OVERRIDE-REVOKE-002",
            revoked_at="2026-04-01T12:00:00Z",
            revoked_by="operator@example.test",
            reason="Invalid early revocation.",
        )


def _current_row(
    *,
    revoked_at: datetime | None = None,
) -> dict[str, Any]:
    return {
        "override_id": "operational-readiness-override-v1-" + "e" * 24,
        "model_version": "operational-readiness-override-v1",
        "decision_id": _decision()["decision_id"],
        "decision_document_sha256": "d" * 64,
        "gate_id": "us-tech-local",
        "gate_fingerprint": "operational-readiness-gate-" + "b" * 24,
        "operational_policy_id": "us-tech-local",
        "operational_policy_fingerprint": "operational-slo-policy-" + "c" * 24,
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "calendar_id": "XNYS",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_fingerprint": "portfolio-mandate-example",
        "latest_expected_session": date(2026, 3, 31),
        "request_id": "OVERRIDE-001",
        "approved_at": datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc),
        "expires_at": datetime(2026, 4, 1, 13, 5, tzinfo=timezone.utc),
        "approved_by": "operator@example.test",
        "revocation_id": (
            "operational-readiness-override-revocation-v1-" + "f" * 24
            if revoked_at is not None
            else None
        ),
        "revoked_at": revoked_at,
    }


def test_active_override_lookup_uses_explicit_evaluation_time() -> None:
    decision_id = str(_decision()["decision_id"])
    active = read_active_operational_readiness_override(
        dsn="not-used",
        decision_id=decision_id,
        evaluated_at="2026-04-01T12:30:00Z",
        row_reader=lambda **_: [_current_row()],
    )
    expired = read_active_operational_readiness_override(
        dsn="not-used",
        decision_id=decision_id,
        evaluated_at="2026-04-01T13:05:00Z",
        row_reader=lambda **_: [_current_row()],
    )
    revoked = read_active_operational_readiness_override(
        dsn="not-used",
        decision_id=decision_id,
        evaluated_at="2026-04-01T12:30:00Z",
        row_reader=lambda **_: [
            _current_row(
                revoked_at=datetime(2026, 4, 1, 12, 20, tzinfo=timezone.utc)
            )
        ],
    )

    assert active is not None and active["active"] is True
    assert expired is None
    assert revoked is None


def test_active_override_lookup_rejects_duplicate_current_grain() -> None:
    with pytest.raises(StorageError, match="not unique"):
        read_active_operational_readiness_override(
            dsn="not-used",
            decision_id=str(_decision()["decision_id"]),
            evaluated_at="2026-04-01T12:30:00Z",
            row_reader=lambda **_: [{}, {}],
        )
