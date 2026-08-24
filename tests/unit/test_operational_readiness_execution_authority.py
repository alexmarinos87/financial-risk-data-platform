from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.orchestration.operational_readiness_execution_authority import (
    build_operational_readiness_execution_authority,
    validate_operational_readiness_execution_authority,
)


def _plan(*, effect: str = "would_run", decision: str = "allow") -> dict[str, Any]:
    reasons = [] if decision == "allow" else ["report_status_critical"]
    return {
        "plan_id": "readiness-aware-schedule-plan-v1-plan-" + "a" * 24,
        "model_version": "readiness-aware-schedule-plan-v1",
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_id": "us-tech-2026",
        "mandate_fingerprint": "portfolio-mandate-example",
        "as_of_date": "2026-01-10",
        "latest_expected_session": "2026-01-09",
        "readiness": {
            "status": "current",
            "decision_id": "operational-readiness-gate-v1-decision-" + "b" * 24,
            "decision": decision,
            "reasons": reasons,
            "document_sha256": "c" * 64,
            "gate_id": "us-tech-local",
            "gate_fingerprint": "operational-readiness-gate-" + "d" * 24,
            "operational_policy_id": "us-tech-local",
            "operational_policy_fingerprint": "operational-slo-policy-" + "e" * 24,
        },
        "schedule_plan": {
            "calendar": {
                "calendar_id": "XNYS",
                "calendar_fingerprint": "market-calendar-example",
            },
        },
        "schedule_effect": {
            "decision": effect,
            "sessions_selected": 1,
            "session_dates": ["2026-01-09"],
        },
    }


def _override() -> dict[str, Any]:
    return {
        "override_id": "operational-readiness-override-v1-" + "f" * 24,
        "active": True,
        "decision_id": "operational-readiness-gate-v1-decision-" + "b" * 24,
        "decision_document_sha256": "c" * 64,
        "gate_id": "us-tech-local",
        "gate_fingerprint": "operational-readiness-gate-" + "d" * 24,
        "operational_policy_id": "us-tech-local",
        "operational_policy_fingerprint": "operational-slo-policy-" + "e" * 24,
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "calendar_id": "XNYS",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_fingerprint": "portfolio-mandate-example",
        "latest_expected_session": "2026-01-09",
        "evaluated_at": "2026-01-10T12:00:00+00:00",
        "expires_at": "2026-01-10T13:00:00+00:00",
    }


def _validate(authority: dict[str, Any]) -> dict[str, Any]:
    return validate_operational_readiness_execution_authority(
        authority,
        schedule_id="us-tech-local",
        schedule_fingerprint="local-schedule-example",
        calendar_id="XNYS",
        calendar_fingerprint="market-calendar-example",
        portfolio_id="us-tech-equal",
        risk_limit_policy_id="us-tech-standard",
        as_of_date=date(2026, 1, 10),
        latest_expected_session=date(2026, 1, 9),
        session_dates=(date(2026, 1, 9),),
        mandate_fingerprints=("portfolio-mandate-example",),
    )


def test_gate_allow_authority_is_deterministic_and_validates() -> None:
    first = build_operational_readiness_execution_authority(
        plan=_plan(),
        authorized_at="2026-01-10T12:00:00Z",
    )
    second = build_operational_readiness_execution_authority(
        plan=_plan(),
        authorized_at=datetime(2026, 1, 10, 12, tzinfo=timezone.utc),
    )

    assert first == second
    assert first["authority_type"] == "gate_allow"
    assert first["override_id"] is None
    assert _validate(first) == first


def test_blocked_decision_requires_exact_active_override() -> None:
    plan = _plan(effect="would_block", decision="block")
    with pytest.raises(ValidationError, match="active override"):
        build_operational_readiness_execution_authority(
            plan=plan,
            authorized_at="2026-01-10T12:00:00Z",
        )

    authority = build_operational_readiness_execution_authority(
        plan=plan,
        authorized_at="2026-01-10T12:00:00Z",
        active_override=_override(),
    )
    assert authority["authority_type"] == "active_override"
    assert authority["override_id"] == _override()["override_id"]
    assert _validate(authority) == authority

    wrong = _override()
    wrong["mandate_fingerprint"] = "other-mandate"
    with pytest.raises(ValidationError, match="mandate_fingerprint"):
        build_operational_readiness_execution_authority(
            plan=plan,
            authorized_at="2026-01-10T12:00:00Z",
            active_override=wrong,
        )


def test_missing_decision_and_expired_override_cannot_authorize() -> None:
    missing = _plan(effect="would_block", decision="block")
    missing["readiness"]["status"] = "missing"
    missing["readiness"]["decision_id"] = None
    missing["readiness"]["document_sha256"] = None
    with pytest.raises(ValidationError, match="retained readiness decision"):
        build_operational_readiness_execution_authority(
            plan=missing,
            authorized_at="2026-01-10T12:00:00Z",
            active_override=_override(),
        )

    expired = _override()
    expired["expires_at"] = "2026-01-10T12:00:00Z"
    with pytest.raises(ValidationError, match="expired"):
        build_operational_readiness_execution_authority(
            plan=_plan(effect="would_block", decision="block"),
            authorized_at="2026-01-10T12:00:00Z",
            active_override=expired,
        )


def test_base_validation_rejects_stale_sessions_and_tampered_identity() -> None:
    authority = build_operational_readiness_execution_authority(
        plan=_plan(),
        authorized_at="2026-01-10T12:00:00Z",
    )
    with pytest.raises(ValidationError, match="sessions"):
        validate_operational_readiness_execution_authority(
            authority,
            schedule_id="us-tech-local",
            schedule_fingerprint="local-schedule-example",
            calendar_id="XNYS",
            calendar_fingerprint="market-calendar-example",
            portfolio_id="us-tech-equal",
            risk_limit_policy_id="us-tech-standard",
            as_of_date=date(2026, 1, 10),
            latest_expected_session=date(2026, 1, 9),
            session_dates=(date(2026, 1, 8),),
            mandate_fingerprints=("portfolio-mandate-example",),
        )

    authority["authority_id"] = (
        "operational-readiness-execution-authority-v1-authority-" + "0" * 24
    )
    with pytest.raises(ValidationError, match="does not match"):
        _validate(authority)


def test_base_validation_rejects_multiple_mandates_in_one_authority() -> None:
    authority = build_operational_readiness_execution_authority(
        plan=_plan(),
        authorized_at="2026-01-10T12:00:00Z",
    )
    with pytest.raises(ValidationError, match="one mandate"):
        validate_operational_readiness_execution_authority(
            authority,
            schedule_id="us-tech-local",
            schedule_fingerprint="local-schedule-example",
            calendar_id="XNYS",
            calendar_fingerprint="market-calendar-example",
            portfolio_id="us-tech-equal",
            risk_limit_policy_id="us-tech-standard",
            as_of_date=date(2026, 1, 10),
            latest_expected_session=date(2026, 1, 9),
            session_dates=(date(2026, 1, 8), date(2026, 1, 9)),
            mandate_fingerprints=("mandate-a", "mandate-b"),
        )
