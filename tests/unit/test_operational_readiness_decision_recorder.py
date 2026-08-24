from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_readiness_decision_recorder import (
    canonical_operational_readiness_decision_bytes,
    read_operational_readiness_decision,
    validate_operational_readiness_decision,
)
from src.warehouse.operational_readiness_gate import (
    OperationalReadinessGatePolicy,
    evaluate_operational_readiness,
)


def _policy(*, allow_warning: bool = True) -> OperationalReadinessGatePolicy:
    return OperationalReadinessGatePolicy(
        gate_id="us-tech-local",
        operational_policy_id="us-tech-local",
        max_report_age_seconds=3600,
        allow_warning=allow_warning,
    )


def _report(
    *,
    as_of: datetime,
    status: str = "ok",
    latest_session: date = date(2026, 3, 31),
) -> dict[str, object]:
    return {
        "calculation_id": "operational-service-levels-v1-report-" + "a" * 24,
        "policy_id": "us-tech-local",
        "policy_fingerprint": "operational-slo-policy-" + "b" * 24,
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-portfolio-schedule-v1-example",
        "calendar_id": "XNYS",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_fingerprint": "portfolio-mandate-v1-example",
        "as_of": as_of,
        "latest_expected_session": latest_session,
        "overall_status": status,
        "document_sha256": "c" * 64,
    }


def _decision(
    *,
    evaluated_at: datetime,
    report: dict[str, object] | None,
    allow_warning: bool = True,
    expected_session: date = date(2026, 3, 31),
) -> dict[str, object]:
    return evaluate_operational_readiness(
        gate_policy=_policy(allow_warning=allow_warning),
        evaluated_at=evaluated_at,
        latest_expected_session=expected_session,
        operational_policy_fingerprint="operational-slo-policy-" + "b" * 24,
        schedule_id="us-tech-local",
        schedule_fingerprint="local-portfolio-schedule-v1-example",
        calendar_id="XNYS",
        portfolio_id="us-tech-equal",
        risk_limit_policy_id="us-tech-standard",
        mandate_fingerprint="portfolio-mandate-v1-example",
        report=report,
    )


def test_validate_allow_decision_preserves_canonical_identity() -> None:
    evaluated_at = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    decision = _decision(
        evaluated_at=evaluated_at,
        report=_report(as_of=evaluated_at - timedelta(minutes=5)),
    )

    validated = validate_operational_readiness_decision(decision)

    assert validated["decision"] == "allow"
    assert validated["reasons"] == []
    assert validated["decision_id"] == decision["decision_id"]
    assert canonical_operational_readiness_decision_bytes(validated).startswith(b'{"')


def test_validate_missing_report_and_critical_report_block() -> None:
    evaluated_at = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    missing = validate_operational_readiness_decision(
        _decision(evaluated_at=evaluated_at, report=None)
    )
    critical = validate_operational_readiness_decision(
        _decision(
            evaluated_at=evaluated_at,
            report=_report(
                as_of=evaluated_at - timedelta(minutes=5),
                status="critical",
            ),
        )
    )

    assert missing["decision"] == "block"
    assert missing["reasons"] == ["report_missing"]
    assert critical["decision"] == "block"
    assert critical["reasons"] == ["report_status_critical"]


def test_warning_policy_and_report_age_are_recomputed() -> None:
    evaluated_at = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    warning = _decision(
        evaluated_at=evaluated_at,
        report=_report(
            as_of=evaluated_at - timedelta(minutes=5),
            status="warning",
        ),
        allow_warning=False,
    )
    stale = _decision(
        evaluated_at=evaluated_at,
        report=_report(as_of=evaluated_at - timedelta(hours=2)),
    )

    assert validate_operational_readiness_decision(warning)["reasons"] == [
        "report_status_warning"
    ]
    assert validate_operational_readiness_decision(stale)["reasons"] == [
        "report_age_exceeds_limit"
    ]

    stale["report_age_seconds"] = 1.0
    with pytest.raises(ValidationError, match="age evidence"):
        validate_operational_readiness_decision(stale)


def test_noncanonical_reason_order_and_side_effects_fail_closed() -> None:
    evaluated_at = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    decision = _decision(
        evaluated_at=evaluated_at,
        expected_session=date(2026, 4, 1),
        report=_report(
            as_of=evaluated_at + timedelta(minutes=5),
            status="critical",
        ),
    )
    assert decision["reasons"] == [
        "report_timestamp_future",
        "report_session_mismatch",
        "report_status_critical",
    ]

    decision["reasons"] = list(reversed(decision["reasons"]))
    with pytest.raises(ValidationError, match="canonical order"):
        validate_operational_readiness_decision(decision)

    valid = _decision(evaluated_at=evaluated_at, report=None)
    valid["schedule_executed"] = True
    with pytest.raises(ValidationError, match="must be false"):
        validate_operational_readiness_decision(valid)


def test_read_decision_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "decision.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "decision-link.json"
    link.symlink_to(target)

    with pytest.raises(StorageError, match="symbolic link"):
        read_operational_readiness_decision(link)
