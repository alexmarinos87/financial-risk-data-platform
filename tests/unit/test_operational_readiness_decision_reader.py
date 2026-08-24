from __future__ import annotations

import hashlib
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.warehouse.operational_readiness_decision_reader import (
    read_current_operational_readiness_decision,
)
from src.warehouse.operational_readiness_decision_recorder import (
    canonical_operational_readiness_decision_bytes,
)
from src.warehouse.operational_readiness_gate import (
    OperationalReadinessGatePolicy,
    evaluate_operational_readiness,
)


def _decision() -> dict[str, Any]:
    evaluated_at = datetime(2026, 4, 1, 12, tzinfo=timezone.utc)
    gate = OperationalReadinessGatePolicy(
        gate_id="us-tech-local",
        operational_policy_id="us-tech-local",
        max_report_age_seconds=3600,
        allow_warning=False,
    )
    return evaluate_operational_readiness(
        gate_policy=gate,
        evaluated_at=evaluated_at,
        latest_expected_session=date(2026, 3, 31),
        operational_policy_fingerprint="operational-slo-policy-" + "a" * 24,
        schedule_id="us-tech-local",
        schedule_fingerprint="local-schedule-example",
        calendar_id="XNYS",
        portfolio_id="us-tech-equal",
        risk_limit_policy_id="us-tech-standard",
        mandate_fingerprint="portfolio-mandate-example",
        report={
            "calculation_id": "operational-service-levels-v1-report-" + "b" * 24,
            "policy_id": "us-tech-local",
            "policy_fingerprint": "operational-slo-policy-" + "a" * 24,
            "schedule_id": "us-tech-local",
            "schedule_fingerprint": "local-schedule-example",
            "calendar_id": "XNYS",
            "portfolio_id": "us-tech-equal",
            "risk_limit_policy_id": "us-tech-standard",
            "mandate_fingerprint": "portfolio-mandate-example",
            "as_of": evaluated_at - timedelta(minutes=5),
            "latest_expected_session": date(2026, 3, 31),
            "overall_status": "ok",
            "document_sha256": "c" * 64,
        },
    )


def _kwargs() -> dict[str, Any]:
    return {
        "dsn": "not-used",
        "gate_id": "us-tech-local",
        "gate_fingerprint": "operational-readiness-gate-" + "d" * 24,
        "operational_policy_id": "us-tech-local",
        "operational_policy_fingerprint": "operational-slo-policy-" + "a" * 24,
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "calendar_id": "XNYS",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_fingerprint": "portfolio-mandate-example",
        "latest_expected_session": date(2026, 3, 31),
    }


def test_current_readiness_reader_validates_document_and_digest() -> None:
    decision = _decision()
    decision["gate_fingerprint"] = _kwargs()["gate_fingerprint"]
    # Gate fingerprint participates in identity, so rebuild the supplied ID.
    identity = {
        "calendar_id": decision["calendar_id"],
        "decision": decision["decision"],
        "evaluated_at": decision["evaluated_at"],
        "gate_fingerprint": decision["gate_fingerprint"],
        "latest_expected_session": decision["latest_expected_session"],
        "mandate_fingerprint": decision["mandate_fingerprint"],
        "operational_policy_fingerprint": decision[
            "operational_policy_fingerprint"
        ],
        "portfolio_id": decision["portfolio_id"],
        "reasons": decision["reasons"],
        "report_calculation_id": decision["report_calculation_id"],
        "report_document_sha256": decision["report_document_sha256"],
        "risk_limit_policy_id": decision["risk_limit_policy_id"],
        "schedule_fingerprint": decision["schedule_fingerprint"],
        "schedule_id": decision["schedule_id"],
    }
    import json

    decision["decision_id"] = (
        "operational-readiness-gate-v1-decision-"
        + hashlib.sha256(
            json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()[:24]
    )
    digest = hashlib.sha256(
        canonical_operational_readiness_decision_bytes(decision)
    ).hexdigest()

    result = read_current_operational_readiness_decision(
        **_kwargs(),
        row_reader=lambda **_: [
            {
                "decision_json": decision,
                "document_sha256": digest,
                "recorded_at": datetime(2026, 4, 1, 12, 1, tzinfo=timezone.utc),
            }
        ],
    )

    assert result is not None
    assert result["decision"] == "allow"
    assert result["document_sha256"] == digest


def test_current_readiness_reader_returns_none_and_rejects_duplicate_grain() -> None:
    assert (
        read_current_operational_readiness_decision(
            **_kwargs(),
            row_reader=lambda **_: [],
        )
        is None
    )

    with pytest.raises(StorageError, match="not unique"):
        read_current_operational_readiness_decision(
            **_kwargs(),
            row_reader=lambda **_: [{}, {}],
        )


def test_current_readiness_reader_rejects_digest_mismatch() -> None:
    decision = _decision()
    with pytest.raises(StorageError, match="does not match the plan"):
        read_current_operational_readiness_decision(
            **_kwargs(),
            row_reader=lambda **_: [
                {
                    "decision_json": decision,
                    "document_sha256": "0" * 64,
                    "recorded_at": datetime.now(timezone.utc),
                }
            ],
        )
