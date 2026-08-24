from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_readiness_enforced_local_schedule import (
    run_readiness_enforced_local_schedule,
)

AUTHORIZATION_TIME = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
DECISION_ID = "operational-readiness-gate-v1-decision-" + "a" * 24
DOCUMENT_SHA256 = "b" * 64


def _plan(
    *,
    effect: str = "would_run",
    decision: str = "allow",
    decision_id: str | None = DECISION_ID,
) -> dict[str, Any]:
    reasons = [] if decision == "allow" else ["report_status_critical"]
    status = "current" if decision_id is not None else "missing"
    return {
        "plan_id": "readiness-aware-schedule-plan-v1-plan-" + "c" * 24,
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
            "status": status,
            "decision_id": decision_id,
            "decision": decision if decision_id is not None else "block",
            "reasons": reasons if decision_id is not None else ["decision_missing"],
            "document_sha256": DOCUMENT_SHA256 if decision_id is not None else None,
            "recorded_at": "2026-01-10T11:56:00+00:00",
            "gate_id": "us-tech-local",
            "gate_fingerprint": "operational-readiness-gate-" + "d" * 24,
            "operational_policy_id": "us-tech-local",
            "operational_policy_fingerprint": "operational-slo-policy-" + "e" * 24,
            "latest_expected_session": "2026-01-09",
        },
        "schedule_plan": {
            "schedule_id": "us-tech-local",
            "schedule_fingerprint": "local-schedule-example",
            "enabled": True,
            "calendar": {
                "calendar_id": "XNYS",
                "calendar_fingerprint": "market-calendar-example",
            },
            "selection": {
                "as_of_date": "2026-01-10",
                "checkpoint_before": None,
                "sessions_selected": 0 if effect == "no_work" else 1,
                "session_dates": [] if effect == "no_work" else ["2026-01-09"],
            },
            "plans": [],
        },
        "schedule_effect": {
            "decision": effect,
            "sessions_selected": 0 if effect == "no_work" else 1,
            "session_dates": [] if effect == "no_work" else ["2026-01-09"],
        },
    }


def _current_decision(
    *,
    decision: str = "allow",
    evaluated_at: datetime | None = None,
    decision_id: str = DECISION_ID,
) -> dict[str, Any]:
    reasons = [] if decision == "allow" else ["report_status_critical"]
    return {
        "decision_id": decision_id,
        "model_version": "operational-readiness-gate-v1",
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
        "evaluated_at": (
            evaluated_at
            or AUTHORIZATION_TIME - timedelta(minutes=5)
        ).isoformat(),
        "latest_expected_session": "2026-01-09",
        "max_report_age_seconds": 3600,
        "allow_warning": False,
        "report_calculation_id": "operational-service-levels-v1-report-" + "f" * 24,
        "report_document_sha256": "1" * 64,
        "report_as_of": (AUTHORIZATION_TIME - timedelta(minutes=10)).isoformat(),
        "report_latest_expected_session": "2026-01-09",
        "report_status": "ok" if decision == "allow" else "critical",
        "report_age_seconds": 300.0,
        "report_future_seconds": 0.0,
        "decision": decision,
        "reasons": reasons,
        "schedule_executed": False,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
        "document_sha256": DOCUMENT_SHA256,
        "recorded_at": "2026-01-10T11:56:00+00:00",
    }


def _override() -> dict[str, Any]:
    return {
        "override_id": "operational-readiness-override-v1-" + "2" * 24,
        "model_version": "operational-readiness-override-v1",
        "decision_id": DECISION_ID,
        "decision_document_sha256": DOCUMENT_SHA256,
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
        "request_id": "OVERRIDE-001",
        "approved_at": "2026-01-10T11:55:00+00:00",
        "expires_at": "2026-01-10T13:00:00+00:00",
        "approved_by": "operator@example.test",
        "revocation_id": None,
        "revoked_at": None,
        "evaluated_at": AUTHORIZATION_TIME.isoformat(),
        "active": True,
    }


def _execution_result(authority: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": "schedule-run",
        "execution_authority": authority,
        "execution": {
            "requested": True,
            "performed": True,
            "completed_sessions": ["2026-01-09"],
            "checkpoint_after": "2026-01-09",
        },
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def _run(
    *,
    plan: dict[str, Any],
    current: dict[str, Any] | None,
    override: dict[str, Any] | None = None,
    execute: bool = True,
    counters: dict[str, int] | None = None,
    executor_result_factory: Any = _execution_result,
) -> dict[str, Any]:
    calls = counters if counters is not None else {
        "current": 0,
        "override": 0,
        "executor": 0,
    }

    def current_reader(**_: Any) -> dict[str, Any] | None:
        calls["current"] += 1
        return current

    def override_reader(**_: Any) -> dict[str, Any] | None:
        calls["override"] += 1
        return override

    def executor(**kwargs: Any) -> dict[str, Any]:
        calls["executor"] += 1
        return executor_result_factory(kwargs["execution_authority"])

    return run_readiness_enforced_local_schedule(
        schedule_id="us-tech-local",
        gate_id="us-tech-local",
        as_of_date=date(2026, 1, 10),
        evaluated_at=AUTHORIZATION_TIME,
        schedule_config_path=Path("schedule.yaml"),
        gate_config_path=Path("gate.yaml"),
        operational_policy_config_path=Path("policy.yaml"),
        calendar_config_path=Path("calendar.yaml"),
        portfolio_config_path=Path("portfolio.yaml"),
        risk_limit_config_path=Path("limits.yaml"),
        storage_config_path=Path("storage.yaml"),
        state_dir=Path(".scheduler"),
        dsn="not-used",
        execute=execute,
        python_executable="python",
        plan_builder=lambda **_: plan,
        current_decision_reader=current_reader,
        override_reader=override_reader,
        schedule_executor=executor,
    )


def test_plan_only_and_no_work_do_not_read_authority_or_execute() -> None:
    plan_calls = {"current": 0, "override": 0, "executor": 0}
    plan_result = _run(
        plan=_plan(),
        current=_current_decision(),
        execute=False,
        counters=plan_calls,
    )
    no_work_calls = {"current": 0, "override": 0, "executor": 0}
    no_work = _run(
        plan=_plan(effect="no_work"),
        current=None,
        counters=no_work_calls,
    )

    assert plan_result["decision"] == "would_run"
    assert no_work["decision"] == "no_work"
    assert plan_calls == {"current": 0, "override": 0, "executor": 0}
    assert no_work_calls == {"current": 0, "override": 0, "executor": 0}


def test_current_allow_executes_with_gate_authority() -> None:
    calls = {"current": 0, "override": 0, "executor": 0}
    result = _run(
        plan=_plan(),
        current=_current_decision(),
        counters=calls,
    )

    assert result["decision"] == "executed"
    assert result["execution_authority"]["authority_type"] == "gate_allow"
    assert result["checkpoint_after"] == "2026-01-09"
    assert calls == {"current": 1, "override": 0, "executor": 1}


def test_block_without_active_override_never_calls_executor() -> None:
    calls = {"current": 0, "override": 0, "executor": 0}
    result = _run(
        plan=_plan(effect="would_block", decision="block"),
        current=_current_decision(decision="block"),
        override=None,
        counters=calls,
    )

    assert result["decision"] == "block"
    assert result["block_reasons"] == [
        "report_status_critical",
        "override_missing_or_inactive",
    ]
    assert calls == {"current": 1, "override": 1, "executor": 0}


def test_block_with_exact_active_override_executes() -> None:
    calls = {"current": 0, "override": 0, "executor": 0}
    result = _run(
        plan=_plan(effect="would_block", decision="block"),
        current=_current_decision(decision="block"),
        override=_override(),
        counters=calls,
    )

    authority = result["execution_authority"]
    assert result["decision"] == "executed"
    assert authority["authority_type"] == "active_override"
    assert authority["override_id"] == _override()["override_id"]
    assert calls == {"current": 1, "override": 1, "executor": 1}


def test_missing_or_stale_decision_blocks_without_override_lookup() -> None:
    missing_calls = {"current": 0, "override": 0, "executor": 0}
    missing = _run(
        plan=_plan(effect="would_block", decision="block", decision_id=None),
        current=None,
        counters=missing_calls,
    )
    stale_calls = {"current": 0, "override": 0, "executor": 0}
    stale = _run(
        plan=_plan(),
        current=_current_decision(
            evaluated_at=AUTHORIZATION_TIME - timedelta(hours=2)
        ),
        counters=stale_calls,
    )

    assert missing["block_reasons"] == ["decision_missing"]
    assert stale["block_reasons"] == ["decision_age_exceeds_limit"]
    assert missing_calls == {"current": 1, "override": 0, "executor": 0}
    assert stale_calls == {"current": 1, "override": 0, "executor": 0}


def test_changed_decision_and_wrong_override_contract_fail_closed() -> None:
    with pytest.raises(ValidationError, match="changed after planning"):
        _run(
            plan=_plan(),
            current=_current_decision(
                decision_id="operational-readiness-gate-v1-decision-" + "9" * 24
            ),
        )

    wrong_override = _override()
    wrong_override["schedule_fingerprint"] = "other-schedule"
    with pytest.raises(ValidationError, match="schedule_fingerprint"):
        _run(
            plan=_plan(effect="would_block", decision="block"),
            current=_current_decision(decision="block"),
            override=wrong_override,
        )


def test_incomplete_execution_evidence_fails_closed() -> None:
    def incomplete(authority: dict[str, Any]) -> dict[str, Any]:
        result = _execution_result(authority)
        result["execution"]["completed_sessions"] = []
        return result

    with pytest.raises(StorageError, match="execution evidence"):
        _run(
            plan=_plan(),
            current=_current_decision(),
            executor_result_factory=incomplete,
        )