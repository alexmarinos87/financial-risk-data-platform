from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.operational_readiness_decision_recorder import (
    record_operational_readiness_decision,
)
from src.warehouse.operational_readiness_gate import (
    OperationalReadinessGatePolicy,
    evaluate_operational_readiness,
)
from src.warehouse.operational_readiness_override_contract_check import (
    run_contract_check as run_override_contract_check,
)
from src.warehouse.postgres_consistency import run_consistency_checks


def _report_row(
    report: Mapping[str, Any],
    document_sha256: str,
) -> dict[str, Any]:
    return {
        "calculation_id": report["calculation_id"],
        "policy_id": report["policy_id"],
        "policy_fingerprint": report["policy_fingerprint"],
        "schedule_id": report["schedule_id"],
        "schedule_fingerprint": report["schedule_fingerprint"],
        "calendar_id": report["calendar_id"],
        "portfolio_id": report["portfolio_id"],
        "risk_limit_policy_id": report["risk_limit_policy_id"],
        "mandate_fingerprint": report["mandate_fingerprint"],
        "as_of": report["as_of"],
        "latest_expected_session": report["latest_expected_session"],
        "overall_status": report["overall_status"],
        "document_sha256": document_sha256,
    }


def _decision(
    *,
    report: Mapping[str, Any],
    document_sha256: str,
    evaluated_at: datetime,
) -> dict[str, Any]:
    gate = OperationalReadinessGatePolicy(
        gate_id="us-tech-local",
        operational_policy_id=str(report["policy_id"]),
        max_report_age_seconds=3600,
        allow_warning=True,
    )
    latest_session = report["latest_expected_session"]
    if not isinstance(latest_session, str):
        raise AssertionError("contract fixture latest session is incompatible")
    return evaluate_operational_readiness(
        gate_policy=gate,
        evaluated_at=evaluated_at,
        latest_expected_session=date.fromisoformat(latest_session),
        operational_policy_fingerprint=str(report["policy_fingerprint"]),
        schedule_id=str(report["schedule_id"]),
        schedule_fingerprint=str(report["schedule_fingerprint"]),
        calendar_id=str(report["calendar_id"]),
        portfolio_id=str(report["portfolio_id"]),
        risk_limit_policy_id=str(report["risk_limit_policy_id"]),
        mandate_fingerprint=str(report["mandate_fingerprint"]),
        report=_report_row(report, document_sha256),
    )


def run_contract_check(
    *,
    dsn: str,
    first_report: Mapping[str, Any],
    first_document_sha256: str,
    second_report: Mapping[str, Any],
    second_document_sha256: str,
) -> dict[str, Any]:
    first_as_of = datetime.fromisoformat(str(first_report["as_of"]))
    second_as_of = datetime.fromisoformat(str(second_report["as_of"]))
    first_decision = _decision(
        report=first_report,
        document_sha256=first_document_sha256,
        evaluated_at=first_as_of + timedelta(minutes=5),
    )
    second_decision = _decision(
        report=second_report,
        document_sha256=second_document_sha256,
        evaluated_at=second_as_of + timedelta(minutes=5),
    )
    if first_decision["decision"] != "allow":
        raise AssertionError("warning-allowed readiness fixture did not allow")
    if second_decision["decision"] != "block":
        raise AssertionError("critical readiness fixture did not block")

    first = record_operational_readiness_decision(
        dsn=dsn,
        decision=first_decision,
    )
    replay = record_operational_readiness_decision(
        dsn=dsn,
        decision=first_decision,
    )
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("operational readiness retry did not converge")

    conflicting = dict(first_decision)
    conflicting["report_status"] = "ok"
    try:
        record_operational_readiness_decision(
            dsn=dsn,
            decision=conflicting,
        )
    except ValidationError:
        pass
    else:
        raise AssertionError("conflicting readiness decision identity was accepted")

    second = record_operational_readiness_decision(
        dsn=dsn,
        decision=second_decision,
    )
    if second["created"] is not True:
        raise AssertionError("second operational readiness decision was not appended")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational readiness contract requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_decisions),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_decision_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_readiness_reason_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.latest_operational_readiness_decisions),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_allowed_operational_readiness_decisions),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_blocked_operational_readiness_decisions),
                    (SELECT decision
                     FROM risk_platform.latest_operational_readiness_decisions)
                """
            )
            counts = cursor.fetchone()
            if counts != (2, 2, 1, 1, 0, 1, "block"):
                raise AssertionError(
                    f"operational readiness serving views are incompatible: {counts!r}"
                )

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE risk_platform.operational_readiness_decisions
                    SET recorded_at = recorded_at
                    WHERE decision_id = %s
                    """,
                    (first_decision["decision_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational readiness update was not blocked")

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM risk_platform.operational_readiness_decisions
                    WHERE decision_id = %s
                    """,
                    (first_decision["decision_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational readiness delete was not blocked")

    consistency = run_consistency_checks(
        dsn=dsn,
        check_paths=(
            Path("sql/operational_readiness_decisions_consistency_checks.sql"),
        ),
    )
    failures = [result for result in consistency if result.status != "pass"]
    if failures:
        names = ", ".join(result.check_name for result in failures)
        raise AssertionError("operational readiness reconciliation failed: " + names)

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_health_summary),
                    (SELECT health_status
                     FROM risk_platform.current_operational_health_summary),
                    (SELECT current_exception_count
                     FROM risk_platform.current_operational_health_summary),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_exception_summary),
                    (SELECT COUNT(*)
                     FROM risk_platform.recent_operational_readiness_decisions),
                    (SELECT COUNT(*)
                     FROM risk_platform.rolling_operational_objective_attainment),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_evidence_drillthrough)
                """
            )
            review_counts = cursor.fetchone()
            if review_counts != (1, "blocked", 3, 3, 2, 8, 10):
                raise AssertionError(
                    "operational review serving views are incompatible: "
                    f"{review_counts!r}"
                )

    review_consistency = run_consistency_checks(
        dsn=dsn,
        check_paths=(Path("sql/operational_review_consistency_checks.sql"),),
    )
    review_failures = [
        result for result in review_consistency if result.status != "pass"
    ]
    if review_failures:
        names = ", ".join(result.check_name for result in review_failures)
        raise AssertionError("operational review reconciliation failed: " + names)

    override_result = run_override_contract_check(
        dsn=dsn,
        blocked_decision_id=str(second_decision["decision_id"]),
        allowed_decision_id=str(first_decision["decision_id"]),
    )

    return {
        "first_decision_id": first_decision["decision_id"],
        "second_decision_id": second_decision["decision_id"],
        "decision_rows": 2,
        "latest_decision": "block",
        "append_only_verified": True,
        "replay_verified": True,
        "conflict_verified": True,
        "consistency_checks": len(consistency),
        "operational_review_health_status": "blocked",
        "operational_review_exception_rows": 3,
        "operational_review_consistency_checks": len(review_consistency),
        "override_contract": override_result,
    }
