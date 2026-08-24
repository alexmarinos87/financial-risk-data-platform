from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.warehouse.local_schedule_run_recorder import (
    build_local_schedule_run_id,
    record_local_schedule_run,
)
from src.warehouse.postgres_consistency import run_consistency_checks


def _stage(
    index: int,
    name: str,
    *,
    started_at: datetime,
    finished_at: datetime,
    status: str = "completed",
    failure_code: str | None = None,
) -> dict[str, Any]:
    return {
        "stage_index": index,
        "stage_name": name,
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "failure_code": failure_code,
    }


def _session(
    session_date: date,
    *,
    mandate_id: str,
    mandate_fingerprint: str,
    status: str,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    stages: list[dict[str, Any]] | None = None,
    failed_stage_index: int | None = None,
    failed_stage_name: str | None = None,
    failure_code: str | None = None,
) -> dict[str, Any]:
    return {
        "session_date": session_date.isoformat(),
        "mandate_id": mandate_id,
        "mandate_fingerprint": mandate_fingerprint,
        "status": status,
        "started_at": started_at.isoformat() if started_at else None,
        "finished_at": finished_at.isoformat() if finished_at else None,
        "checkpoint_after": session_date.isoformat() if status == "completed" else None,
        "failed_stage_index": failed_stage_index,
        "failed_stage_name": failed_stage_name,
        "failure_code": failure_code,
        "stages": stages or [],
    }


def _run_document(
    *,
    request_id: str,
    plan_id: str,
    authority_id: str,
    authority_type: str,
    decision: dict[str, Any],
    override_id: str | None,
    authorized_at: datetime,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    checkpoint_before: date | None,
    checkpoint_after: date | None,
    sessions: list[dict[str, Any]],
    failure_code: str | None = None,
    failed_session: date | None = None,
    failed_stage_index: int | None = None,
    failed_stage_name: str | None = None,
) -> dict[str, Any]:
    started_count = sum(
        1 for session in sessions if session["status"] in {"completed", "failed"}
    )
    completed_count = sum(
        1 for session in sessions if session["status"] == "completed"
    )
    return {
        "run_id": build_local_schedule_run_id(
            request_identifier=request_id,
            plan_id=plan_id,
            authority_id=authority_id,
        ),
        "model_version": "local-schedule-run-v1",
        "request_id": request_id,
        "plan_id": plan_id,
        "authority_id": authority_id,
        "authority_type": authority_type,
        "schedule_id": decision["schedule_id"],
        "schedule_fingerprint": decision["schedule_fingerprint"],
        "calendar_id": decision["calendar_id"],
        "calendar_fingerprint": "market-calendar-contract-example",
        "portfolio_id": decision["portfolio_id"],
        "risk_limit_policy_id": decision["risk_limit_policy_id"],
        "mandate_id": "us-tech-2026",
        "mandate_fingerprint": decision["mandate_fingerprint"],
        "as_of_date": decision["latest_expected_session"],
        "latest_expected_session": decision["latest_expected_session"],
        "readiness_decision_id": decision["decision_id"],
        "readiness_document_sha256": decision["document_sha256"],
        "override_id": override_id,
        "authorized_at": authorized_at.isoformat(),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "run_status": status,
        "checkpoint_before": checkpoint_before.isoformat() if checkpoint_before else None,
        "checkpoint_after": checkpoint_after.isoformat() if checkpoint_after else None,
        "selected_session_count": len(sessions),
        "started_session_count": started_count,
        "completed_session_count": completed_count,
        "failed_session": failed_session.isoformat() if failed_session else None,
        "failed_stage_index": failed_stage_index,
        "failed_stage_name": failed_stage_name,
        "failure_code": failure_code,
        "sessions": sessions,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def run_contract_check(
    *,
    dsn: str,
    allowed_decision_id: str,
    blocked_decision_id: str,
    active_override_id: str,
) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Local schedule run contract requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    decision_id,
                    document_sha256,
                    schedule_id,
                    schedule_fingerprint,
                    calendar_id,
                    portfolio_id,
                    risk_limit_policy_id,
                    mandate_fingerprint,
                    latest_expected_session,
                    evaluated_at,
                    decision
                FROM risk_platform.operational_readiness_decisions
                WHERE decision_id IN (%s, %s)
                ORDER BY decision_id
                """,
                (allowed_decision_id, blocked_decision_id),
            )
            rows = cursor.fetchall()
            decisions = {
                str(row[0]): {
                    "decision_id": str(row[0]),
                    "document_sha256": str(row[1]),
                    "schedule_id": str(row[2]),
                    "schedule_fingerprint": str(row[3]),
                    "calendar_id": str(row[4]),
                    "portfolio_id": str(row[5]),
                    "risk_limit_policy_id": str(row[6]),
                    "mandate_fingerprint": str(row[7]),
                    "latest_expected_session": row[8].isoformat(),
                    "evaluated_at": row[9],
                    "decision": str(row[10]),
                }
                for row in rows
            }
            cursor.execute(
                """
                SELECT approved_at, expires_at
                FROM risk_platform.operational_readiness_overrides
                WHERE override_id = %s
                """,
                (active_override_id,),
            )
            override_window = cursor.fetchone()
    if set(decisions) != {allowed_decision_id, blocked_decision_id}:
        raise AssertionError("local run contract readiness fixtures are missing")
    if override_window is None:
        raise AssertionError("local run contract active override fixture is missing")

    allowed = decisions[allowed_decision_id]
    blocked = decisions[blocked_decision_id]
    if allowed["decision"] != "allow" or blocked["decision"] != "block":
        raise AssertionError("local run contract readiness fixture decisions are wrong")

    now = datetime.now(timezone.utc)
    completed_started = max(
        allowed["evaluated_at"] + timedelta(seconds=1),
        now - timedelta(minutes=10),
    )
    completed_finished = completed_started + timedelta(seconds=4)
    completed_session_date = date.fromisoformat(allowed["latest_expected_session"])
    completed_sessions = [
        _session(
            completed_session_date,
            mandate_id="us-tech-2026",
            mandate_fingerprint=allowed["mandate_fingerprint"],
            status="completed",
            started_at=completed_started,
            finished_at=completed_finished,
            stages=[
                _stage(
                    0,
                    "governed-cycle",
                    started_at=completed_started,
                    finished_at=completed_started + timedelta(seconds=2),
                ),
                _stage(
                    1,
                    "checkpoint",
                    started_at=completed_started + timedelta(seconds=2),
                    finished_at=completed_finished,
                ),
            ],
        )
    ]
    completed_authority_id = (
        "operational-readiness-execution-authority-v1-authority-" + "2" * 24
    )
    completed = _run_document(
        request_id=completed_authority_id,
        plan_id="readiness-aware-schedule-plan-v1-plan-" + "1" * 24,
        authority_id=completed_authority_id,
        authority_type="gate_allow",
        decision=allowed,
        override_id=None,
        authorized_at=completed_started - timedelta(seconds=1),
        started_at=completed_started,
        finished_at=completed_finished,
        status="completed",
        checkpoint_before=completed_session_date - timedelta(days=1),
        checkpoint_after=completed_session_date,
        sessions=completed_sessions,
    )
    first = record_local_schedule_run(dsn=dsn, run=completed)
    replay = record_local_schedule_run(dsn=dsn, run=completed)
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("local schedule run retry did not converge")

    conflicting = dict(completed)
    conflicting["calendar_fingerprint"] = "market-calendar-conflict"
    try:
        record_local_schedule_run(dsn=dsn, run=conflicting)
    except ValidationError:
        pass
    else:
        raise AssertionError("conflicting local schedule run request was accepted")

    override_approved_at, override_expires_at = override_window
    failed_started = max(now, override_approved_at + timedelta(seconds=1))
    if failed_started >= override_expires_at:
        raise AssertionError("active override expired before local run contract check")
    failed_finished = failed_started + timedelta(seconds=6)
    if failed_finished >= override_expires_at:
        failed_finished = override_expires_at - timedelta(milliseconds=1)
    latest = date.fromisoformat(blocked["latest_expected_session"])
    first_selected = latest - timedelta(days=4)
    failed_selected = latest - timedelta(days=1)
    failed_sessions = [
        _session(
            first_selected,
            mandate_id="us-tech-2026",
            mandate_fingerprint=blocked["mandate_fingerprint"],
            status="completed",
            started_at=failed_started,
            finished_at=failed_started + timedelta(seconds=2),
            stages=[
                _stage(
                    0,
                    "governed-cycle",
                    started_at=failed_started,
                    finished_at=failed_started + timedelta(seconds=1),
                ),
                _stage(
                    1,
                    "checkpoint",
                    started_at=failed_started + timedelta(seconds=1),
                    finished_at=failed_started + timedelta(seconds=2),
                ),
            ],
        ),
        _session(
            failed_selected,
            mandate_id="us-tech-2026",
            mandate_fingerprint=blocked["mandate_fingerprint"],
            status="failed",
            started_at=failed_started + timedelta(seconds=2),
            finished_at=failed_finished,
            failed_stage_index=1,
            failed_stage_name="warehouse-load",
            failure_code="command_failed",
            stages=[
                _stage(
                    0,
                    "governed-cycle",
                    started_at=failed_started + timedelta(seconds=2),
                    finished_at=failed_started + timedelta(seconds=3),
                ),
                _stage(
                    1,
                    "warehouse-load",
                    started_at=failed_started + timedelta(seconds=3),
                    finished_at=failed_finished,
                    status="failed",
                    failure_code="command_failed",
                ),
            ],
        ),
        _session(
            latest,
            mandate_id="us-tech-2026",
            mandate_fingerprint=blocked["mandate_fingerprint"],
            status="selected",
        ),
    ]
    failed_authority_id = (
        "operational-readiness-execution-authority-v1-authority-" + "4" * 24
    )
    failed = _run_document(
        request_id=failed_authority_id,
        plan_id="readiness-aware-schedule-plan-v1-plan-" + "3" * 24,
        authority_id=failed_authority_id,
        authority_type="active_override",
        decision=blocked,
        override_id=active_override_id,
        authorized_at=failed_started,
        started_at=failed_started,
        finished_at=failed_finished,
        status="failed",
        checkpoint_before=first_selected - timedelta(days=1),
        checkpoint_after=first_selected,
        sessions=failed_sessions,
        failure_code="command_failed",
        failed_session=failed_selected,
        failed_stage_index=1,
        failed_stage_name="warehouse-load",
    )
    second = record_local_schedule_run(dsn=dsn, run=failed)
    if second["created"] is not True:
        raise AssertionError("failed local schedule run was not appended")

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM risk_platform.local_schedule_runs),
                    (SELECT COUNT(*)
                     FROM risk_platform.local_schedule_run_session_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.local_schedule_run_stage_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.recent_local_schedule_runs),
                    (SELECT run_status
                     FROM risk_platform.current_local_schedule_run_status),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_local_schedule_run_failures),
                    (SELECT COUNT(*)
                     FROM risk_platform.incomplete_local_schedule_sessions)
                """
            )
            counts = cursor.fetchone()
            if counts != (2, 4, 6, 2, "failed", 1, 2):
                raise AssertionError(
                    f"local schedule run serving views are incompatible: {counts!r}"
                )

    for statement in (
        """
        UPDATE risk_platform.local_schedule_runs
        SET recorded_at = recorded_at
        WHERE run_id = %s
        """,
        """
        DELETE FROM risk_platform.local_schedule_runs
        WHERE run_id = %s
        """,
    ):
        with psycopg.connect(dsn) as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(statement, (completed["run_id"],))
            except psycopg.Error:
                connection.rollback()
            else:
                raise AssertionError("local schedule run mutation was not blocked")

    consistency = run_consistency_checks(
        dsn=dsn,
        check_paths=(Path("sql/local_schedule_runs_consistency_checks.sql"),),
    )
    failures = [result for result in consistency if result.status != "pass"]
    if failures:
        names = ", ".join(result.check_name for result in failures)
        raise AssertionError("local schedule run reconciliation failed: " + names)

    return {
        "run_rows": 2,
        "session_rows": 4,
        "stage_rows": 6,
        "current_status": "failed",
        "current_failure_rows": 1,
        "incomplete_session_rows": 2,
        "replay_verified": True,
        "conflict_verified": True,
        "append_only_verified": True,
        "consistency_checks": len(consistency),
        "completed_run_id": first["run_id"],
        "failed_run_id": second["run_id"],
    }
