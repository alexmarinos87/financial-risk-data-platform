from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.local_schedule_run_recorder import (
    build_local_schedule_run_id,
    canonical_local_schedule_run_bytes,
    read_local_schedule_run,
    validate_local_schedule_run,
)

PLAN_ID = "readiness-aware-schedule-plan-v1-plan-" + "a" * 24
AUTHORITY_ID = (
    "operational-readiness-execution-authority-v1-authority-" + "b" * 24
)
DECISION_ID = "operational-readiness-gate-v1-decision-" + "c" * 24
OVERRIDE_ID = "operational-readiness-override-v1-" + "d" * 24


def _stage(
    index: int,
    name: str,
    status: str,
    started_at: str,
    finished_at: str,
    failure_code: str | None = None,
) -> dict[str, Any]:
    return {
        "stage_index": index,
        "stage_name": name,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "failure_code": failure_code,
    }


def _completed_run() -> dict[str, Any]:
    request_id = AUTHORITY_ID
    return {
        "run_id": build_local_schedule_run_id(
            request_identifier=request_id,
            plan_id=PLAN_ID,
            authority_id=AUTHORITY_ID,
        ),
        "model_version": "local-schedule-run-v1",
        "request_id": request_id,
        "plan_id": PLAN_ID,
        "authority_id": AUTHORITY_ID,
        "authority_type": "gate_allow",
        "schedule_id": "us-tech-local",
        "schedule_fingerprint": "local-schedule-example",
        "calendar_id": "XNYS",
        "calendar_fingerprint": "market-calendar-example",
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_id": "us-tech-2026",
        "mandate_fingerprint": "portfolio-mandate-example",
        "as_of_date": "2026-01-10",
        "latest_expected_session": "2026-01-09",
        "readiness_decision_id": DECISION_ID,
        "readiness_document_sha256": "e" * 64,
        "override_id": None,
        "authorized_at": "2026-01-10T11:59:00+00:00",
        "started_at": "2026-01-10T12:00:00+00:00",
        "finished_at": "2026-01-10T12:00:04+00:00",
        "run_status": "completed",
        "checkpoint_before": None,
        "checkpoint_after": "2026-01-09",
        "selected_session_count": 1,
        "started_session_count": 1,
        "completed_session_count": 1,
        "failed_session": None,
        "failed_stage_index": None,
        "failed_stage_name": None,
        "failure_code": None,
        "sessions": [
            {
                "session_date": "2026-01-09",
                "mandate_id": "us-tech-2026",
                "mandate_fingerprint": "portfolio-mandate-example",
                "status": "completed",
                "started_at": "2026-01-10T12:00:01+00:00",
                "finished_at": "2026-01-10T12:00:03+00:00",
                "checkpoint_after": "2026-01-09",
                "failed_stage_index": None,
                "failed_stage_name": None,
                "failure_code": None,
                "stages": [
                    _stage(
                        0,
                        "run_daily_risk:AAPL",
                        "completed",
                        "2026-01-10T12:00:01+00:00",
                        "2026-01-10T12:00:02+00:00",
                    ),
                    _stage(
                        1,
                        "checkpoint",
                        "completed",
                        "2026-01-10T12:00:02+00:00",
                        "2026-01-10T12:00:03+00:00",
                    ),
                ],
            }
        ],
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def _failed_run() -> dict[str, Any]:
    payload = _completed_run()
    payload.update(
        {
            "authority_type": "active_override",
            "override_id": OVERRIDE_ID,
            "started_at": "2026-01-10T12:00:00+00:00",
            "finished_at": "2026-01-10T12:00:08+00:00",
            "run_status": "failed",
            "checkpoint_before": "2026-01-06",
            "checkpoint_after": "2026-01-07",
            "selected_session_count": 3,
            "started_session_count": 2,
            "completed_session_count": 1,
            "failed_session": "2026-01-08",
            "failed_stage_index": 1,
            "failed_stage_name": "run_market_freshness:AAPL",
            "failure_code": "command_failed",
            "sessions": [
                {
                    "session_date": "2026-01-07",
                    "mandate_id": "us-tech-2026",
                    "mandate_fingerprint": "portfolio-mandate-example",
                    "status": "completed",
                    "started_at": "2026-01-10T12:00:01+00:00",
                    "finished_at": "2026-01-10T12:00:04+00:00",
                    "checkpoint_after": "2026-01-07",
                    "failed_stage_index": None,
                    "failed_stage_name": None,
                    "failure_code": None,
                    "stages": [
                        _stage(
                            0,
                            "run_daily_risk:AAPL",
                            "completed",
                            "2026-01-10T12:00:01+00:00",
                            "2026-01-10T12:00:02+00:00",
                        ),
                        _stage(
                            1,
                            "checkpoint",
                            "completed",
                            "2026-01-10T12:00:03+00:00",
                            "2026-01-10T12:00:04+00:00",
                        ),
                    ],
                },
                {
                    "session_date": "2026-01-08",
                    "mandate_id": "us-tech-2026",
                    "mandate_fingerprint": "portfolio-mandate-example",
                    "status": "failed",
                    "started_at": "2026-01-10T12:00:05+00:00",
                    "finished_at": "2026-01-10T12:00:07+00:00",
                    "checkpoint_after": None,
                    "failed_stage_index": 1,
                    "failed_stage_name": "run_market_freshness:AAPL",
                    "failure_code": "command_failed",
                    "stages": [
                        _stage(
                            0,
                            "run_daily_risk:AAPL",
                            "completed",
                            "2026-01-10T12:00:05+00:00",
                            "2026-01-10T12:00:06+00:00",
                        ),
                        _stage(
                            1,
                            "run_market_freshness:AAPL",
                            "failed",
                            "2026-01-10T12:00:06+00:00",
                            "2026-01-10T12:00:07+00:00",
                            "command_failed",
                        ),
                    ],
                },
                {
                    "session_date": "2026-01-09",
                    "mandate_id": "us-tech-2026",
                    "mandate_fingerprint": "portfolio-mandate-example",
                    "status": "selected",
                    "started_at": None,
                    "finished_at": None,
                    "checkpoint_after": None,
                    "failed_stage_index": None,
                    "failed_stage_name": None,
                    "failure_code": None,
                    "stages": [],
                },
            ],
        }
    )
    return payload


def test_completed_run_is_canonical_and_deterministic() -> None:
    first = validate_local_schedule_run(_completed_run())
    second = validate_local_schedule_run(deepcopy(_completed_run()))

    assert first == second
    assert first["run_status"] == "completed"
    assert first["checkpoint_after"] == "2026-01-09"
    assert canonical_local_schedule_run_bytes(first) == canonical_local_schedule_run_bytes(
        second
    )


def test_failed_run_retains_terminal_prefix_and_incomplete_sessions() -> None:
    run = validate_local_schedule_run(_failed_run())

    assert [session["status"] for session in run["sessions"]] == [
        "completed",
        "failed",
        "selected",
    ]
    assert run["completed_session_count"] == 1
    assert run["failed_session"] == "2026-01-08"
    assert run["failed_stage_name"] == "run_market_freshness:AAPL"
    assert run["checkpoint_after"] == "2026-01-07"


def test_tampering_and_conflicting_authority_shape_fail_closed() -> None:
    wrong_id = _completed_run()
    wrong_id["run_id"] = "local-schedule-run-v1-run-" + "0" * 24
    with pytest.raises(ValidationError, match="run_id"):
        validate_local_schedule_run(wrong_id)

    extra_secret_field = _completed_run()
    extra_secret_field["dsn"] = "postgresql://secret"
    with pytest.raises(ValidationError, match="shape"):
        validate_local_schedule_run(extra_secret_field)

    gate_with_override = _completed_run()
    gate_with_override["override_id"] = OVERRIDE_ID
    with pytest.raises(ValidationError, match="gate_allow"):
        validate_local_schedule_run(gate_with_override)


def test_session_stage_and_checkpoint_arithmetic_is_strict() -> None:
    wrong_stage = _failed_run()
    wrong_stage["sessions"][1]["stages"][1]["stage_index"] = 2
    with pytest.raises(ValidationError, match="stage_index"):
        validate_local_schedule_run(wrong_stage)

    wrong_prefix = _failed_run()
    wrong_prefix["sessions"][2]["status"] = "completed"
    with pytest.raises(ValidationError):
        validate_local_schedule_run(wrong_prefix)

    wrong_checkpoint = _failed_run()
    wrong_checkpoint["checkpoint_after"] = "2026-01-08"
    with pytest.raises(ValidationError, match="checkpoint_after"):
        validate_local_schedule_run(wrong_checkpoint)


def test_reader_rejects_symlinks_and_non_objects(tmp_path: Path) -> None:
    target = tmp_path / "run.json"
    target.write_text("[]", encoding="utf-8")
    link = tmp_path / "run-link.json"
    link.symlink_to(target)

    with pytest.raises(StorageError, match="symbolic"):
        read_local_schedule_run(link)
    with pytest.raises(ValidationError, match="JSON object"):
        read_local_schedule_run(target)
