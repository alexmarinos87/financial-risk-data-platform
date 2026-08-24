from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.market_calendar import MarketCalendar, load_market_calendar
from ..analytics.portfolio_mandates import (
    PortfolioMandate,
    load_portfolio_mandate,
)
from ..common.config import load_yaml
from ..common.exceptions import OverlapError, StorageError, ValidationError
from ..warehouse.local_schedule_run_recorder import (
    build_local_schedule_run_id,
    record_local_schedule_run,
)
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN
from .locks import acquire_partition_locks, release_partition_locks
from .operational_readiness_execution_authority import (
    validate_operational_readiness_execution_authority,
)

MODEL_VERSION = "local-portfolio-schedule-v1"
MAX_CATCH_UP_SESSIONS = 31
MAX_STATE_BYTES = 65_536

Command = tuple[str, ...]
CommandRunner = Callable[[Command, Mapping[str, str]], None]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
CalendarLoader = Callable[[Path, str], MarketCalendar]
RunHistoryRecorder = Callable[..., Mapping[str, Any]]
Clock = Callable[[], datetime]


@dataclass(frozen=True, slots=True)
class LocalPortfolioSchedule:
    schedule_id: str
    enabled: bool
    portfolio_id: str
    policy_id: str
    calendar_id: str
    maximum_catch_up_sessions: int
    volatility_window: int
    var_window: int
    var_confidence: float
    covariance_window: int
    max_snapshots: int
    max_evaluations: int
    max_notification_events: int

    @property
    def fingerprint(self) -> str:
        payload = {
            "calendar_id": self.calendar_id,
            "covariance_window": self.covariance_window,
            "enabled": self.enabled,
            "max_evaluations": self.max_evaluations,
            "max_notification_events": self.max_notification_events,
            "max_snapshots": self.max_snapshots,
            "maximum_catch_up_sessions": self.maximum_catch_up_sessions,
            "model_version": MODEL_VERSION,
            "policy_id": self.policy_id,
            "portfolio_id": self.portfolio_id,
            "schedule_id": self.schedule_id,
            "var_confidence": self.var_confidence,
            "var_window": self.var_window,
            "volatility_window": self.volatility_window,
        }
        digest = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"local-schedule-{digest}"


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if (
        parsed in {".", ".."}
        or "/" in parsed
        or "\\" in parsed
        or any(ord(character) < 32 for character in parsed)
    ):
        raise ValidationError(f"{label} must be one safe text segment")
    return parsed


def _bounded_integer(
    value: Any,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def parse_local_portfolio_schedule(
    payload: Mapping[str, Any],
    schedule_id: str,
) -> LocalPortfolioSchedule:
    if not isinstance(payload, Mapping):
        raise ValidationError("local schedule configuration must be a mapping")
    schedule_id = _required_text(schedule_id, "schedule_id")
    schedules = payload.get("schedules")
    if not isinstance(schedules, Mapping):
        raise ValidationError(
            "local schedule configuration must define a schedules mapping"
        )
    candidate = schedules.get(schedule_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"local schedule '{schedule_id}' is not configured")
    enabled = candidate.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("schedule enabled must be boolean")
    confidence = candidate.get("var_confidence")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not 0 < float(confidence) < 1
    ):
        raise ValidationError("var_confidence must be between 0 and 1")

    return LocalPortfolioSchedule(
        schedule_id=schedule_id,
        enabled=enabled,
        portfolio_id=_required_text(candidate.get("portfolio_id"), "portfolio_id"),
        policy_id=_required_text(candidate.get("policy_id"), "policy_id"),
        calendar_id=_required_text(candidate.get("calendar_id"), "calendar_id"),
        maximum_catch_up_sessions=_bounded_integer(
            candidate.get("maximum_catch_up_sessions"),
            "maximum_catch_up_sessions",
            minimum=1,
            maximum=MAX_CATCH_UP_SESSIONS,
        ),
        volatility_window=_bounded_integer(
            candidate.get("volatility_window"),
            "volatility_window",
            minimum=2,
            maximum=10_000,
        ),
        var_window=_bounded_integer(
            candidate.get("var_window"),
            "var_window",
            minimum=2,
            maximum=10_000,
        ),
        var_confidence=float(confidence),
        covariance_window=_bounded_integer(
            candidate.get("covariance_window"),
            "covariance_window",
            minimum=2,
            maximum=2_520,
        ),
        max_snapshots=_bounded_integer(
            candidate.get("max_snapshots"),
            "max_snapshots",
            minimum=1,
            maximum=2_500,
        ),
        max_evaluations=_bounded_integer(
            candidate.get("max_evaluations"),
            "max_evaluations",
            minimum=1,
            maximum=10_000,
        ),
        max_notification_events=_bounded_integer(
            candidate.get("max_notification_events"),
            "max_notification_events",
            minimum=1,
            maximum=5_000,
        ),
    )


def load_local_portfolio_schedule(path: Path, schedule_id: str) -> LocalPortfolioSchedule:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("local schedule configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("local schedule configuration must be a mapping")
    return parse_local_portfolio_schedule(payload, schedule_id)


def _state_path(state_dir: Path, schedule_id: str) -> Path:
    return state_dir / f"{_required_text(schedule_id, 'schedule_id')}.json"


def _read_state(path: Path) -> dict[str, Any] | None:
    if path.is_symlink():
        raise StorageError("local schedule state must not be a symbolic link")
    if not path.exists():
        return None
    if not path.is_file():
        raise StorageError("local schedule state must be a regular file")
    try:
        if path.stat().st_size > MAX_STATE_BYTES:
            raise StorageError("local schedule state exceeds the byte limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except StorageError:
        raise
    except (OSError, ValueError):
        raise StorageError("local schedule state is unreadable") from None
    if not isinstance(payload, dict):
        raise StorageError("local schedule state must be a JSON object")
    return payload


def _checkpoint_date(
    state: Mapping[str, Any] | None,
    *,
    schedule: LocalPortfolioSchedule,
) -> date | None:
    if state is None:
        return None
    if state.get("model_version") != MODEL_VERSION:
        raise ValidationError("local schedule state model version is unsupported")
    if state.get("schedule_id") != schedule.schedule_id:
        raise ValidationError("local schedule state belongs to another schedule")
    if state.get("schedule_fingerprint") != schedule.fingerprint:
        raise ValidationError(
            "local schedule configuration changed; archive or reset the state "
            "before execution"
        )
    raw = state.get("last_successful_session")
    if not isinstance(raw, str):
        raise ValidationError("local schedule state is missing last_successful_session")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        raise ValidationError("local schedule checkpoint must use YYYY-MM-DD") from None
    if raw != parsed.isoformat():
        raise ValidationError("local schedule checkpoint must use YYYY-MM-DD")
    return parsed


def plan_schedule_sessions(
    *,
    schedule: LocalPortfolioSchedule,
    calendar: MarketCalendar,
    as_of_date: date,
    state: Mapping[str, Any] | None,
) -> tuple[date, ...]:
    calendar.require_covered(as_of_date)
    latest_expected = calendar.latest_expected_session(as_of_date)
    checkpoint = _checkpoint_date(state, schedule=schedule)
    if checkpoint is None:
        return (latest_expected,)
    calendar.require_covered(checkpoint)
    if checkpoint > latest_expected:
        raise ValidationError(
            "local schedule checkpoint is after the latest expected session"
        )
    if checkpoint == latest_expected:
        return ()
    candidates = calendar.sessions_between(checkpoint + timedelta(days=1), latest_expected)
    if len(candidates) > schedule.maximum_catch_up_sessions:
        raise ValidationError(
            "local schedule catch-up exceeds maximum_catch_up_sessions; "
            "run a bounded backfill explicitly"
        )
    return candidates


def _summary_path(
    state_dir: Path,
    schedule_id: str,
    session_date: date,
    name: str,
) -> str:
    return str(
        state_dir
        / "runs"
        / schedule_id
        / session_date.isoformat()
        / f"{name}.json"
    )


def build_session_commands(
    *,
    schedule: LocalPortfolioSchedule,
    session_date: date,
    mandate: PortfolioMandate,
    python_executable: str,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    calendar_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
) -> tuple[Command, ...]:
    session = session_date.isoformat()
    commands: list[Command] = []
    for constituent in mandate.constituents:
        if constituent.source != "alpha_vantage":
            raise ValidationError(
                "local scheduling currently supports alpha_vantage constituents only"
            )
        symbol = constituent.symbol
        commands.extend(
            [
                (
                    python_executable,
                    "-m",
                    "src.orchestration.run_daily_risk",
                    "--symbol",
                    symbol,
                    "--start-date",
                    session,
                    "--end-date",
                    session,
                    "--vol-window",
                    str(schedule.volatility_window),
                    "--var-window",
                    str(schedule.var_window),
                    "--var-confidence",
                    str(schedule.var_confidence),
                    "--storage-config",
                    str(storage_config_path),
                    "--summary-json",
                    _summary_path(
                        state_dir,
                        schedule.schedule_id,
                        session_date,
                        f"daily-risk-{symbol}",
                    ),
                ),
                (
                    python_executable,
                    "-m",
                    "src.orchestration.run_market_freshness",
                    "--symbol",
                    symbol,
                    "--calendar-id",
                    schedule.calendar_id,
                    "--calendar-config",
                    str(calendar_config_path),
                    "--as-of-date",
                    session,
                    "--storage-config",
                    str(storage_config_path),
                    "--summary-json",
                    _summary_path(
                        state_dir,
                        schedule.schedule_id,
                        session_date,
                        f"freshness-{symbol}",
                    ),
                ),
            ]
        )

    commands.append(
        (
            python_executable,
            "-m",
            "src.orchestration.run_governed_portfolio_cycle",
            "--portfolio-id",
            schedule.portfolio_id,
            "--policy-id",
            schedule.policy_id,
            "--portfolio-config",
            str(portfolio_config_path),
            "--risk-limit-config",
            str(risk_limit_config_path),
            "--start-date",
            session,
            "--end-date",
            session,
            "--vol-window",
            str(schedule.volatility_window),
            "--var-window",
            str(schedule.var_window),
            "--var-confidence",
            str(schedule.var_confidence),
            "--covariance-window",
            str(schedule.covariance_window),
            "--max-snapshots",
            str(schedule.max_snapshots),
            "--max-evaluations",
            str(schedule.max_evaluations),
            "--storage-config",
            str(storage_config_path),
            "--lock-base-dir",
            str(state_dir),
            "--summary-json",
            _summary_path(
                state_dir,
                schedule.schedule_id,
                session_date,
                "governed-cycle",
            ),
        )
    )
    commands.extend(
        [
            ("make", "portfolio-risk-limits-warehouse-load"),
            (
                python_executable,
                "-m",
                "src.warehouse.market_freshness_loader",
                "--storage-config",
                str(storage_config_path),
            ),
            (
                python_executable,
                "-m",
                "src.orchestration.run_optional_portfolio_risk_notification_outbox",
                "--policy-id",
                schedule.policy_id,
                "--start-date",
                session,
                "--end-date",
                session,
                "--max-events",
                str(schedule.max_notification_events),
                "--storage-config",
                str(storage_config_path),
                "--summary-json",
                _summary_path(
                    state_dir,
                    schedule.schedule_id,
                    session_date,
                    "notification-outbox",
                ),
            ),
            (
                python_executable,
                "-m",
                "src.warehouse.portfolio_risk_notification_outbox_loader",
                "--storage-config",
                str(storage_config_path),
            ),
        ]
    )
    return tuple(commands)


def _default_command_runner(command: Command, environment: Mapping[str, str]) -> None:
    merged_environment = dict(os.environ)
    merged_environment.update(environment)
    try:
        subprocess.run(list(command), check=True, env=merged_environment)
    except (OSError, subprocess.CalledProcessError):
        raise StorageError(
            "local portfolio schedule command failed; checkpoint was not advanced"
        ) from None


def _write_state(
    path: Path,
    *,
    schedule: LocalPortfolioSchedule,
    session_date: date,
) -> None:
    if path.is_symlink():
        raise StorageError("local schedule state must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    payload = {
        "last_successful_session": session_date.isoformat(),
        "model_version": MODEL_VERSION,
        "schedule_fingerprint": schedule.fingerprint,
        "schedule_id": schedule.schedule_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        temporary.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to update local schedule checkpoint") from None


def _clock_value(clock: Clock) -> datetime:
    value = clock()
    if value.tzinfo is None or value.utcoffset() is None:
        raise StorageError("local schedule execution clock must be timezone-aware")
    return value.astimezone(timezone.utc)


def _command_stage_name(command: Sequence[str]) -> str:
    if len(command) >= 3 and command[1] == "-m":
        name = command[2].rsplit(".", 1)[-1]
        if "--symbol" in command:
            index = command.index("--symbol")
            if index + 1 < len(command):
                name = f"{name}:{command[index + 1]}"
        return name
    if len(command) >= 2 and command[0] == "make":
        return f"make:{command[1]}"
    return command[0] if command else "unknown-stage"


def _selected_session_outcomes(plans: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "session_date": str(plan["session_date"]),
            "mandate_id": str(plan["mandate_id"]),
            "mandate_fingerprint": str(plan["mandate_fingerprint"]),
            "status": "selected",
            "started_at": None,
            "finished_at": None,
            "checkpoint_after": None,
            "failed_stage_index": None,
            "failed_stage_name": None,
            "failure_code": None,
            "stages": [],
        }
        for plan in plans
    ]


def _terminal_run_document(
    *,
    run_id: str,
    authority: Mapping[str, Any],
    started_at: datetime,
    finished_at: datetime,
    run_status: str,
    checkpoint_before: date | None,
    checkpoint_after: date | None,
    session_outcomes: Sequence[Mapping[str, Any]],
    failed_session: str | None,
    failed_stage_index: int | None,
    failed_stage_name: str | None,
    failure_code: str | None,
) -> dict[str, Any]:
    statuses = [str(session["status"]) for session in session_outcomes]
    return {
        "run_id": run_id,
        "model_version": "local-schedule-run-v1",
        "request_id": str(authority["authority_id"]),
        "plan_id": str(authority["plan_id"]),
        "authority_id": str(authority["authority_id"]),
        "authority_type": str(authority["authority_type"]),
        "schedule_id": str(authority["schedule_id"]),
        "schedule_fingerprint": str(authority["schedule_fingerprint"]),
        "calendar_id": str(authority["calendar_id"]),
        "calendar_fingerprint": str(authority["calendar_fingerprint"]),
        "portfolio_id": str(authority["portfolio_id"]),
        "risk_limit_policy_id": str(authority["risk_limit_policy_id"]),
        "mandate_id": str(authority["mandate_id"]),
        "mandate_fingerprint": str(authority["mandate_fingerprint"]),
        "as_of_date": str(authority["as_of_date"]),
        "latest_expected_session": str(authority["latest_expected_session"]),
        "readiness_decision_id": str(authority["readiness_decision_id"]),
        "readiness_document_sha256": str(authority["readiness_document_sha256"]),
        "override_id": authority.get("override_id"),
        "authorized_at": str(authority["authorized_at"]),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "run_status": run_status,
        "checkpoint_before": checkpoint_before.isoformat() if checkpoint_before else None,
        "checkpoint_after": checkpoint_after.isoformat() if checkpoint_after else None,
        "selected_session_count": len(session_outcomes),
        "started_session_count": sum(
            status in {"completed", "failed"} for status in statuses
        ),
        "completed_session_count": statuses.count("completed"),
        "failed_session": failed_session,
        "failed_stage_index": failed_stage_index,
        "failed_stage_name": failed_stage_name,
        "failure_code": failure_code,
        "sessions": [dict(session) for session in session_outcomes],
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def run_local_portfolio_schedule(
    *,
    schedule_id: str,
    as_of_date: date,
    schedule_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
    dsn: str,
    execute: bool = False,
    execution_authority: Mapping[str, Any] | None = None,
    python_executable: str = sys.executable,
    command_runner: CommandRunner | None = None,
    mandate_loader: MandateLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
    run_history_recorder: RunHistoryRecorder | None = None,
    clock: Clock | None = None,
) -> dict[str, Any]:
    schedule = load_local_portfolio_schedule(schedule_config_path, schedule_id)
    selected_calendar_loader = calendar_loader or load_market_calendar
    calendar = selected_calendar_loader(calendar_config_path, schedule.calendar_id)
    state_path = _state_path(state_dir, schedule.schedule_id)
    state = _read_state(state_path)
    checkpoint_before = _checkpoint_date(state, schedule=schedule)
    sessions = plan_schedule_sessions(
        schedule=schedule,
        calendar=calendar,
        as_of_date=as_of_date,
        state=state,
    )
    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    plans: list[dict[str, Any]] = []
    for session_date in sessions:
        mandate = selected_mandate_loader(
            portfolio_config_path,
            schedule.portfolio_id,
            session_date,
        )
        commands = build_session_commands(
            schedule=schedule,
            session_date=session_date,
            mandate=mandate,
            python_executable=python_executable,
            portfolio_config_path=portfolio_config_path,
            risk_limit_config_path=risk_limit_config_path,
            calendar_config_path=calendar_config_path,
            storage_config_path=storage_config_path,
            state_dir=state_dir,
        )
        plans.append(
            {
                "session_date": session_date.isoformat(),
                "mandate_id": mandate.mandate_id,
                "mandate_fingerprint": mandate.fingerprint,
                "commands": [list(command) for command in commands],
            }
        )

    summary: dict[str, Any] = {
        "run_id": str(uuid4()),
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "enabled": schedule.enabled,
        "calendar": {
            "calendar_id": calendar.calendar_id,
            "calendar_fingerprint": calendar.fingerprint,
        },
        "selection": {
            "as_of_date": as_of_date.isoformat(),
            "checkpoint_before": checkpoint_before.isoformat() if checkpoint_before else None,
            "sessions_selected": len(sessions),
            "session_dates": [value.isoformat() for value in sessions],
        },
        "plans": plans,
        "execution_authority": None,
        "execution": {
            "requested": execute,
            "performed": False,
            "completed_sessions": [],
        },
        "run_history": None,
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }
    if not execute or not sessions:
        return summary

    latest_expected_session = calendar.latest_expected_session(as_of_date)
    validated_authority = validate_operational_readiness_execution_authority(
        execution_authority,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        calendar_fingerprint=calendar.fingerprint,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        as_of_date=as_of_date,
        latest_expected_session=latest_expected_session,
        session_dates=sessions,
        mandate_fingerprints=[str(plan["mandate_fingerprint"]) for plan in plans],
    )
    summary["execution_authority"] = validated_authority

    if not schedule.enabled:
        raise ValidationError(
            "local schedule is disabled; enable it in reviewed configuration "
            "before execution"
        )
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")

    run_id = build_local_schedule_run_id(
        request_identifier=str(validated_authority["authority_id"]),
        plan_id=str(validated_authority["plan_id"]),
        authority_id=str(validated_authority["authority_id"]),
    )
    summary["run_id"] = run_id
    selected_runner = command_runner or _default_command_runner
    selected_recorder = run_history_recorder or record_local_schedule_run
    selected_clock = clock or (lambda: datetime.now(timezone.utc))
    environment = {
        "LOCAL_POSTGRES_DSN": dsn,
        "WAREHOUSE_POSTGRES_DSN": dsn,
    }
    lock_paths: list[Path] = []
    completed: list[str] = []
    session_outcomes = _selected_session_outcomes(plans)
    run_started_at = _clock_value(selected_clock)
    failure_code: str | None = None
    failed_session: str | None = None
    failed_stage_index: int | None = None
    failed_stage_name: str | None = None

    try:
        try:
            lock_paths = acquire_partition_locks(
                state_dir,
                [f"local-schedule/{schedule.schedule_id}"],
                run_id,
                stale_after_seconds=21_600,
            )
            if len(lock_paths) != 1:
                raise StorageError("local schedule did not acquire exactly one lock")
            for session_date, plan, outcome in zip(
                sessions,
                plans,
                session_outcomes,
                strict=True,
            ):
                session_started_at = _clock_value(selected_clock)
                outcome["started_at"] = session_started_at.isoformat()
                for stage_index, raw_command in enumerate(plan["commands"]):
                    command = tuple(str(value) for value in raw_command)
                    stage_name = _command_stage_name(command)
                    stage_started_at = _clock_value(selected_clock)
                    try:
                        selected_runner(command, environment)
                    except Exception:
                        stage_finished_at = _clock_value(selected_clock)
                        failure_code = "command_failed"
                        failed_session = session_date.isoformat()
                        failed_stage_index = stage_index
                        failed_stage_name = stage_name
                        outcome["status"] = "failed"
                        outcome["finished_at"] = stage_finished_at.isoformat()
                        outcome["failed_stage_index"] = stage_index
                        outcome["failed_stage_name"] = stage_name
                        outcome["failure_code"] = failure_code
                        outcome["stages"].append(
                            {
                                "stage_index": stage_index,
                                "stage_name": stage_name,
                                "status": "failed",
                                "started_at": stage_started_at.isoformat(),
                                "finished_at": stage_finished_at.isoformat(),
                                "failure_code": failure_code,
                            }
                        )
                        raise
                    stage_finished_at = _clock_value(selected_clock)
                    outcome["stages"].append(
                        {
                            "stage_index": stage_index,
                            "stage_name": stage_name,
                            "status": "completed",
                            "started_at": stage_started_at.isoformat(),
                            "finished_at": stage_finished_at.isoformat(),
                            "failure_code": None,
                        }
                    )

                checkpoint_index = len(plan["commands"])
                checkpoint_started_at = _clock_value(selected_clock)
                try:
                    _write_state(
                        state_path,
                        schedule=schedule,
                        session_date=session_date,
                    )
                except Exception:
                    checkpoint_finished_at = _clock_value(selected_clock)
                    failure_code = "checkpoint_failed"
                    failed_session = session_date.isoformat()
                    failed_stage_index = checkpoint_index
                    failed_stage_name = "checkpoint"
                    outcome["status"] = "failed"
                    outcome["finished_at"] = checkpoint_finished_at.isoformat()
                    outcome["failed_stage_index"] = checkpoint_index
                    outcome["failed_stage_name"] = "checkpoint"
                    outcome["failure_code"] = failure_code
                    outcome["stages"].append(
                        {
                            "stage_index": checkpoint_index,
                            "stage_name": "checkpoint",
                            "status": "failed",
                            "started_at": checkpoint_started_at.isoformat(),
                            "finished_at": checkpoint_finished_at.isoformat(),
                            "failure_code": failure_code,
                        }
                    )
                    raise
                checkpoint_finished_at = _clock_value(selected_clock)
                outcome["stages"].append(
                    {
                        "stage_index": checkpoint_index,
                        "stage_name": "checkpoint",
                        "status": "completed",
                        "started_at": checkpoint_started_at.isoformat(),
                        "finished_at": checkpoint_finished_at.isoformat(),
                        "failure_code": None,
                    }
                )
                outcome["status"] = "completed"
                outcome["finished_at"] = checkpoint_finished_at.isoformat()
                outcome["checkpoint_after"] = session_date.isoformat()
                completed.append(session_date.isoformat())
        finally:
            if lock_paths:
                release_partition_locks(lock_paths)
    except Exception as exc:
        if failure_code is None:
            failure_code = (
                "lock_overlap"
                if isinstance(exc, OverlapError)
                else "execution_stage_failed"
            )
        run_finished_at = _clock_value(selected_clock)
        checkpoint_after = date.fromisoformat(completed[-1]) if completed else checkpoint_before
        run_document = _terminal_run_document(
            run_id=run_id,
            authority=validated_authority,
            started_at=run_started_at,
            finished_at=run_finished_at,
            run_status="failed",
            checkpoint_before=checkpoint_before,
            checkpoint_after=checkpoint_after,
            session_outcomes=session_outcomes,
            failed_session=failed_session,
            failed_stage_index=failed_stage_index,
            failed_stage_name=failed_stage_name,
            failure_code=failure_code,
        )
        history = selected_recorder(dsn=dsn, run=run_document)
        summary["run_history"] = dict(history)
        summary["execution"] = {
            "requested": True,
            "performed": any(
                outcome["status"] in {"completed", "failed"}
                for outcome in session_outcomes
            ),
            "completed_sessions": completed,
            "checkpoint_after": checkpoint_after.isoformat() if checkpoint_after else None,
            "failure_code": failure_code,
            "failed_session": failed_session,
            "failed_stage_index": failed_stage_index,
            "failed_stage_name": failed_stage_name,
            "session_outcomes": session_outcomes,
        }
        raise

    run_finished_at = _clock_value(selected_clock)
    checkpoint_after = date.fromisoformat(completed[-1])
    run_document = _terminal_run_document(
        run_id=run_id,
        authority=validated_authority,
        started_at=run_started_at,
        finished_at=run_finished_at,
        run_status="completed",
        checkpoint_before=checkpoint_before,
        checkpoint_after=checkpoint_after,
        session_outcomes=session_outcomes,
        failed_session=None,
        failed_stage_index=None,
        failed_stage_name=None,
        failure_code=None,
    )
    history = selected_recorder(dsn=dsn, run=run_document)
    summary["run_history"] = dict(history)
    summary["execution"] = {
        "requested": True,
        "performed": True,
        "completed_sessions": completed,
        "checkpoint_after": completed[-1],
        "session_outcomes": session_outcomes,
    }
    return summary


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        ) from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError("must be a calendar date in YYYY-MM-DD format")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan a bounded local portfolio schedule. Direct execution requires "
            "readiness-enforced authority from the dedicated wrapper."
        )
    )
    parser.add_argument("--schedule-id", required=True)
    parser.add_argument("--as-of-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--schedule-config",
        type=Path,
        default=Path("config/local_portfolio_schedules.yaml"),
    )
    parser.add_argument(
        "--calendar-config",
        type=Path,
        default=Path("config/market_calendars.yaml"),
    )
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument(
        "--risk-limit-config",
        type=Path,
        default=Path("config/portfolio_risk_limits.yaml"),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--state-dir", type=Path, default=Path(".scheduler"))
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write local schedule summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_local_portfolio_schedule(
            schedule_id=args.schedule_id,
            as_of_date=args.as_of_date,
            schedule_config_path=args.schedule_config,
            calendar_config_path=args.calendar_config,
            portfolio_config_path=args.portfolio_config,
            risk_limit_config_path=args.risk_limit_config,
            storage_config_path=args.storage_config,
            state_dir=args.state_dir,
            dsn=args.dsn,
            execute=args.execute,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Local portfolio schedule failed: configuration, checkpoint, "
            "calendar, options, or execution authority were invalid",
            file=sys.stderr,
        )
        return 1
    except OverlapError:
        print(
            "Local portfolio schedule failed: another schedule run owns the lock",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Local portfolio schedule failed: a local stage failed; the last "
            "completed checkpoint is unchanged",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Local portfolio schedule failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
