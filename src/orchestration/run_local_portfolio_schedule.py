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
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN
from .locks import acquire_partition_locks, release_partition_locks

MODEL_VERSION = "local-portfolio-schedule-v1"
MAX_CATCH_UP_SESSIONS = 31
MAX_STATE_BYTES = 65_536

Command = tuple[str, ...]
CommandRunner = Callable[[Command, Mapping[str, str]], None]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
CalendarLoader = Callable[[Path, str], MarketCalendar]


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
        raise ValidationError(
            f"local schedule '{schedule_id}' is not configured"
        )
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
        portfolio_id=_required_text(
            candidate.get("portfolio_id"),
            "portfolio_id",
        ),
        policy_id=_required_text(candidate.get("policy_id"), "policy_id"),
        calendar_id=_required_text(
            candidate.get("calendar_id"),
            "calendar_id",
        ),
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


def load_local_portfolio_schedule(
    path: Path,
    schedule_id: str,
) -> LocalPortfolioSchedule:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "local schedule configuration could not be loaded"
        ) from None
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
        raise ValidationError(
            "local schedule state is missing last_successful_session"
        )
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        raise ValidationError(
            "local schedule checkpoint must use YYYY-MM-DD"
        ) from None
    if raw != parsed.isoformat():
        raise ValidationError(
            "local schedule checkpoint must use YYYY-MM-DD"
        )
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
    candidates = calendar.sessions_between(
        checkpoint + timedelta(days=1),
        latest_expected,
    )
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


def _default_command_runner(
    command: Command,
    environment: Mapping[str, str],
) -> None:
    merged_environment = dict(os.environ)
    merged_environment.update(environment)
    try:
        subprocess.run(
            list(command),
            check=True,
            env=merged_environment,
        )
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
    python_executable: str = sys.executable,
    command_runner: CommandRunner | None = None,
    mandate_loader: MandateLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
) -> dict[str, Any]:
    schedule = load_local_portfolio_schedule(
        schedule_config_path,
        schedule_id,
    )
    selected_calendar_loader = calendar_loader or load_market_calendar
    calendar = selected_calendar_loader(
        calendar_config_path,
        schedule.calendar_id,
    )
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
            "checkpoint_before": (
                checkpoint_before.isoformat()
                if checkpoint_before is not None
                else None
            ),
            "sessions_selected": len(sessions),
            "session_dates": [value.isoformat() for value in sessions],
        },
        "plans": plans,
        "execution": {
            "requested": execute,
            "performed": False,
            "completed_sessions": [],
        },
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }
    if not execute or not sessions:
        return summary
    if not schedule.enabled:
        raise ValidationError(
            "local schedule is disabled; enable it in reviewed configuration "
            "before execution"
        )
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")

    selected_runner = command_runner or _default_command_runner
    environment = {
        "LOCAL_POSTGRES_DSN": dsn,
        "WAREHOUSE_POSTGRES_DSN": dsn,
    }
    lock_paths: list[Path] = []
    completed: list[str] = []
    try:
        lock_paths = acquire_partition_locks(
            state_dir,
            [f"local-schedule/{schedule.schedule_id}"],
            summary["run_id"],
            stale_after_seconds=21_600,
        )
        if len(lock_paths) != 1:
            raise StorageError(
                "local schedule did not acquire exactly one lock"
            )
        for session_date, plan in zip(sessions, plans, strict=True):
            for raw_command in plan["commands"]:
                selected_runner(tuple(raw_command), environment)
            _write_state(
                state_path,
                schedule=schedule,
                session_date=session_date,
            )
            completed.append(session_date.isoformat())
    finally:
        if lock_paths:
            release_partition_locks(lock_paths)

    summary["execution"] = {
        "requested": True,
        "performed": True,
        "completed_sessions": completed,
        "checkpoint_after": completed[-1],
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
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or explicitly execute a bounded local portfolio schedule."
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
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
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
            "calendar, or options were invalid",
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
        print(
            "Local portfolio schedule failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
