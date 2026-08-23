from __future__ import annotations

import argparse
import inspect
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from importlib import import_module
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..common.config import load_yaml
from ..common.exceptions import OverlapError, StorageError, ValidationError
from .locks import acquire_partition_locks, release_partition_locks

MAX_CATCHUP_DAYS = 31
DEFAULT_STALE_LOCK_SECONDS = 21_600

Stage = Callable[..., dict[str, Any]]
LockAcquirer = Callable[..., list[Path]]
LockReleaser = Callable[[list[Path]], None]


@dataclass(frozen=True, slots=True)
class LocalSchedule:
    schedule_id: str
    enabled: bool
    portfolio_id: str
    policy_id: str
    source: str
    symbols: tuple[str, ...]
    volatility_window: int
    var_window: int
    var_confidence: float
    covariance_window: int
    max_snapshots: int
    max_evaluations: int
    max_notifications: int
    max_catchup_days: int
    initial_lookback_days: int


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _positive_integer(
    value: Any,
    label: str,
    *,
    maximum: int | None = None,
) -> int:
    if type(value) is not int or value <= 0:
        raise ValidationError(f"{label} must be a positive integer")
    if maximum is not None and value > maximum:
        raise ValidationError(f"{label} must not exceed {maximum}")
    return value


def parse_local_schedule(
    payload: Mapping[str, Any],
    schedule_id: str,
) -> LocalSchedule:
    schedules = payload.get("schedules")
    if not isinstance(schedules, Mapping):
        raise ValidationError("local schedule configuration must define schedules")
    candidate = schedules.get(schedule_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"schedule '{schedule_id}' is not configured")

    enabled = candidate.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("schedule enabled must be true or false")

    symbols_raw = candidate.get("symbols")
    if (
        not isinstance(symbols_raw, Sequence)
        or isinstance(symbols_raw, (str, bytes))
        or not 1 <= len(symbols_raw) <= 50
    ):
        raise ValidationError("schedule symbols must contain between 1 and 50 items")
    symbols = tuple(
        _required_text(value, "schedule symbol").upper()
        for value in symbols_raw
    )
    if len(set(symbols)) != len(symbols):
        raise ValidationError("schedule symbols must be unique")

    confidence = candidate.get("var_confidence")
    if (
        isinstance(confidence, bool)
        or not isinstance(confidence, (int, float))
        or not 0 < float(confidence) < 1
    ):
        raise ValidationError("var_confidence must be between 0 and 1")

    max_catchup_days = _positive_integer(
        candidate.get("max_catchup_days"),
        "max_catchup_days",
        maximum=MAX_CATCHUP_DAYS,
    )
    initial_lookback_days = _positive_integer(
        candidate.get("initial_lookback_days", 1),
        "initial_lookback_days",
        maximum=max_catchup_days,
    )

    return LocalSchedule(
        schedule_id=_required_text(schedule_id, "schedule_id"),
        enabled=enabled,
        portfolio_id=_required_text(
            candidate.get("portfolio_id"),
            "portfolio_id",
        ),
        policy_id=_required_text(candidate.get("policy_id"), "policy_id"),
        source=_required_text(candidate.get("source"), "source"),
        symbols=symbols,
        volatility_window=_positive_integer(
            candidate.get("volatility_window"),
            "volatility_window",
        ),
        var_window=_positive_integer(candidate.get("var_window"), "var_window"),
        var_confidence=float(confidence),
        covariance_window=_positive_integer(
            candidate.get("covariance_window"),
            "covariance_window",
        ),
        max_snapshots=_positive_integer(
            candidate.get("max_snapshots"),
            "max_snapshots",
        ),
        max_evaluations=_positive_integer(
            candidate.get("max_evaluations"),
            "max_evaluations",
        ),
        max_notifications=_positive_integer(
            candidate.get("max_notifications"),
            "max_notifications",
        ),
        max_catchup_days=max_catchup_days,
        initial_lookback_days=initial_lookback_days,
    )


def load_local_schedule(path: Path, schedule_id: str) -> LocalSchedule:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("local schedule configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("local schedule configuration must be a mapping")
    return parse_local_schedule(payload, schedule_id)


def _state_path(state_dir: Path, schedule_id: str) -> Path:
    if state_dir.is_symlink():
        raise StorageError("local schedule state directory must not be a symbolic link")
    return state_dir / f"{schedule_id}.json"


def _read_checkpoint(path: Path, schedule_id: str) -> date | None:
    if not path.exists():
        return None
    if path.is_symlink() or not path.is_file():
        raise StorageError("local schedule checkpoint must be a regular file")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise StorageError("local schedule checkpoint is invalid") from None
    if not isinstance(payload, Mapping) or payload.get("schedule_id") != schedule_id:
        raise StorageError("local schedule checkpoint identity is invalid")
    value = payload.get("last_success_date")
    if not isinstance(value, str):
        raise StorageError("local schedule checkpoint date is invalid")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise StorageError("local schedule checkpoint date is invalid") from None
    if value != parsed.isoformat():
        raise StorageError("local schedule checkpoint date is invalid")
    return parsed


def _write_checkpoint(path: Path, schedule_id: str, run_date: date) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.parent.is_symlink():
        raise StorageError("local schedule state directory must not be a symbolic link")
    temporary = path.with_suffix(path.suffix + ".tmp")
    payload = {
        "last_success_date": run_date.isoformat(),
        "schedule_id": schedule_id,
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


def plan_due_dates(
    schedule: LocalSchedule,
    *,
    as_of_date: date,
    last_success_date: date | None,
) -> tuple[date, ...]:
    if type(as_of_date) is not date:
        raise ValidationError("as_of_date must be a calendar date")
    if last_success_date is not None and last_success_date > as_of_date:
        raise ValidationError("checkpoint must not be after as_of_date")

    if last_success_date is None:
        first = as_of_date - timedelta(days=schedule.initial_lookback_days - 1)
    else:
        first = last_success_date + timedelta(days=1)
    if first > as_of_date:
        return ()

    available = (as_of_date - first).days + 1
    selected = min(available, schedule.max_catchup_days)
    return tuple(first + timedelta(days=offset) for offset in range(selected))


def _default_stage(module_name: str, function_name: str) -> Stage:
    def invoke(**kwargs: Any) -> dict[str, Any]:
        try:
            function = getattr(import_module(module_name), function_name)
        except (ImportError, AttributeError):
            raise StorageError(
                f"local schedule stage {function_name} is unavailable"
            ) from None
        signature = inspect.signature(function)
        accepted = {
            name: value
            for name, value in kwargs.items()
            if name in signature.parameters
        }
        result = function(**accepted)
        if not isinstance(result, dict):
            raise StorageError(
                f"local schedule stage {function_name} returned invalid evidence"
            )
        return result

    return invoke


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD") from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or explicitly run one bounded local schedule invocation. "
            "Configuration is disabled by default."
        )
    )
    parser.add_argument("--schedule-id", required=True)
    parser.add_argument("--as-of-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--schedule-config",
        type=Path,
        default=Path("config/local_schedule.yaml"),
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
    parser.add_argument("--lock-base-dir", type=Path, default=Path("."))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-disabled-run", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _run_date(
    *,
    schedule: LocalSchedule,
    run_date: date,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
    daily_stage: Stage,
    governed_stage: Stage,
    notification_stage: Stage,
) -> dict[str, Any]:
    daily: dict[str, Any] = {}
    for symbol in schedule.symbols:
        daily[symbol] = daily_stage(
            source=schedule.source,
            symbol=symbol,
            start_date=None,
            end_date=run_date,
            volatility_window=schedule.volatility_window,
            var_window=schedule.var_window,
            var_confidence=schedule.var_confidence,
            storage_config_path=storage_config_path,
        )

    governed = governed_stage(
        portfolio_id=schedule.portfolio_id,
        policy_id=schedule.policy_id,
        portfolio_config_path=portfolio_config_path,
        risk_limit_config_path=risk_limit_config_path,
        start_date=run_date,
        end_date=run_date,
        volatility_window=schedule.volatility_window,
        var_window=schedule.var_window,
        var_confidence=schedule.var_confidence,
        covariance_window=schedule.covariance_window,
        max_snapshots=schedule.max_snapshots,
        max_evaluations=schedule.max_evaluations,
        storage_config_path=storage_config_path,
        lock_base_dir=state_dir,
    )
    notifications = notification_stage(
        policy_id=schedule.policy_id,
        start_date=run_date,
        end_date=run_date,
        max_notifications=schedule.max_notifications,
        storage_config_path=storage_config_path,
    )
    return {
        "daily_risk": daily,
        "governed_portfolio_cycle": governed,
        "risk_limit_notifications": notifications,
        "run_date": run_date.isoformat(),
    }


def run_local_schedule(
    *,
    schedule_id: str,
    as_of_date: date,
    schedule_config_path: Path,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
    lock_base_dir: Path,
    dry_run: bool = False,
    allow_disabled_run: bool = False,
    schedule_loader: Callable[[Path, str], LocalSchedule] | None = None,
    daily_stage: Stage | None = None,
    governed_stage: Stage | None = None,
    notification_stage: Stage | None = None,
    lock_acquirer: LockAcquirer | None = None,
    lock_releaser: LockReleaser | None = None,
) -> dict[str, Any]:
    selected_loader = schedule_loader or load_local_schedule
    schedule = selected_loader(schedule_config_path, schedule_id)
    checkpoint_path = _state_path(state_dir, schedule.schedule_id)
    last_success = _read_checkpoint(checkpoint_path, schedule.schedule_id)
    due_dates = plan_due_dates(
        schedule,
        as_of_date=as_of_date,
        last_success_date=last_success,
    )
    permitted = schedule.enabled or allow_disabled_run
    base = {
        "run_id": str(uuid4()),
        "schedule_id": schedule.schedule_id,
        "enabled": schedule.enabled,
        "execution_permitted": permitted,
        "as_of_date": as_of_date.isoformat(),
        "last_success_date": (
            last_success.isoformat() if last_success is not None else None
        ),
        "due_dates": [value.isoformat() for value in due_dates],
        "catchup_remaining": max(
            0,
            (
                as_of_date - due_dates[-1]
            ).days if due_dates else 0,
        ),
        "provider_request_performed": False,
    }
    if dry_run or not permitted or not due_dates:
        return {
            **base,
            "execution": {
                "performed": False,
                "reason": (
                    "dry_run"
                    if dry_run
                    else "schedule_disabled"
                    if not permitted
                    else "nothing_due"
                ),
            },
            "runs": [],
        }

    selected_daily = daily_stage or _default_stage(
        "src.orchestration.run_daily_risk",
        "run_daily_risk",
    )
    selected_governed = governed_stage or _default_stage(
        "src.orchestration.run_governed_portfolio_cycle",
        "run_governed_portfolio_cycle",
    )
    selected_notifications = notification_stage or _default_stage(
        "src.orchestration.run_portfolio_risk_limit_notifications",
        "run_portfolio_risk_limit_notifications",
    )
    selected_lock_acquirer = lock_acquirer or acquire_partition_locks
    selected_lock_releaser = lock_releaser or release_partition_locks

    lock_paths: list[Path] = []
    completed: list[dict[str, Any]] = []
    try:
        lock_paths = selected_lock_acquirer(
            lock_base_dir,
            [f"local-schedule/{schedule.schedule_id}"],
            base["run_id"],
            stale_after_seconds=DEFAULT_STALE_LOCK_SECONDS,
        )
        if len(lock_paths) != 1:
            raise StorageError("local schedule did not acquire exactly one lock")
        for run_date in due_dates:
            evidence = _run_date(
                schedule=schedule,
                run_date=run_date,
                portfolio_config_path=portfolio_config_path,
                risk_limit_config_path=risk_limit_config_path,
                storage_config_path=storage_config_path,
                state_dir=state_dir,
                daily_stage=selected_daily,
                governed_stage=selected_governed,
                notification_stage=selected_notifications,
            )
            completed.append(evidence)
            _write_checkpoint(
                checkpoint_path,
                schedule.schedule_id,
                run_date,
            )
    finally:
        if lock_paths:
            selected_lock_releaser(lock_paths)

    return {
        **base,
        "execution": {
            "performed": True,
            "completed_run_dates": [item["run_date"] for item in completed],
        },
        "runs": completed,
    }


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
        summary = run_local_schedule(
            schedule_id=args.schedule_id,
            as_of_date=args.as_of_date,
            schedule_config_path=args.schedule_config,
            portfolio_config_path=args.portfolio_config,
            risk_limit_config_path=args.risk_limit_config,
            storage_config_path=args.storage_config,
            state_dir=args.state_dir,
            lock_base_dir=args.lock_base_dir,
            dry_run=args.dry_run,
            allow_disabled_run=args.allow_disabled_run,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Local schedule failed: configuration, state, or options were invalid",
            file=sys.stderr,
        )
        return 1
    except OverlapError:
        print("Local schedule failed: another invocation owns the lock", file=sys.stderr)
        return 1
    except StorageError:
        print(
            "Local schedule failed: local state or a stage failed; resume is bounded",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Local schedule failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
