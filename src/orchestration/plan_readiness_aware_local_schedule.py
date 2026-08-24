from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from src.analytics.market_calendar import MarketCalendar, load_market_calendar
from src.analytics.operational_service_levels import (
    OperationalServiceLevelPolicy,
    load_operational_service_level_policy,
)
from src.analytics.portfolio_mandates import (
    PortfolioMandate,
    load_portfolio_mandate,
)
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_local_portfolio_schedule import (
    LocalPortfolioSchedule,
    load_local_portfolio_schedule,
    run_local_portfolio_schedule,
)
from src.warehouse.operational_readiness_decision_reader import (
    read_current_operational_readiness_decision,
)
from src.warehouse.operational_readiness_gate import (
    OperationalReadinessGatePolicy,
    load_operational_readiness_gate_policy,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "readiness-aware-schedule-plan-v1"

ScheduleLoader = Callable[[Path, str], LocalPortfolioSchedule]
GateLoader = Callable[[Path, str], OperationalReadinessGatePolicy]
OperationalPolicyLoader = Callable[[Path, str], OperationalServiceLevelPolicy]
CalendarLoader = Callable[[Path, str], MarketCalendar]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
ReadinessReader = Callable[..., dict[str, Any] | None]
SchedulePlanner = Callable[..., Mapping[str, Any]]


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


def _default_schedule_planner(**kwargs: Any) -> Mapping[str, Any]:
    return run_local_portfolio_schedule(
        **kwargs,
        execute=False,
        command_runner=lambda *_: (_ for _ in ()).throw(
            AssertionError("plan-only readiness integration executed a command")
        ),
    )


def _deterministic_schedule_plan(
    raw: Mapping[str, Any],
    *,
    schedule: LocalPortfolioSchedule,
    calendar: MarketCalendar,
    as_of_date: date,
) -> dict[str, Any]:
    required = {
        "schedule_id",
        "schedule_fingerprint",
        "enabled",
        "calendar",
        "selection",
        "plans",
        "execution",
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
    }
    if not required.issubset(raw):
        raise StorageError("local schedule plan is missing required evidence")
    if raw.get("schedule_id") != schedule.schedule_id:
        raise StorageError("local schedule plan belongs to another schedule")
    if raw.get("schedule_fingerprint") != schedule.fingerprint:
        raise StorageError("local schedule plan fingerprint changed during planning")
    if raw.get("enabled") != schedule.enabled:
        raise StorageError("local schedule enabled state changed during planning")
    calendar_evidence = raw.get("calendar")
    if not isinstance(calendar_evidence, Mapping) or calendar_evidence != {
        "calendar_id": calendar.calendar_id,
        "calendar_fingerprint": calendar.fingerprint,
    }:
        raise StorageError("local schedule calendar evidence changed during planning")
    selection = raw.get("selection")
    if not isinstance(selection, Mapping):
        raise StorageError("local schedule selection evidence is incompatible")
    if selection.get("as_of_date") != as_of_date.isoformat():
        raise StorageError("local schedule plan uses another as_of_date")
    session_dates = selection.get("session_dates")
    sessions_selected = selection.get("sessions_selected")
    if (
        not isinstance(session_dates, list)
        or any(not isinstance(value, str) for value in session_dates)
        or type(sessions_selected) is not int
        or sessions_selected != len(session_dates)
    ):
        raise StorageError("local schedule selected sessions are incompatible")
    plans = raw.get("plans")
    if not isinstance(plans, list) or len(plans) != sessions_selected:
        raise StorageError("local schedule command plans are incompatible")
    execution = raw.get("execution")
    if not isinstance(execution, Mapping) or execution != {
        "requested": False,
        "performed": False,
        "completed_sessions": [],
    }:
        raise StorageError("local schedule plan performed execution")
    for flag in (
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
    ):
        if raw.get(flag) is not False:
            raise StorageError("local schedule plan reported a side effect")
    return {
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "enabled": schedule.enabled,
        "calendar": dict(calendar_evidence),
        "selection": dict(selection),
        "plans": plans,
    }


def _plan_id(payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-plan-{digest}"


def plan_readiness_aware_local_schedule(
    *,
    schedule_id: str,
    gate_id: str,
    as_of_date: date,
    schedule_config_path: Path,
    gate_config_path: Path,
    operational_policy_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
    dsn: str,
    python_executable: str = sys.executable,
    schedule_loader: ScheduleLoader | None = None,
    gate_loader: GateLoader | None = None,
    operational_policy_loader: OperationalPolicyLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
    mandate_loader: MandateLoader | None = None,
    readiness_reader: ReadinessReader | None = None,
    schedule_planner: SchedulePlanner | None = None,
) -> dict[str, Any]:
    selected_schedule_loader = schedule_loader or load_local_portfolio_schedule
    selected_gate_loader = gate_loader or load_operational_readiness_gate_policy
    selected_policy_loader = (
        operational_policy_loader or load_operational_service_level_policy
    )
    selected_calendar_loader = calendar_loader or load_market_calendar
    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    selected_readiness_reader = (
        readiness_reader or read_current_operational_readiness_decision
    )
    selected_schedule_planner = schedule_planner or _default_schedule_planner

    schedule = selected_schedule_loader(schedule_config_path, schedule_id)
    gate = selected_gate_loader(gate_config_path, gate_id)
    policy = selected_policy_loader(
        operational_policy_config_path,
        gate.operational_policy_id,
    )
    if policy.policy_id != gate.operational_policy_id:
        raise ValidationError("readiness gate selected another operational policy")
    if policy.schedule_id != schedule.schedule_id:
        raise ValidationError(
            "operational readiness policy belongs to another schedule"
        )

    calendar = selected_calendar_loader(
        calendar_config_path,
        schedule.calendar_id,
    )
    calendar.require_covered(as_of_date)
    latest_expected_session = calendar.latest_expected_session(as_of_date)
    mandate = selected_mandate_loader(
        portfolio_config_path,
        schedule.portfolio_id,
        latest_expected_session,
    )

    readiness = selected_readiness_reader(
        dsn=dsn,
        gate_id=gate.gate_id,
        gate_fingerprint=gate.fingerprint,
        operational_policy_id=policy.policy_id,
        operational_policy_fingerprint=policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        latest_expected_session=latest_expected_session,
    )

    raw_schedule_plan = selected_schedule_planner(
        schedule_id=schedule.schedule_id,
        as_of_date=as_of_date,
        schedule_config_path=schedule_config_path,
        calendar_config_path=calendar_config_path,
        portfolio_config_path=portfolio_config_path,
        risk_limit_config_path=risk_limit_config_path,
        storage_config_path=storage_config_path,
        state_dir=state_dir,
        dsn=dsn,
        python_executable=python_executable,
    )
    if not isinstance(raw_schedule_plan, Mapping):
        raise StorageError("local schedule planner returned invalid evidence")
    schedule_plan = _deterministic_schedule_plan(
        raw_schedule_plan,
        schedule=schedule,
        calendar=calendar,
        as_of_date=as_of_date,
    )
    sessions_selected = schedule_plan["selection"]["sessions_selected"]
    if sessions_selected == 0:
        schedule_effect = "no_work"
    elif readiness is not None and readiness.get("decision") == "allow":
        schedule_effect = "would_run"
    else:
        schedule_effect = "would_block"

    readiness_evidence = {
        "status": "missing" if readiness is None else "current",
        "decision_id": readiness.get("decision_id") if readiness else None,
        "decision": readiness.get("decision") if readiness else "block",
        "reasons": readiness.get("reasons") if readiness else ["decision_missing"],
        "document_sha256": (
            readiness.get("document_sha256") if readiness else None
        ),
        "recorded_at": readiness.get("recorded_at") if readiness else None,
        "gate_id": gate.gate_id,
        "gate_fingerprint": gate.fingerprint,
        "operational_policy_id": policy.policy_id,
        "operational_policy_fingerprint": policy.fingerprint,
        "latest_expected_session": latest_expected_session.isoformat(),
    }
    identity_payload = {
        "as_of_date": as_of_date.isoformat(),
        "calendar_fingerprint": calendar.fingerprint,
        "gate_fingerprint": gate.fingerprint,
        "latest_expected_session": latest_expected_session.isoformat(),
        "mandate_fingerprint": mandate.fingerprint,
        "model_version": MODEL_VERSION,
        "operational_policy_fingerprint": policy.fingerprint,
        "readiness_decision": readiness_evidence["decision"],
        "readiness_decision_id": readiness_evidence["decision_id"],
        "readiness_reasons": readiness_evidence["reasons"],
        "schedule_effect": schedule_effect,
        "schedule_fingerprint": schedule.fingerprint,
        "schedule_plan": schedule_plan,
    }
    return {
        "plan_id": _plan_id(identity_payload),
        "model_version": MODEL_VERSION,
        "schedule_id": schedule.schedule_id,
        "schedule_fingerprint": schedule.fingerprint,
        "portfolio_id": schedule.portfolio_id,
        "risk_limit_policy_id": schedule.policy_id,
        "mandate_id": mandate.mandate_id,
        "mandate_fingerprint": mandate.fingerprint,
        "as_of_date": as_of_date.isoformat(),
        "latest_expected_session": latest_expected_session.isoformat(),
        "readiness": readiness_evidence,
        "schedule_plan": schedule_plan,
        "schedule_effect": {
            "decision": schedule_effect,
            "sessions_selected": sessions_selected,
            "session_dates": schedule_plan["selection"]["session_dates"],
        },
        "execution": {
            "requested": False,
            "performed": False,
            "completed_sessions": [],
        },
        "checkpoint_updated": False,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Combine a local schedule plan with the exact retained operational "
            "readiness decision without executing commands."
        )
    )
    parser.add_argument("--schedule-id", required=True)
    parser.add_argument("--gate-id", required=True)
    parser.add_argument("--as-of-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--schedule-config",
        type=Path,
        default=Path("config/local_portfolio_schedules.yaml"),
    )
    parser.add_argument(
        "--gate-config",
        type=Path,
        default=Path("config/operational_readiness_gates.yaml"),
    )
    parser.add_argument(
        "--operational-policy-config",
        type=Path,
        default=Path("config/operational_service_levels.yaml"),
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
        raise StorageError("Unable to write readiness-aware schedule plan") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = plan_readiness_aware_local_schedule(
            schedule_id=args.schedule_id,
            gate_id=args.gate_id,
            as_of_date=args.as_of_date,
            schedule_config_path=args.schedule_config,
            gate_config_path=args.gate_config,
            operational_policy_config_path=args.operational_policy_config,
            calendar_config_path=args.calendar_config,
            portfolio_config_path=args.portfolio_config,
            risk_limit_config_path=args.risk_limit_config,
            storage_config_path=args.storage_config,
            state_dir=args.state_dir,
            dsn=args.dsn,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Readiness-aware schedule planning failed: configuration or "
            "retained evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Readiness-aware schedule planning failed: local or PostgreSQL "
            "operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Readiness-aware schedule planning failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 2 if summary["schedule_effect"]["decision"] == "would_block" else 0


if __name__ == "__main__":
    raise SystemExit(main())
