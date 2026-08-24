from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.operational_readiness_execution_authority import (
    build_operational_readiness_execution_authority,
)
from src.orchestration.plan_readiness_aware_local_schedule import (
    plan_readiness_aware_local_schedule,
)
from src.orchestration.run_local_portfolio_schedule import (
    CommandRunner,
    run_local_portfolio_schedule,
)
from src.warehouse.operational_readiness_decision_reader import (
    read_current_operational_readiness_decision,
)
from src.warehouse.operational_readiness_override_registry import (
    read_active_operational_readiness_override,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "readiness-enforced-local-schedule-v1"

PlanBuilder = Callable[..., Mapping[str, Any]]
CurrentDecisionReader = Callable[..., dict[str, Any] | None]
OverrideReader = Callable[..., dict[str, Any] | None]
ScheduleExecutor = Callable[..., Mapping[str, Any]]


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


def _aware_utc(value: str | datetime, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise StorageError(f"{label} is incompatible")
    return value


def _current_decision_contract(plan: Mapping[str, Any]) -> dict[str, Any]:
    readiness = _mapping(plan.get("readiness"), "readiness plan evidence")
    schedule_plan = _mapping(plan.get("schedule_plan"), "schedule plan evidence")
    calendar = _mapping(schedule_plan.get("calendar"), "schedule calendar evidence")
    required = {
        "gate_id": readiness.get("gate_id"),
        "gate_fingerprint": readiness.get("gate_fingerprint"),
        "operational_policy_id": readiness.get("operational_policy_id"),
        "operational_policy_fingerprint": readiness.get(
            "operational_policy_fingerprint"
        ),
        "schedule_id": plan.get("schedule_id"),
        "schedule_fingerprint": plan.get("schedule_fingerprint"),
        "calendar_id": calendar.get("calendar_id"),
        "portfolio_id": plan.get("portfolio_id"),
        "risk_limit_policy_id": plan.get("risk_limit_policy_id"),
        "mandate_fingerprint": plan.get("mandate_fingerprint"),
        "latest_expected_session": plan.get("latest_expected_session"),
    }
    if any(not isinstance(value, str) or not value for value in required.values()):
        raise StorageError("readiness plan current-contract evidence is incomplete")
    return required


def _refresh_current_decision(
    *,
    dsn: str,
    plan: Mapping[str, Any],
    reader: CurrentDecisionReader,
) -> dict[str, Any] | None:
    contract = _current_decision_contract(plan)
    current = reader(
        dsn=dsn,
        gate_id=contract["gate_id"],
        gate_fingerprint=contract["gate_fingerprint"],
        operational_policy_id=contract["operational_policy_id"],
        operational_policy_fingerprint=contract[
            "operational_policy_fingerprint"
        ],
        schedule_id=contract["schedule_id"],
        schedule_fingerprint=contract["schedule_fingerprint"],
        calendar_id=contract["calendar_id"],
        portfolio_id=contract["portfolio_id"],
        risk_limit_policy_id=contract["risk_limit_policy_id"],
        mandate_fingerprint=contract["mandate_fingerprint"],
        latest_expected_session=date.fromisoformat(
            contract["latest_expected_session"]
        ),
    )
    if current is None:
        return None
    planned = _mapping(plan.get("readiness"), "planned readiness evidence")
    if (
        current.get("decision_id") != planned.get("decision_id")
        or current.get("document_sha256") != planned.get("document_sha256")
    ):
        raise ValidationError(
            "current readiness decision changed after planning; generate a new plan"
        )
    return current


def _decision_is_fresh(
    decision: Mapping[str, Any],
    authorized_at: datetime,
) -> tuple[bool, str | None]:
    raw_evaluated_at = decision.get("evaluated_at")
    if not isinstance(raw_evaluated_at, (str, datetime)):
        raise StorageError("current readiness decision timestamp is incompatible")
    evaluated_at = _aware_utc(raw_evaluated_at, "decision.evaluated_at")
    max_age = decision.get("max_report_age_seconds")
    if type(max_age) is not int or max_age < 1:
        raise StorageError("current readiness decision age limit is incompatible")
    age_seconds = (authorized_at - evaluated_at).total_seconds()
    if age_seconds < 0:
        return False, "decision_timestamp_future"
    if age_seconds > max_age:
        return False, "decision_age_exceeds_limit"
    return True, None


def _enriched_plan(
    plan: Mapping[str, Any],
    current_decision: Mapping[str, Any],
) -> dict[str, Any]:
    enriched = dict(plan)
    enriched["readiness"] = {
        **dict(current_decision),
        "status": "current",
        "document_sha256": current_decision["document_sha256"],
        "recorded_at": current_decision["recorded_at"],
    }
    return enriched


def _control_id(payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-control-{digest}"


def _base_summary(
    *,
    plan: Mapping[str, Any],
    execute: bool,
    evaluated_at: datetime,
) -> dict[str, Any]:
    return {
        "control_id": "",
        "model_version": MODEL_VERSION,
        "evaluated_at": evaluated_at.isoformat(),
        "execution_requested": execute,
        "plan_id": plan.get("plan_id"),
        "schedule_id": plan.get("schedule_id"),
        "schedule_fingerprint": plan.get("schedule_fingerprint"),
        "portfolio_id": plan.get("portfolio_id"),
        "risk_limit_policy_id": plan.get("risk_limit_policy_id"),
        "mandate_id": plan.get("mandate_id"),
        "mandate_fingerprint": plan.get("mandate_fingerprint"),
        "as_of_date": plan.get("as_of_date"),
        "latest_expected_session": plan.get("latest_expected_session"),
        "readiness": plan.get("readiness"),
        "schedule_effect": plan.get("schedule_effect"),
        "execution_authority": None,
        "execution": {
            "requested": execute,
            "performed": False,
            "completed_sessions": [],
        },
        "checkpoint_before": _mapping(
            _mapping(plan.get("schedule_plan"), "schedule plan").get("selection"),
            "schedule selection",
        ).get("checkpoint_before"),
        "checkpoint_after": None,
        "decision": "plan_only" if not execute else "pending",
        "block_reasons": [],
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def run_readiness_enforced_local_schedule(
    *,
    schedule_id: str,
    gate_id: str,
    as_of_date: date,
    evaluated_at: datetime | str,
    schedule_config_path: Path,
    gate_config_path: Path,
    operational_policy_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    storage_config_path: Path,
    state_dir: Path,
    dsn: str,
    execute: bool = False,
    python_executable: str = sys.executable,
    command_runner: CommandRunner | None = None,
    plan_builder: PlanBuilder | None = None,
    current_decision_reader: CurrentDecisionReader | None = None,
    override_reader: OverrideReader | None = None,
    schedule_executor: ScheduleExecutor | None = None,
) -> dict[str, Any]:
    authorization_time = _aware_utc(evaluated_at, "evaluated_at")
    selected_plan_builder = plan_builder or plan_readiness_aware_local_schedule
    selected_decision_reader = (
        current_decision_reader or read_current_operational_readiness_decision
    )
    selected_override_reader = (
        override_reader or read_active_operational_readiness_override
    )
    selected_schedule_executor = schedule_executor or run_local_portfolio_schedule

    plan = selected_plan_builder(
        schedule_id=schedule_id,
        gate_id=gate_id,
        as_of_date=as_of_date,
        schedule_config_path=schedule_config_path,
        gate_config_path=gate_config_path,
        operational_policy_config_path=operational_policy_config_path,
        calendar_config_path=calendar_config_path,
        portfolio_config_path=portfolio_config_path,
        risk_limit_config_path=risk_limit_config_path,
        storage_config_path=storage_config_path,
        state_dir=state_dir,
        dsn=dsn,
        python_executable=python_executable,
    )
    if not isinstance(plan, Mapping):
        raise StorageError("readiness-aware planner returned invalid evidence")
    summary = _base_summary(
        plan=plan,
        execute=execute,
        evaluated_at=authorization_time,
    )
    effect = _mapping(plan.get("schedule_effect"), "schedule_effect").get(
        "decision"
    )
    if effect not in {"would_run", "would_block", "no_work"}:
        raise StorageError("readiness-aware plan effect is incompatible")

    if not execute:
        summary["decision"] = effect
        identity = {
            "evaluated_at": summary["evaluated_at"],
            "execution_requested": False,
            "model_version": MODEL_VERSION,
            "plan_id": summary["plan_id"],
            "schedule_effect": effect,
        }
        summary["control_id"] = _control_id(identity)
        return summary
    if effect == "no_work":
        summary["decision"] = "no_work"
        identity = {
            "evaluated_at": summary["evaluated_at"],
            "execution_requested": True,
            "model_version": MODEL_VERSION,
            "plan_id": summary["plan_id"],
            "schedule_effect": "no_work",
        }
        summary["control_id"] = _control_id(identity)
        return summary

    current_decision = _refresh_current_decision(
        dsn=dsn,
        plan=plan,
        reader=selected_decision_reader,
    )
    if current_decision is None:
        summary["decision"] = "block"
        summary["block_reasons"] = ["decision_missing"]
        summary["control_id"] = _control_id(
            {
                "evaluated_at": summary["evaluated_at"],
                "model_version": MODEL_VERSION,
                "plan_id": summary["plan_id"],
                "reasons": summary["block_reasons"],
            }
        )
        return summary
    fresh, freshness_reason = _decision_is_fresh(
        current_decision,
        authorization_time,
    )
    if not fresh:
        summary["decision"] = "block"
        summary["block_reasons"] = [str(freshness_reason)]
        summary["readiness"] = current_decision
        summary["control_id"] = _control_id(
            {
                "decision_id": current_decision["decision_id"],
                "evaluated_at": summary["evaluated_at"],
                "model_version": MODEL_VERSION,
                "plan_id": summary["plan_id"],
                "reasons": summary["block_reasons"],
            }
        )
        return summary

    active_override: dict[str, Any] | None = None
    if current_decision.get("decision") == "block":
        active_override = selected_override_reader(
            dsn=dsn,
            decision_id=str(current_decision["decision_id"]),
            evaluated_at=authorization_time,
        )
        if active_override is None:
            summary["decision"] = "block"
            summary["block_reasons"] = [
                *list(current_decision.get("reasons", [])),
                "override_missing_or_inactive",
            ]
            summary["readiness"] = current_decision
            summary["control_id"] = _control_id(
                {
                    "decision_id": current_decision["decision_id"],
                    "evaluated_at": summary["evaluated_at"],
                    "model_version": MODEL_VERSION,
                    "plan_id": summary["plan_id"],
                    "reasons": summary["block_reasons"],
                }
            )
            return summary
    elif current_decision.get("decision") != "allow":
        raise StorageError("current readiness decision value is incompatible")

    authority_plan = _enriched_plan(plan, current_decision)
    authority = build_operational_readiness_execution_authority(
        plan=authority_plan,
        authorized_at=authorization_time,
        active_override=active_override,
    )
    execution_result = selected_schedule_executor(
        schedule_id=schedule_id,
        as_of_date=as_of_date,
        schedule_config_path=schedule_config_path,
        calendar_config_path=calendar_config_path,
        portfolio_config_path=portfolio_config_path,
        risk_limit_config_path=risk_limit_config_path,
        storage_config_path=storage_config_path,
        state_dir=state_dir,
        dsn=dsn,
        execute=True,
        execution_authority=authority,
        python_executable=python_executable,
        command_runner=command_runner,
    )
    if not isinstance(execution_result, Mapping):
        raise StorageError("local schedule executor returned invalid evidence")
    execution = _mapping(execution_result.get("execution"), "schedule execution")
    expected_sessions = _mapping(plan.get("schedule_effect"), "schedule effect").get(
        "session_dates"
    )
    if (
        execution.get("performed") is not True
        or execution.get("completed_sessions") != expected_sessions
        or execution_result.get("execution_authority") != authority
    ):
        raise StorageError(
            "local schedule plan changed or execution evidence is incomplete"
        )

    summary["decision"] = "executed"
    summary["readiness"] = current_decision
    summary["execution_authority"] = authority
    summary["execution"] = dict(execution)
    summary["checkpoint_after"] = execution.get("checkpoint_after")
    summary["provider_request_performed"] = execution_result.get(
        "provider_request_performed"
    )
    summary["notification_delivery_performed"] = execution_result.get(
        "external_delivery_performed"
    )
    summary["cloud_schedule_activated"] = execution_result.get(
        "cloud_schedule_activated"
    )
    if any(
        summary[key] is not False
        for key in (
            "provider_request_performed",
            "notification_delivery_performed",
            "cloud_schedule_activated",
        )
    ):
        raise StorageError("local schedule execution reported an unauthorized side effect")
    summary["control_id"] = _control_id(
        {
            "authority_id": authority["authority_id"],
            "completed_sessions": execution["completed_sessions"],
            "evaluated_at": summary["evaluated_at"],
            "model_version": MODEL_VERSION,
            "plan_id": summary["plan_id"],
        }
    )
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan or explicitly execute the local portfolio schedule through "
            "current operational readiness and override evidence."
        )
    )
    parser.add_argument("--schedule-id", required=True)
    parser.add_argument("--gate-id", required=True)
    parser.add_argument("--as-of-date", required=True, type=_calendar_date)
    parser.add_argument("--evaluated-at", required=True)
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
        raise StorageError("unable to write readiness-enforced schedule summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_readiness_enforced_local_schedule(
            schedule_id=args.schedule_id,
            gate_id=args.gate_id,
            as_of_date=args.as_of_date,
            evaluated_at=args.evaluated_at,
            schedule_config_path=args.schedule_config,
            gate_config_path=args.gate_config,
            operational_policy_config_path=args.operational_policy_config,
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
    except ValidationError as exc:
        print(f"Readiness-enforced local schedule rejected: {exc}", file=sys.stderr)
        return 1
    except OverlapError:
        print(
            "Readiness-enforced local schedule failed: another schedule run owns the lock",
            file=sys.stderr,
        )
        return 1
    except StorageError as exc:
        print(f"Readiness-enforced local schedule failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Readiness-enforced local schedule failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 2 if summary["decision"] == "block" else 0


if __name__ == "__main__":
    raise SystemExit(main())
