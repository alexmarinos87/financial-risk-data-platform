from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

from ..analytics.market_calendar import MarketCalendar, load_market_calendar
from ..analytics.operational_service_level_objectives import (
    MAX_REPORT_ROWS,
    OperationalObjectivePolicy,
    evaluate_operational_objectives,
    load_operational_objective_policy,
)
from ..analytics.operational_service_levels import (
    OperationalServiceLevelPolicy,
    load_operational_service_level_policy,
)
from ..analytics.portfolio_mandates import (
    PortfolioMandate,
    load_portfolio_mandate,
)
from ..common.exceptions import StorageError, ValidationError
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN
from .run_local_portfolio_schedule import (
    LocalPortfolioSchedule,
    load_local_portfolio_schedule,
)

ObjectivePolicyLoader = Callable[[Path, str], OperationalObjectivePolicy]
OperationalPolicyLoader = Callable[[Path, str], OperationalServiceLevelPolicy]
ScheduleLoader = Callable[[Path, str], LocalPortfolioSchedule]
CalendarLoader = Callable[[Path, str], MarketCalendar]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
ReportReader = Callable[..., list[dict[str, Any]]]


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD") from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD")
    return parsed


def _quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("schema_name must be non-empty text")
    return '"' + value.replace('"', '""') + '"'


def read_operational_report_history(
    *,
    dsn: str,
    operational_policy_id: str,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
    window_start_session: date,
    through_session: date,
    schema_name: str = "risk_platform",
) -> list[dict[str, Any]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    schema = _quote_identifier(schema_name)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "Operational objective reporting requires psycopg. Run `make setup`."
        ) from exc

    query = f"""
        SELECT
            calculation_id,
            policy_id,
            policy_fingerprint,
            schedule_id,
            schedule_fingerprint,
            calendar_id,
            portfolio_id,
            risk_limit_policy_id,
            mandate_fingerprint,
            as_of,
            latest_expected_session,
            metrics_json,
            document_sha256
        FROM {schema}.operational_service_level_reports
        WHERE policy_id = %s
          AND policy_fingerprint = %s
          AND schedule_id = %s
          AND schedule_fingerprint = %s
          AND calendar_id = %s
          AND portfolio_id = %s
          AND risk_limit_policy_id = %s
          AND mandate_fingerprint = %s
          AND latest_expected_session >= %s
          AND latest_expected_session <= %s
        ORDER BY latest_expected_session, as_of, calculation_id
        LIMIT %s
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        operational_policy_id,
                        operational_policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_fingerprint,
                        window_start_session,
                        through_session,
                        MAX_REPORT_ROWS + 1,
                    ),
                )
                rows = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read retained operational service-level reports"
        ) from None
    if len(rows) > MAX_REPORT_ROWS:
        raise ValidationError("operational objective input exceeds the row limit")
    return rows


def run_operational_service_level_objectives(
    *,
    objective_policy_id: str,
    through_session: date,
    objective_config_path: Path,
    operational_policy_config_path: Path,
    schedule_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    dsn: str,
    schema_name: str = "risk_platform",
    objective_policy_loader: ObjectivePolicyLoader | None = None,
    operational_policy_loader: OperationalPolicyLoader | None = None,
    schedule_loader: ScheduleLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
    mandate_loader: MandateLoader | None = None,
    report_reader: ReportReader | None = None,
) -> dict[str, Any]:
    selected_objective_loader = (
        objective_policy_loader or load_operational_objective_policy
    )
    objective_policy = selected_objective_loader(
        objective_config_path,
        objective_policy_id,
    )
    selected_operational_loader = (
        operational_policy_loader or load_operational_service_level_policy
    )
    operational_policy = selected_operational_loader(
        operational_policy_config_path,
        objective_policy.operational_policy_id,
    )
    selected_schedule_loader = schedule_loader or load_local_portfolio_schedule
    schedule = selected_schedule_loader(
        schedule_config_path,
        operational_policy.schedule_id,
    )
    if schedule.schedule_id != operational_policy.schedule_id:
        raise ValidationError("operational policy and schedule do not align")
    selected_calendar_loader = calendar_loader or load_market_calendar
    calendar = selected_calendar_loader(
        calendar_config_path,
        schedule.calendar_id,
    )
    if calendar.latest_expected_session(through_session) != through_session:
        raise ValidationError("through_session must be an expected market session")
    available_sessions = calendar.sessions_between(
        calendar.valid_from,
        through_session,
    )
    expected_sessions = available_sessions[-objective_policy.window_sessions :]
    if not expected_sessions:
        raise ValidationError(
            "calendar coverage contains no objective-window sessions"
        )
    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    mandate = selected_mandate_loader(
        portfolio_config_path,
        schedule.portfolio_id,
        through_session,
    )
    selected_report_reader = report_reader or read_operational_report_history
    reports = selected_report_reader(
        dsn=dsn,
        operational_policy_id=operational_policy.policy_id,
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        window_start_session=expected_sessions[0],
        through_session=through_session,
        schema_name=schema_name,
    )
    output = evaluate_operational_objectives(
        objective_policy=objective_policy,
        through_session=through_session,
        expected_sessions=expected_sessions,
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_id=mandate.mandate_id,
        mandate_fingerprint=mandate.fingerprint,
        reports=reports,
    )
    return dict(output.report)


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
        raise StorageError(
            "Unable to write the operational objective summary"
        ) from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build deterministic rolling operational service-level "
            "objective attainment evidence."
        )
    )
    parser.add_argument("--objective-policy-id", required=True)
    parser.add_argument("--through-session", required=True, type=_calendar_date)
    parser.add_argument(
        "--objective-config",
        type=Path,
        default=Path("config/operational_service_level_objectives.yaml"),
    )
    parser.add_argument(
        "--operational-policy-config",
        type=Path,
        default=Path("config/operational_service_levels.yaml"),
    )
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
    parser.add_argument("--dsn", default=DEFAULT_POSTGRES_DSN)
    parser.add_argument("--schema", default="risk_platform")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_operational_service_level_objectives(
            objective_policy_id=args.objective_policy_id,
            through_session=args.through_session,
            objective_config_path=args.objective_config,
            operational_policy_config_path=args.operational_policy_config,
            schedule_config_path=args.schedule_config,
            calendar_config_path=args.calendar_config,
            portfolio_config_path=args.portfolio_config,
            dsn=args.dsn,
            schema_name=args.schema,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Operational objective reporting failed: configuration or retained "
            "evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational objective reporting failed: PostgreSQL or local output "
            "could not be accessed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational objective reporting failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
