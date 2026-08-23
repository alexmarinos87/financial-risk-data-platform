from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from ..analytics.market_calendar import MarketCalendar, load_market_calendar
from ..analytics.operational_service_levels import (
    MAX_FRESHNESS_ROWS,
    MAX_NOTIFICATION_ROWS,
    OperationalServiceLevelPolicy,
    evaluate_operational_service_levels,
    load_operational_service_level_policy,
)
from ..analytics.portfolio_mandates import PortfolioMandate, load_portfolio_mandate
from ..common.exceptions import StorageError, ValidationError
from ..warehouse.postgres_loader import DEFAULT_POSTGRES_DSN
from .deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
    load_webhook_delivery_config,
)
from .run_local_portfolio_schedule import (
    MODEL_VERSION as SCHEDULE_MODEL_VERSION,
)
from .run_local_portfolio_schedule import (
    LocalPortfolioSchedule,
    load_local_portfolio_schedule,
)

MAX_STATE_BYTES = 65_536

ScheduleLoader = Callable[[Path, str], LocalPortfolioSchedule]
CalendarLoader = Callable[[Path, str], MarketCalendar]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
PolicyLoader = Callable[[Path, str], OperationalServiceLevelPolicy]
DeliveryConfigLoader = Callable[[Path], WebhookDeliveryConfig]
EvidenceReader = Callable[..., tuple[list[dict[str, Any]], list[dict[str, Any]]]]
StateReader = Callable[[Path], Mapping[str, Any] | None]


def _aware_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a timezone-aware ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError(
            "must be a timezone-aware ISO-8601 timestamp"
        )
    return parsed.astimezone(timezone.utc)


def _quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("schema_name must be non-empty text")
    return '"' + value.replace('"', '""') + '"'


def read_operational_evidence(
    *,
    dsn: str,
    calendar_id: str,
    policy_id: str,
    portfolio_id: str,
    schema_name: str = "risk_platform",
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "Operational service-level reporting requires psycopg. Run `make setup`."
        ) from exc

    schema = _quote_identifier(schema_name)
    freshness_sql = f"""
        SELECT
            source,
            symbol,
            calendar_id,
            as_of_date,
            freshness_status,
            trailing_missing_session_count
        FROM {schema}.current_daily_market_freshness
        WHERE calendar_id = %s
        ORDER BY source, symbol
        LIMIT %s
    """
    notifications_sql = f"""
        SELECT
            event_id,
            ts_event,
            attempt_count,
            delivered,
            last_attempted_at
        FROM {schema}.portfolio_risk_notification_delivery_status
        WHERE policy_id = %s
          AND portfolio_id = %s
          AND NOT delivered
        ORDER BY ts_event, event_id
        LIMIT %s
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    freshness_sql,
                    [calendar_id, MAX_FRESHNESS_ROWS + 1],
                )
                freshness = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    notifications_sql,
                    [policy_id, portfolio_id, MAX_NOTIFICATION_ROWS + 1],
                )
                notifications = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read operational service-level evidence from PostgreSQL"
        ) from None
    if len(freshness) > MAX_FRESHNESS_ROWS:
        raise ValidationError("market freshness evidence exceeds the row limit")
    if len(notifications) > MAX_NOTIFICATION_ROWS:
        raise ValidationError("notification delivery evidence exceeds the row limit")
    return freshness, notifications


def _read_schedule_state(path: Path) -> Mapping[str, Any] | None:
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
    if not isinstance(payload, Mapping):
        raise StorageError("local schedule state must be a JSON object")
    return payload


def _schedule_checkpoint(
    state: Mapping[str, Any] | None,
    *,
    schedule: LocalPortfolioSchedule,
) -> date | None:
    if state is None:
        return None
    if state.get("model_version") != SCHEDULE_MODEL_VERSION:
        raise ValidationError("local schedule state model version is unsupported")
    if state.get("schedule_id") != schedule.schedule_id:
        raise ValidationError("local schedule state belongs to another schedule")
    if state.get("schedule_fingerprint") != schedule.fingerprint:
        raise ValidationError(
            "local schedule configuration changed; archive or reset its state"
        )
    raw = state.get("last_successful_session")
    if not isinstance(raw, str):
        raise ValidationError("local schedule state is missing its checkpoint")
    try:
        parsed = date.fromisoformat(raw)
    except ValueError:
        raise ValidationError("local schedule checkpoint must use YYYY-MM-DD") from None
    if raw != parsed.isoformat():
        raise ValidationError("local schedule checkpoint must use YYYY-MM-DD")
    return parsed


def _schedule_lag(
    *,
    calendar: MarketCalendar,
    checkpoint: date | None,
    latest_expected_session: date,
) -> int | None:
    if checkpoint is None:
        return None
    if checkpoint > latest_expected_session:
        raise ValidationError(
            "local schedule checkpoint is after the latest expected session"
        )
    if checkpoint == latest_expected_session:
        return 0
    return len(
        calendar.sessions_between(
            checkpoint + timedelta(days=1),
            latest_expected_session,
        )
    )


def run_operational_service_levels(
    *,
    policy_id: str,
    as_of: datetime,
    policy_config_path: Path,
    schedule_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    delivery_config_path: Path,
    state_dir: Path,
    dsn: str,
    schema_name: str = "risk_platform",
    policy_loader: PolicyLoader | None = None,
    schedule_loader: ScheduleLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
    mandate_loader: MandateLoader | None = None,
    delivery_config_loader: DeliveryConfigLoader | None = None,
    state_reader: StateReader | None = None,
    evidence_reader: EvidenceReader | None = None,
) -> dict[str, Any]:
    if (
        not isinstance(as_of, datetime)
        or as_of.tzinfo is None
        or as_of.utcoffset() is None
    ):
        raise ValidationError("as_of must be a timezone-aware timestamp")
    as_of = as_of.astimezone(timezone.utc)
    selected_policy_loader = policy_loader or load_operational_service_level_policy
    policy = selected_policy_loader(policy_config_path, policy_id)
    selected_schedule_loader = schedule_loader or load_local_portfolio_schedule
    schedule = selected_schedule_loader(schedule_config_path, policy.schedule_id)
    if schedule.schedule_id != policy.schedule_id:
        raise ValidationError("service-level policy and schedule do not align")
    selected_calendar_loader = calendar_loader or load_market_calendar
    calendar = selected_calendar_loader(calendar_config_path, schedule.calendar_id)
    as_of_date = as_of.date()
    latest_expected_session = calendar.latest_expected_session(as_of_date)
    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    mandate = selected_mandate_loader(
        portfolio_config_path,
        schedule.portfolio_id,
        latest_expected_session,
    )
    selected_state_reader = state_reader or _read_schedule_state
    state_path = state_dir / f"{schedule.schedule_id}.json"
    state = selected_state_reader(state_path)
    checkpoint = _schedule_checkpoint(state, schedule=schedule)
    lag = _schedule_lag(
        calendar=calendar,
        checkpoint=checkpoint,
        latest_expected_session=latest_expected_session,
    )
    selected_delivery_loader = (
        delivery_config_loader or load_webhook_delivery_config
    )
    delivery = selected_delivery_loader(delivery_config_path)
    selected_evidence_reader = evidence_reader or read_operational_evidence
    freshness_records, notification_records = selected_evidence_reader(
        dsn=dsn,
        calendar_id=calendar.calendar_id,
        policy_id=schedule.policy_id,
        portfolio_id=schedule.portfolio_id,
        schema_name=schema_name,
    )
    expected_constituents = tuple(
        constituent.key for constituent in mandate.constituents
    )
    output = evaluate_operational_service_levels(
        policy=policy,
        as_of=as_of,
        schedule_fingerprint=schedule.fingerprint,
        latest_expected_session=latest_expected_session,
        schedule_checkpoint=checkpoint,
        schedule_lag_sessions=lag,
        expected_constituents=expected_constituents,
        calendar_id=calendar.calendar_id,
        freshness_records=freshness_records,
        notification_records=notification_records,
        maximum_notification_attempts=delivery.max_attempts_per_event,
    )
    return {
        **dict(output.report),
        "portfolio_id": schedule.portfolio_id,
        "risk_limit_policy_id": schedule.policy_id,
        "mandate_id": mandate.mandate_id,
        "mandate_fingerprint": mandate.fingerprint,
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
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
        raise StorageError(
            "Unable to write the operational service-level summary"
        ) from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a deterministic local operational service-level report."
    )
    parser.add_argument("--policy-id", required=True)
    parser.add_argument("--as-of", required=True, type=_aware_timestamp)
    parser.add_argument(
        "--policy-config",
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
    parser.add_argument(
        "--delivery-config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--state-dir",
        type=Path,
        default=Path(".schedule"),
    )
    parser.add_argument(
        "--dsn",
        default=DEFAULT_POSTGRES_DSN,
    )
    parser.add_argument("--schema", default="risk_platform")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_operational_service_levels(
            policy_id=args.policy_id,
            as_of=args.as_of,
            policy_config_path=args.policy_config,
            schedule_config_path=args.schedule_config,
            calendar_config_path=args.calendar_config,
            portfolio_config_path=args.portfolio_config,
            delivery_config_path=args.delivery_config,
            state_dir=args.state_dir,
            dsn=args.dsn,
            schema_name=args.schema,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Operational service-level reporting failed: configuration or evidence "
            "was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational service-level reporting failed: local or PostgreSQL "
            "evidence could not be read",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational service-level reporting failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
