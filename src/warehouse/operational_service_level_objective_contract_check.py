from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timezone
from typing import Any

from src.analytics.operational_service_level_objectives import (
    evaluate_operational_objectives,
    parse_operational_objective_policy,
)
from src.analytics.operational_service_levels import (
    evaluate_operational_service_levels,
    parse_operational_service_level_policy,
)
from src.common.exceptions import ValidationError
from src.warehouse.operational_service_level_objective_recorder import (
    record_operational_objective_report,
)
from src.warehouse.operational_service_level_recorder import (
    record_operational_service_level_report,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

SESSIONS = (
    date(2026, 3, 27),
    date(2026, 3, 30),
    date(2026, 3, 31),
)


def _operational_policy():
    return parse_operational_service_level_policy(
        {
            "policies": {
                "us-tech-local": {
                    "schedule_id": "us-tech-local",
                    "metrics": {
                        "schedule_lag_sessions": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "market_freshness_exception_count": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "notification_retry_exhausted_count": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "notification_oldest_dead_letter_age_seconds": {
                            "warning": 900,
                            "critical": 3600,
                        },
                    },
                }
            }
        },
        "us-tech-local",
    )


def _objective_policy():
    return parse_operational_objective_policy(
        {
            "objective_policies": {
                "us-tech-three-session": {
                    "operational_policy_id": "us-tech-local",
                    "window_sessions": 3,
                    "minimum_observations": 2,
                    "targets": {
                        "schedule_completion_attainment": {
                            "source_metric_name": "schedule_lag_sessions",
                            "success_threshold": 0,
                            "target_ratio": 1.0,
                        },
                        "market_freshness_attainment": {
                            "source_metric_name": (
                                "market_freshness_exception_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1.0,
                        },
                        "notification_retry_exhaustion_free_attainment": {
                            "source_metric_name": (
                                "notification_retry_exhausted_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1.0,
                        },
                        "notification_dead_letter_duration_attainment": {
                            "source_metric_name": (
                                "notification_oldest_dead_letter_age_seconds"
                            ),
                            "success_threshold": 900,
                            "target_ratio": 1.0,
                        },
                    },
                }
            }
        },
        "us-tech-three-session",
    )


def _source_report(
    *,
    session: date,
    as_of: datetime,
    lag: int,
) -> dict[str, Any]:
    freshness = [
        {
            "source": "alpha_vantage",
            "symbol": symbol,
            "calendar_id": "XNYS",
            "as_of_date": session,
            "freshness_status": "current",
            "trailing_missing_session_count": 0,
        }
        for symbol in ("AAPL", "MSFT")
    ]
    output = evaluate_operational_service_levels(
        policy=_operational_policy(),
        as_of=as_of,
        schedule_fingerprint="local-portfolio-schedule-v1-example",
        latest_expected_session=session,
        schedule_checkpoint=session,
        schedule_lag_sessions=lag,
        expected_constituents=(
            "alpha_vantage:AAPL",
            "alpha_vantage:MSFT",
        ),
        calendar_id="XNYS",
        freshness_records=freshness,
        notification_records=[],
        maximum_notification_attempts=3,
    )
    return {
        **dict(output.report),
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_id": "us-tech-2026",
        "mandate_fingerprint": "portfolio-mandate-v1-example",
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def _record_source(
    dsn: str,
    *,
    session: date,
    as_of: datetime,
    lag: int,
) -> dict[str, Any]:
    report = _source_report(session=session, as_of=as_of, lag=lag)
    recorded = record_operational_service_level_report(
        dsn=dsn,
        report=report,
    )
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
        "as_of": datetime.fromisoformat(report["as_of"]),
        "latest_expected_session": date.fromisoformat(
            report["latest_expected_session"]
        ),
        "metrics_json": report["metrics"],
        "document_sha256": recorded["document_sha256"],
    }


def _objective_report(source_reports: list[dict[str, Any]]) -> dict[str, Any]:
    output = evaluate_operational_objectives(
        objective_policy=_objective_policy(),
        through_session=SESSIONS[-1],
        expected_sessions=SESSIONS,
        operational_policy_fingerprint=_operational_policy().fingerprint,
        schedule_id="us-tech-local",
        schedule_fingerprint="local-portfolio-schedule-v1-example",
        calendar_id="XNYS",
        portfolio_id="us-tech-equal",
        risk_limit_policy_id="us-tech-standard",
        mandate_id="us-tech-2026",
        mandate_fingerprint="portfolio-mandate-v1-example",
        reports=source_reports,
    )
    return dict(output.report)


def run_contract_check(dsn: str) -> dict[str, Any]:
    source_reports = [
        _record_source(
            dsn,
            session=session,
            as_of=datetime(
                2026,
                4,
                index,
                12,
                tzinfo=timezone.utc,
            ),
            lag=0,
        )
        for index, session in enumerate(SESSIONS, start=1)
    ]
    first_report = _objective_report(source_reports)
    first = record_operational_objective_report(
        dsn=dsn,
        report=first_report,
    )
    replay = record_operational_objective_report(
        dsn=dsn,
        report=first_report,
    )
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("operational objective retry did not converge")

    conflicting = dict(first_report)
    conflicting["input_rows_scanned"] = first_report["input_rows_scanned"] + 1
    try:
        record_operational_objective_report(
            dsn=dsn,
            report=conflicting,
        )
    except ValidationError:
        pass
    else:
        raise AssertionError("conflicting objective calculation identity was accepted")

    corrected = _record_source(
        dsn,
        session=SESSIONS[-1],
        as_of=datetime(2026, 4, 4, 12, tzinfo=timezone.utc),
        lag=1,
    )
    second_report = _objective_report([*source_reports[:-1], corrected])
    second = record_operational_objective_report(
        dsn=dsn,
        report=second_report,
    )
    if second["created"] is not True:
        raise AssertionError("corrected operational objective report was not appended")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational objective contract check requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_service_level_objective_reports),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_service_level_objective_metric_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.latest_operational_service_level_objective_reports),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_service_level_objective_status),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_service_level_objective_exceptions),
                    (SELECT overall_status
                     FROM risk_platform.latest_operational_service_level_objective_reports)
                """
            )
            counts = cursor.fetchone()
            if counts != (2, 8, 1, 4, 1, "missed"):
                raise AssertionError(
                    f"operational objective serving views are incompatible: {counts!r}"
                )

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE risk_platform.operational_service_level_objective_reports
                    SET recorded_at = recorded_at
                    WHERE calculation_id = %s
                    """,
                    (first_report["calculation_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational objective update was not blocked")

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM risk_platform.operational_service_level_objective_reports
                    WHERE calculation_id = %s
                    """,
                    (first_report["calculation_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational objective delete was not blocked")

    return {
        "first_calculation_id": first_report["calculation_id"],
        "second_calculation_id": second_report["calculation_id"],
        "report_rows": 2,
        "objective_rows": 8,
        "latest_status": "missed",
        "append_only_verified": True,
        "replay_verified": True,
        "conflict_verified": True,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exercise the operational SLO objective PostgreSQL contract."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main() -> int:
    args = _build_parser().parse_args()
    try:
        result = run_contract_check(args.dsn)
    except Exception as exc:
        print(
            f"Operational SLO objective contract check failed: {exc}",
            file=sys.stderr,
        )
        return 1
    print(
        "Operational SLO objective PostgreSQL contract passed: "
        f"{result['report_rows']} reports, {result['objective_rows']} objectives"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
