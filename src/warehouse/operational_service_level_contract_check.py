from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.analytics.operational_service_levels import (
    evaluate_operational_service_levels,
    parse_operational_service_level_policy,
)
from src.common.exceptions import ValidationError
from src.warehouse.operational_readiness_decision_contract_check import (
    run_contract_check as run_readiness_contract_check,
)
from src.warehouse.operational_service_level_objective_contract_check import (
    run_contract_check as run_objective_contract_check,
)
from src.warehouse.operational_service_level_recorder import (
    record_operational_service_level_report,
)
from src.warehouse.postgres_consistency import run_consistency_checks
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN


def _policy():
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


def _freshness(
    *,
    latest_session: date,
    stale_symbol: str | None = None,
) -> list[dict[str, Any]]:
    return [
        {
            "source": "alpha_vantage",
            "symbol": symbol,
            "calendar_id": "XNYS",
            "as_of_date": latest_session,
            "freshness_status": (
                "stale" if symbol == stale_symbol else "current"
            ),
            "trailing_missing_session_count": (
                1 if symbol == stale_symbol else 0
            ),
        }
        for symbol in ("AAPL", "MSFT")
    ]


def _report(
    *,
    as_of: datetime,
    checkpoint: date,
    lag: int,
    stale_symbol: str | None = None,
    notification_records: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    latest_session = date(2026, 3, 31)
    output = evaluate_operational_service_levels(
        policy=_policy(),
        as_of=as_of,
        schedule_fingerprint="local-portfolio-schedule-v1-example",
        latest_expected_session=latest_session,
        schedule_checkpoint=checkpoint,
        schedule_lag_sessions=lag,
        expected_constituents=(
            "alpha_vantage:AAPL",
            "alpha_vantage:MSFT",
        ),
        calendar_id="XNYS",
        freshness_records=_freshness(
            latest_session=latest_session,
            stale_symbol=stale_symbol,
        ),
        notification_records=notification_records or [],
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


def run_contract_check(dsn: str) -> dict[str, Any]:
    first_report = _report(
        as_of=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        checkpoint=date(2026, 3, 30),
        lag=1,
    )
    first = record_operational_service_level_report(
        dsn=dsn,
        report=first_report,
    )
    replay = record_operational_service_level_report(
        dsn=dsn,
        report=first_report,
    )
    if first["created"] is not True or replay["created"] is not False:
        raise AssertionError("operational report retry did not converge")

    conflicting = dict(first_report)
    conflicting["mandate_fingerprint"] = "portfolio-mandate-v1-conflict"
    try:
        record_operational_service_level_report(
            dsn=dsn,
            report=conflicting,
        )
    except ValidationError:
        pass
    else:
        raise AssertionError("conflicting calculation identity was accepted")

    second_as_of = datetime(2026, 4, 1, 13, tzinfo=timezone.utc)
    second_report = _report(
        as_of=second_as_of,
        checkpoint=date(2026, 3, 31),
        lag=0,
        stale_symbol="MSFT",
        notification_records=[
            {
                "event_id": "notification-dead-letter-1",
                "ts_event": second_as_of - timedelta(hours=3),
                "attempt_count": 3,
                "delivered": False,
                "last_attempted_at": second_as_of - timedelta(hours=2),
            }
        ],
    )
    second = record_operational_service_level_report(
        dsn=dsn,
        report=second_report,
    )
    if second["created"] is not True:
        raise AssertionError("second operational report was not appended")

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational contract check requires psycopg") from exc

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_service_level_reports),
                    (SELECT COUNT(*)
                     FROM risk_platform.operational_service_level_metric_history),
                    (SELECT COUNT(*)
                     FROM risk_platform.latest_operational_service_level_reports),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_service_level_metric_status),
                    (SELECT COUNT(*)
                     FROM risk_platform.current_operational_service_level_exceptions),
                    (SELECT overall_status
                     FROM risk_platform.latest_operational_service_level_reports)
                """
            )
            counts = cursor.fetchone()
            if counts != (2, 8, 1, 4, 3, "critical"):
                raise AssertionError(
                    f"operational report serving views are incompatible: {counts!r}"
                )

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE risk_platform.operational_service_level_reports
                    SET recorded_at = recorded_at
                    WHERE calculation_id = %s
                    """,
                    (first_report["calculation_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational report update was not blocked")

    with psycopg.connect(dsn) as connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    DELETE FROM risk_platform.operational_service_level_reports
                    WHERE calculation_id = %s
                    """,
                    (first_report["calculation_id"],),
                )
        except psycopg.Error:
            connection.rollback()
        else:
            raise AssertionError("operational report delete was not blocked")

    objective_result = run_objective_contract_check(dsn)
    objective_consistency = run_consistency_checks(
        dsn=dsn,
        check_paths=(
            Path(
                "sql/operational_service_level_objectives_consistency_checks.sql"
            ),
        ),
    )
    objective_failures = [
        result for result in objective_consistency if result.status != "pass"
    ]
    if objective_failures:
        names = ", ".join(result.check_name for result in objective_failures)
        raise AssertionError(
            "operational objective reconciliation failed: " + names
        )

    readiness_result = run_readiness_contract_check(
        dsn=dsn,
        first_report=first_report,
        first_document_sha256=str(first["document_sha256"]),
        second_report=second_report,
        second_document_sha256=str(second["document_sha256"]),
    )

    return {
        "first_calculation_id": first_report["calculation_id"],
        "second_calculation_id": second_report["calculation_id"],
        "report_rows": 2,
        "metric_rows": 8,
        "latest_status": "critical",
        "append_only_verified": True,
        "replay_verified": True,
        "conflict_verified": True,
        "objective_contract": objective_result,
        "objective_consistency_checks": len(objective_consistency),
        "readiness_contract": readiness_result,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Exercise the operational service-level PostgreSQL contract."
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
            f"Operational service-level contract check failed: {exc}",
            file=sys.stderr,
        )
        return 1
    readiness = result["readiness_contract"]
    print(
        "Operational service-level PostgreSQL contract passed: "
        f"{result['report_rows']} reports, {result['metric_rows']} metrics, "
        f"{result['objective_consistency_checks']} objective checks, "
        f"{readiness['consistency_checks']} readiness checks"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
