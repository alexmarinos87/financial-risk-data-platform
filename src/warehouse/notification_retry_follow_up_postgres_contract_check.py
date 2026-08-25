from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

from src.warehouse.notification_retry_follow_up_contract_check import (
    CHECK_PATH,
    _assert_follow_up_rows,
    _failed_record,
    _insert_acknowledgement,
    _insert_attempt,
    _insert_evaluation_and_event,
    _insert_execution_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

ATTRIBUTION_COLUMNS = (
    "calculation_id",
    "model_version",
    "portfolio_id",
    "base_currency",
    "definition_fingerprint",
    "weighting_method",
    "covariance_method",
    "correlation_method",
    "covariance_window",
    "annualization_days",
    "ts_event",
    "ts_ingest",
)


def run_contract_check(dsn: str) -> dict[str, Any]:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Notification retry follow-up contract requires psycopg") from exc

    connection = psycopg.connect(dsn)
    checks: list[tuple[Any, ...]] = []
    classifications: dict[str, str] = {}
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    calculation_id,
                    model_version,
                    portfolio_id,
                    base_currency,
                    definition_fingerprint,
                    weighting_method,
                    covariance_method,
                    correlation_method,
                    covariance_window,
                    annualization_days,
                    ts_event,
                    ts_ingest
                FROM risk_platform.portfolio_risk_attribution
                ORDER BY ts_event DESC, calculation_id DESC
                LIMIT 1
                """
            )
            row = cursor.fetchone()
            if row is None:
                raise AssertionError("portfolio attribution fixture is unavailable")
            attribution = dict(zip(ATTRIBUTION_COLUMNS, row, strict=True))
            base_time = max(
                attribution["ts_event"],
                attribution["ts_ingest"],
                datetime(2026, 4, 1, tzinfo=timezone.utc),
            ) + timedelta(days=1)

            events: dict[str, dict[str, Any]] = {}
            suffixes = (
                "initial",
                "retry",
                "uncertain",
                "execution",
                "delivered",
                "acknowledged",
                "superseded",
            )
            for index, suffix in enumerate(suffixes):
                events[suffix] = _insert_evaluation_and_event(
                    cursor,
                    attribution=attribution,
                    suffix=suffix,
                    event_time=base_time + timedelta(minutes=index),
                    jsonb=Jsonb,
                )

            _insert_attempt(
                cursor,
                event=events["retry"],
                attempt_number=1,
                attempted_at=events["retry"]["ingest_time"] + timedelta(minutes=1),
                succeeded=False,
            )

            _insert_attempt(
                cursor,
                event=events["uncertain"],
                attempt_number=1,
                attempted_at=events["uncertain"]["ingest_time"]
                + timedelta(minutes=1),
                succeeded=False,
            )
            uncertain_start = events["uncertain"]["ingest_time"] + timedelta(minutes=2)
            _insert_execution_record(
                cursor,
                record=_failed_record(
                    request_id="FOLLOW-UP-UNCERTAIN",
                    plan_id="follow-up-plan-uncertain",
                    event_id=events["uncertain"]["event_id"],
                    started_at=uncertain_start,
                    finished_at=uncertain_start + timedelta(seconds=1),
                    terminal_status="persistence_uncertain",
                    attempt_id=None,
                ),
                jsonb=Jsonb,
            )

            _insert_attempt(
                cursor,
                event=events["execution"],
                attempt_number=1,
                attempted_at=events["execution"]["ingest_time"]
                + timedelta(minutes=1),
                succeeded=False,
            )
            execution_time = events["execution"]["ingest_time"] + timedelta(minutes=2)
            execution_attempt = _insert_attempt(
                cursor,
                event=events["execution"],
                attempt_number=2,
                attempted_at=execution_time,
                succeeded=False,
            )
            _insert_execution_record(
                cursor,
                record=_failed_record(
                    request_id="FOLLOW-UP-EXECUTION",
                    plan_id="follow-up-plan-execution",
                    event_id=events["execution"]["event_id"],
                    started_at=execution_time,
                    finished_at=execution_time + timedelta(seconds=1),
                    terminal_status="failed_after_request",
                    attempt_id=execution_attempt,
                ),
                jsonb=Jsonb,
            )

            _insert_attempt(
                cursor,
                event=events["delivered"],
                attempt_number=1,
                attempted_at=events["delivered"]["ingest_time"]
                + timedelta(minutes=1),
                succeeded=True,
            )
            _insert_acknowledgement(
                cursor,
                event=events["acknowledged"],
                acknowledged_at=events["acknowledged"]["ingest_time"]
                + timedelta(minutes=1),
            )

            _insert_attempt(
                cursor,
                event=events["superseded"],
                attempt_number=1,
                attempted_at=events["superseded"]["ingest_time"]
                + timedelta(minutes=1),
                succeeded=False,
            )
            old_start = events["superseded"]["ingest_time"] + timedelta(minutes=2)
            _insert_execution_record(
                cursor,
                record=_failed_record(
                    request_id="FOLLOW-UP-SUPERSEDED-OLD",
                    plan_id="follow-up-plan-superseded-old",
                    event_id=events["superseded"]["event_id"],
                    started_at=old_start,
                    finished_at=old_start + timedelta(seconds=1),
                    terminal_status="persistence_uncertain",
                    attempt_id=None,
                ),
                jsonb=Jsonb,
            )
            _insert_attempt(
                cursor,
                event=events["superseded"],
                attempt_number=2,
                attempted_at=old_start + timedelta(seconds=2),
                succeeded=False,
            )
            current_time = old_start + timedelta(minutes=1)
            current_attempt = _insert_attempt(
                cursor,
                event=events["superseded"],
                attempt_number=3,
                attempted_at=current_time,
                succeeded=False,
            )
            _insert_execution_record(
                cursor,
                record=_failed_record(
                    request_id="FOLLOW-UP-SUPERSEDED-CURRENT",
                    plan_id="follow-up-plan-superseded-current",
                    event_id=events["superseded"]["event_id"],
                    started_at=current_time,
                    finished_at=current_time + timedelta(seconds=1),
                    terminal_status="failed_after_request",
                    attempt_id=current_attempt,
                ),
                jsonb=Jsonb,
            )

            classifications = _assert_follow_up_rows(cursor)
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
            failures = [row for row in checks if row[3] != "pass"]
            if failures:
                names = ", ".join(str(row[0]) for row in failures)
                raise AssertionError("retry follow-up reconciliation failed: " + names)
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "model_version": "portfolio-risk-notification-retry-follow-up-v1",
        "fixture_events": 7,
        "classifications": classifications,
        "delivery_failure_rows": 4,
        "ambiguous_outcome_rows": 1,
        "superseded_uncertainty_excluded": True,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "delivery_attempt_written_outside_fixture": False,
        "outbox_mutated_outside_fixture": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise current notification retry follow-up views against "
            "PostgreSQL 16 using transaction-scoped fixtures."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_contract_check(args.dsn)
    except Exception as exc:
        print(f"Notification retry follow-up contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
