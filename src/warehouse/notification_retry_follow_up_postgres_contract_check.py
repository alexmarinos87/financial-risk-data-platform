from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from src.warehouse.notification_retry_destination_binding_contract import (
    build_retry_destination_binding,
    canonical_binding_bytes,
)
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
DESTINATION_CHECK_PATH = Path(
    "sql/portfolio_risk_notification_retry_destination_follow_up_consistency_checks.sql"
)
DESTINATION_ID = "risk-operations-webhook"
DESTINATION_FINGERPRINT = "portfolio-risk-notification-destination-v1-contract"
AUTHORITY_ID = "portfolio-risk-notification-destination-authority-v1-contract"


def _destination_authority(*, evaluated_at: datetime) -> dict[str, Any]:
    return {
        "authority_id": AUTHORITY_ID,
        "destination_fingerprint": DESTINATION_FINGERPRINT,
        "destination_id": DESTINATION_ID,
        "endpoint_environment_variable": "RISK_NOTIFICATION_WEBHOOK_URL",
        "evaluated_at": evaluated_at.isoformat(),
        "evaluated_event_types": ["breach_opened"],
        "model_version": "portfolio-risk-notification-destination-authority-v1",
        "channel": "webhook",
        "activation": {
            "enabled": True,
            "status": "active",
            "change_request_id": "FOLLOW-UP-DESTINATION-CONTRACT",
            "reviewed_at": (evaluated_at - timedelta(days=1)).isoformat(),
            "review_expires_at": (evaluated_at + timedelta(days=1)).isoformat(),
        },
        "allowed_event_types": [
            "breach_escalated",
            "breach_opened",
            "breach_resolved",
        ],
        "active": True,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "delivery_attempt_written": False,
        "outbox_mutated": False,
        "acknowledgement_mutated": False,
    }


def _insert_destination_binding(
    cursor: Any,
    *,
    record: Mapping[str, Any],
    evaluated_at: datetime,
    jsonb: Any,
) -> dict[str, Any]:
    binding = build_retry_destination_binding(
        record_id=record["record_id"],
        request_id=record["request_id"],
        plan_id=record["plan_id"],
        execution_id=record["execution_id"],
        destination_authority=_destination_authority(evaluated_at=evaluated_at),
        recorded_at=record["recorded_at"],
    )
    authority = binding["destination_authority"]
    digest = hashlib.sha256(canonical_binding_bytes(binding)).hexdigest()
    cursor.execute(
        """
        INSERT INTO
        risk_platform.portfolio_risk_notification_retry_destination_bindings (
            binding_id,
            model_version,
            record_id,
            request_id,
            plan_id,
            execution_id,
            authority_id,
            destination_id,
            destination_fingerprint,
            endpoint_environment_variable,
            evaluated_at,
            evaluated_event_types_json,
            authority_json,
            recorded_at,
            binding_json,
            document_sha256
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            binding["binding_id"],
            binding["model_version"],
            binding["record_id"],
            binding["request_id"],
            binding["plan_id"],
            binding["execution_id"],
            authority["authority_id"],
            authority["destination_id"],
            authority["destination_fingerprint"],
            authority["endpoint_environment_variable"],
            authority["evaluated_at"],
            jsonb(authority["evaluated_event_types"]),
            jsonb(authority),
            binding["recorded_at"],
            jsonb(binding),
            digest,
        ),
    )
    return binding


def _assert_destination_rows(cursor: Any) -> dict[str, str]:
    cursor.execute(
        """
        SELECT
            event_id,
            destination_binding_status,
            COALESCE(destination_id, '')
        FROM risk_platform.current_notification_retry_destination_follow_up
        WHERE event_id LIKE 'follow-up-event-%'
        ORDER BY event_id
        """
    )
    rows = cursor.fetchall()
    actual = {
        str(event_id): f"{status}:{destination_id}"
        for event_id, status, destination_id in rows
    }
    expected = {
        "follow-up-event-acknowledged": "not_applicable:",
        "follow-up-event-delivered": "not_applicable:",
        "follow-up-event-execution": "destination_binding_missing:",
        "follow-up-event-initial": "not_applicable:",
        "follow-up-event-retry": "not_applicable:",
        "follow-up-event-superseded": "destination_binding_missing:",
        "follow-up-event-uncertain": f"bound:{DESTINATION_ID}",
    }
    if actual != expected:
        raise AssertionError(f"destination follow-up classifications changed: {actual!r}")

    cursor.execute(
        """
        SELECT event_id, destination_id
        FROM risk_platform.current_notification_retry_destination_ambiguities
        WHERE event_id LIKE 'follow-up-event-%'
        ORDER BY event_id
        """
    )
    ambiguity_rows = cursor.fetchall()
    if ambiguity_rows != [("follow-up-event-uncertain", DESTINATION_ID)]:
        raise AssertionError(
            f"destination ambiguity selection changed: {ambiguity_rows!r}"
        )

    cursor.execute(
        """
        SELECT event_id
        FROM risk_platform.current_notification_retry_destination_binding_reviews
        WHERE event_id LIKE 'follow-up-event-%'
        ORDER BY event_id
        """
    )
    review_rows = [str(row[0]) for row in cursor.fetchall()]
    if review_rows != [
        "follow-up-event-execution",
        "follow-up-event-superseded",
    ]:
        raise AssertionError(
            f"destination binding review selection changed: {review_rows!r}"
        )

    cursor.execute(
        """
        SELECT record_id, destination_bound, destination_id
        FROM risk_platform.latest_notification_retry_destination_by_event
        WHERE event_id = 'follow-up-event-superseded'
        """
    )
    latest = cursor.fetchone()
    if latest is None or latest[1:] != (False, None):
        raise AssertionError(
            f"superseded destination binding remained current: {latest!r}"
        )
    return {event_id: value for event_id, value in actual.items()}


def run_contract_check(dsn: str) -> dict[str, Any]:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Notification retry follow-up contract requires psycopg") from exc

    connection = psycopg.connect(dsn)
    checks: list[tuple[Any, ...]] = []
    destination_checks: list[tuple[Any, ...]] = []
    classifications: dict[str, str] = {}
    destination_classifications: dict[str, str] = {}
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
            uncertain_record = _failed_record(
                request_id="FOLLOW-UP-UNCERTAIN",
                plan_id="follow-up-plan-uncertain",
                event_id=events["uncertain"]["event_id"],
                started_at=uncertain_start,
                finished_at=uncertain_start + timedelta(seconds=1),
                terminal_status="persistence_uncertain",
                attempt_id=None,
            )
            _insert_execution_record(cursor, record=uncertain_record, jsonb=Jsonb)
            _insert_destination_binding(
                cursor,
                record=uncertain_record,
                evaluated_at=uncertain_start,
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
            execution_record = _failed_record(
                request_id="FOLLOW-UP-EXECUTION",
                plan_id="follow-up-plan-execution",
                event_id=events["execution"]["event_id"],
                started_at=execution_time,
                finished_at=execution_time + timedelta(seconds=1),
                terminal_status="failed_after_request",
                attempt_id=execution_attempt,
            )
            _insert_execution_record(cursor, record=execution_record, jsonb=Jsonb)

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
            old_record = _failed_record(
                request_id="FOLLOW-UP-SUPERSEDED-OLD",
                plan_id="follow-up-plan-superseded-old",
                event_id=events["superseded"]["event_id"],
                started_at=old_start,
                finished_at=old_start + timedelta(seconds=1),
                terminal_status="persistence_uncertain",
                attempt_id=None,
            )
            _insert_execution_record(cursor, record=old_record, jsonb=Jsonb)
            _insert_destination_binding(
                cursor,
                record=old_record,
                evaluated_at=old_start,
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
            current_record = _failed_record(
                request_id="FOLLOW-UP-SUPERSEDED-CURRENT",
                plan_id="follow-up-plan-superseded-current",
                event_id=events["superseded"]["event_id"],
                started_at=current_time,
                finished_at=current_time + timedelta(seconds=1),
                terminal_status="failed_after_request",
                attempt_id=current_attempt,
            )
            _insert_execution_record(cursor, record=current_record, jsonb=Jsonb)

            classifications = _assert_follow_up_rows(cursor)
            destination_classifications = _assert_destination_rows(cursor)
            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
            failures = [check for check in checks if check[3] != "pass"]
            if failures:
                names = ", ".join(str(check[0]) for check in failures)
                raise AssertionError("retry follow-up reconciliation failed: " + names)
            cursor.execute(DESTINATION_CHECK_PATH.read_text(encoding="utf-8"))
            destination_checks = list(cursor.fetchall())
            destination_failures = [
                check for check in destination_checks if check[3] != "pass"
            ]
            if destination_failures:
                names = ", ".join(
                    str(check[0]) for check in destination_failures
                )
                raise AssertionError(
                    "destination retry follow-up reconciliation failed: " + names
                )
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return {
        "model_version": "portfolio-risk-notification-retry-follow-up-v2",
        "fixture_events": 7,
        "classifications": classifications,
        "destination_classifications": destination_classifications,
        "destination_bound_current_rows": 1,
        "destination_binding_review_rows": 2,
        "delivery_failure_rows": 4,
        "ambiguous_outcome_rows": 1,
        "superseded_uncertainty_excluded": True,
        "superseded_destination_binding_excluded": True,
        "consistency_checks": len(checks),
        "destination_consistency_checks": len(destination_checks),
        "external_request_performed": False,
        "delivery_attempt_written_outside_fixture": False,
        "outbox_mutated_outside_fixture": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise current notification retry and destination follow-up views "
            "against PostgreSQL 16 using transaction-scoped fixtures."
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
