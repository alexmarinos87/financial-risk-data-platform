from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from src.warehouse.notification_retry_follow_up_contract_check import (
    _failed_record,
    _insert_attempt,
    _insert_execution_record,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

CHECK_PATH = Path("sql/notification_retry_readiness_follow_up_consistency_checks.sql")
BOUND_REQUEST_ID = "BINDING-HISTORY-TERMINAL-001"
EVENT_ID = f"{BOUND_REQUEST_ID}-event"
SUPERSEDING_REQUEST_ID = "READINESS-FOLLOW-UP-SUPERSEDING"
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


def _insert_source_event(
    cursor: Any,
    *,
    attribution: Mapping[str, Any],
    event_time: datetime,
    jsonb: Any,
) -> dict[str, Any]:
    evaluation_id = "readiness-follow-up-evaluation"
    policy_id = "readiness-follow-up-policy"
    policy_fingerprint = "readiness-follow-up-policy-fingerprint"
    ingest_time = event_time + timedelta(seconds=1)
    cursor.execute(
        """
        INSERT INTO risk_platform.portfolio_risk_limit_evaluations (
            calculation_id,
            model_version,
            policy_id,
            policy_fingerprint,
            portfolio_id,
            base_currency,
            definition_fingerprint,
            attribution_calculation_id,
            attribution_model_version,
            weighting_method,
            covariance_method,
            correlation_method,
            covariance_window,
            annualization_days,
            ts_event,
            ts_ingest,
            metric_name,
            subject_type,
            subject_key,
            unit,
            observed_value,
            observed_signed_value,
            warning_threshold,
            critical_threshold,
            status,
            is_breach,
            breach_threshold,
            breach_excess
        )
        VALUES (
            %s, 'portfolio-risk-limits-v1', %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            'portfolio_volatility_annualized', 'portfolio', %s,
            'annualized_decimal', 0.50, 0.50, 0.30, 0.45,
            'critical', TRUE, 0.45, 0.05
        )
        """,
        (
            evaluation_id,
            policy_id,
            policy_fingerprint,
            attribution["portfolio_id"],
            attribution["base_currency"],
            attribution["definition_fingerprint"],
            attribution["calculation_id"],
            attribution["model_version"],
            attribution["weighting_method"],
            attribution["covariance_method"],
            attribution["correlation_method"],
            attribution["covariance_window"],
            attribution["annualization_days"],
            event_time,
            ingest_time,
            attribution["portfolio_id"],
        ),
    )
    payload = {
        "event_id": EVENT_ID,
        "event_type": "breach_opened",
        "transition_type": "opened",
        "policy": {
            "policy_id": policy_id,
            "policy_fingerprint": policy_fingerprint,
        },
        "portfolio": {
            "portfolio_id": attribution["portfolio_id"],
            "definition_fingerprint": attribution["definition_fingerprint"],
            "base_currency": attribution["base_currency"],
        },
        "source": {"evaluation_calculation_id": evaluation_id},
    }
    cursor.execute(
        """
        INSERT INTO risk_platform.portfolio_risk_notification_outbox (
            event_id,
            model_version,
            event_type,
            transition_type,
            delivery_disposition,
            suppression_reason,
            source_evaluation_calculation_id,
            source_previous_evaluation_calculation_id,
            risk_limit_model_version,
            policy_id,
            policy_fingerprint,
            portfolio_id,
            base_currency,
            definition_fingerprint,
            attribution_model_version,
            weighting_method,
            covariance_method,
            correlation_method,
            covariance_window,
            annualization_days,
            ts_event,
            ts_ingest,
            metric_name,
            subject_type,
            subject_key,
            previous_subject_key,
            subject_changed,
            unit,
            previous_status,
            current_status,
            severity_rank,
            observed_value,
            observed_signed_value,
            warning_threshold,
            critical_threshold,
            breach_excess,
            payload_json
        )
        VALUES (
            %s, 'portfolio-risk-notification-outbox-v1',
            'breach_opened', 'opened', 'pending', NULL,
            %s, NULL, 'portfolio-risk-limits-v1', %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            'portfolio_volatility_annualized', 'portfolio', %s,
            NULL, FALSE, 'annualized_decimal', NULL, 'critical', 2,
            0.50, 0.50, 0.30, 0.45, 0.05, %s
        )
        """,
        (
            EVENT_ID,
            evaluation_id,
            policy_id,
            policy_fingerprint,
            attribution["portfolio_id"],
            attribution["base_currency"],
            attribution["definition_fingerprint"],
            attribution["model_version"],
            attribution["weighting_method"],
            attribution["covariance_method"],
            attribution["correlation_method"],
            attribution["covariance_window"],
            attribution["annualization_days"],
            event_time,
            ingest_time,
            attribution["portfolio_id"],
            jsonb(payload),
        ),
    )
    return {
        "evaluation_id": evaluation_id,
        "event_id": EVENT_ID,
        "payload": payload,
        "event_time": event_time,
        "ingest_time": ingest_time,
    }


def _current_row(cursor: Any) -> tuple[Any, ...]:
    cursor.execute(
        """
        SELECT
            latest_execution_request_id,
            latest_execution_record_id,
            readiness_binding_status,
            readiness_bound,
            readiness_review_required,
            readiness_binding_id,
            readiness_record_id,
            retained_decision_id,
            refreshed_decision_id,
            enforcement_id,
            readiness_destination_id
        FROM risk_platform.current_notification_retry_readiness_follow_up
        WHERE event_id = %s
        """,
        (EVENT_ID,),
    )
    row = cursor.fetchone()
    if row is None:
        raise AssertionError("readiness-aware current retry row is missing")
    return tuple(row)


def _assert_bound_row(row: tuple[Any, ...]) -> str:
    (
        request_id,
        record_id,
        status,
        readiness_bound,
        review_required,
        binding_id,
        readiness_record_id,
        retained_decision_id,
        refreshed_decision_id,
        enforcement_id,
        destination_id,
    ) = row
    if request_id != BOUND_REQUEST_ID:
        raise AssertionError("the retained bound terminal was not initially current")
    if status != "bound" or readiness_bound is not True or review_required is not False:
        raise AssertionError(f"bound readiness status changed: {row!r}")
    for label, value in (
        ("record_id", record_id),
        ("binding_id", binding_id),
        ("readiness_record_id", readiness_record_id),
        ("retained_decision_id", retained_decision_id),
        ("refreshed_decision_id", refreshed_decision_id),
        ("enforcement_id", enforcement_id),
        ("destination_id", destination_id),
    ):
        if not isinstance(value, str) or not value:
            raise AssertionError(f"bound readiness {label} is unavailable")
    return str(binding_id)


def _assert_superseded_row(row: tuple[Any, ...], *, record_id: str) -> None:
    if row[:5] != (
        SUPERSEDING_REQUEST_ID,
        record_id,
        "readiness_binding_missing",
        False,
        True,
    ):
        raise AssertionError(f"superseding unbound terminal was not current: {row!r}")
    if any(value is not None for value in row[5:]):
        raise AssertionError("superseded readiness evidence leaked into the current row")


def run_contract_check(dsn: str) -> dict[str, Any]:
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Readiness-aware retry views require psycopg") from exc

    connection = psycopg.connect(dsn)
    checks: list[tuple[Any, ...]] = []
    initial_binding_id = ""
    superseding_record: dict[str, Any] | None = None
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
            attribution_row = cursor.fetchone()
            if attribution_row is None:
                raise AssertionError("portfolio attribution fixture is unavailable")
            attribution = dict(
                zip(ATTRIBUTION_COLUMNS, attribution_row, strict=True)
            )
            event = _insert_source_event(
                cursor,
                attribution=attribution,
                event_time=datetime(2026, 8, 1, 11, 58, tzinfo=timezone.utc),
                jsonb=Jsonb,
            )

            initial_binding_id = _assert_bound_row(_current_row(cursor))

            superseding_start = event["ingest_time"] + timedelta(minutes=10)
            attempt_id = _insert_attempt(
                cursor,
                event=event,
                attempt_number=1,
                attempted_at=superseding_start,
                succeeded=False,
            )
            superseding_record = _failed_record(
                request_id=SUPERSEDING_REQUEST_ID,
                plan_id="readiness-follow-up-superseding-plan",
                event_id=EVENT_ID,
                started_at=superseding_start,
                finished_at=superseding_start + timedelta(seconds=1),
                terminal_status="failed_after_request",
                attempt_id=attempt_id,
            )
            _insert_execution_record(
                cursor,
                record=superseding_record,
                jsonb=Jsonb,
            )
            _assert_superseded_row(
                _current_row(cursor),
                record_id=superseding_record["record_id"],
            )

            cursor.execute(
                """
                SELECT request_id, readiness_bound
                FROM risk_platform.notification_retry_readiness_execution_history
                WHERE event_id = %s
                ORDER BY finished_at, record_id
                """,
                (EVENT_ID,),
            )
            history = [(str(row[0]), bool(row[1])) for row in cursor.fetchall()]
            if history != [
                (BOUND_REQUEST_ID, True),
                (SUPERSEDING_REQUEST_ID, False),
            ]:
                raise AssertionError(f"readiness history selection changed: {history!r}")

            cursor.execute(
                """
                SELECT event_id
                FROM risk_platform.current_notification_retry_readiness_binding_reviews
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )
            if cursor.fetchall() != [(EVENT_ID,)]:
                raise AssertionError("missing readiness binding was not queued for review")

            cursor.execute(
                """
                SELECT event_id
                FROM risk_platform.current_notification_retry_readiness_failures
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )
            if cursor.fetchall() != [(EVENT_ID,)]:
                raise AssertionError("readiness-aware failure membership changed")

            cursor.execute(
                """
                SELECT event_id
                FROM risk_platform.current_notification_retry_readiness_ambiguities
                WHERE event_id = %s
                """,
                (EVENT_ID,),
            )
            if cursor.fetchall():
                raise AssertionError("non-ambiguous retry entered the ambiguity queue")

            cursor.execute(CHECK_PATH.read_text(encoding="utf-8"))
            checks = list(cursor.fetchall())
            failures = [check for check in checks if check[3] != "pass"]
            if failures:
                names = ", ".join(str(check[0]) for check in failures)
                raise AssertionError(
                    "readiness-aware retry reconciliation failed: " + names
                )
        connection.rollback()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    if superseding_record is None:
        raise AssertionError("superseding terminal fixture was not created")
    return {
        "model_version": "portfolio-risk-notification-retry-readiness-follow-up-v1",
        "event_id": EVENT_ID,
        "initial_readiness_binding_id": initial_binding_id,
        "current_terminal_record_id": superseding_record["record_id"],
        "initial_bound_status_proved": True,
        "current_binding_missing_status_proved": True,
        "superseded_readiness_binding_excluded": True,
        "failure_partition_preserved": True,
        "ambiguity_partition_preserved": True,
        "consistency_checks": len(checks),
        "external_request_performed": False,
        "delivery_attempt_written_outside_fixture": False,
        "outbox_mutated_outside_fixture": False,
        "acknowledgement_mutated": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Exercise readiness-aware notification retry operational views "
            "against PostgreSQL 16 with transaction-scoped evidence."
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
        print(f"Readiness-aware retry view contract failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
