from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.warehouse.notification_retry_execution_contract import (
    build_retry_execution_record,
    canonical_retry_execution_record_bytes,
)

CHECK_PATH = Path(
    "sql/portfolio_risk_notification_retry_follow_up_consistency_checks.sql"
)
LOCK_MODEL_VERSION = "portfolio-risk-notification-delivery-lock-v1"
LOCK_KEY_FINGERPRINT = "follow-up-contract-lock-key"


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value),
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _insert_evaluation_and_event(
    cursor: Any,
    *,
    attribution: Mapping[str, Any],
    suffix: str,
    event_time: datetime,
    jsonb: Any,
) -> dict[str, Any]:
    evaluation_id = f"follow-up-evaluation-{suffix}"
    event_id = f"follow-up-event-{suffix}"
    policy_id = f"follow-up-contract-{suffix}"
    policy_fingerprint = f"follow-up-policy-{suffix}"
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
        "event_id": event_id,
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
            event_id,
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
        "event_id": event_id,
        "payload": payload,
        "event_time": event_time,
        "ingest_time": ingest_time,
    }


def _insert_attempt(
    cursor: Any,
    *,
    event: Mapping[str, Any],
    attempt_number: int,
    attempted_at: datetime,
    succeeded: bool,
) -> str:
    attempt_id = f"follow-up-attempt-{event['event_id']}-{attempt_number}"
    http_status = 204 if succeeded else 503
    error_code = None if succeeded else "http_503"
    cursor.execute(
        """
        INSERT INTO risk_platform.portfolio_risk_notification_delivery_attempts (
            attempt_id,
            model_version,
            event_id,
            channel,
            attempt_number,
            idempotency_key,
            attempted_at,
            outcome,
            http_status,
            error_code,
            endpoint_host,
            payload_sha256
        )
        VALUES (
            %s, 'portfolio-risk-webhook-delivery-v1', %s, 'webhook',
            %s, %s, %s, %s, %s, %s, 'alerts.example.test', %s
        )
        """,
        (
            attempt_id,
            event["event_id"],
            attempt_number,
            event["event_id"],
            attempted_at,
            "succeeded" if succeeded else "failed",
            http_status,
            error_code,
            _canonical_sha256(event["payload"]),
        ),
    )
    return attempt_id


def _insert_acknowledgement(
    cursor: Any,
    *,
    event: Mapping[str, Any],
    acknowledged_at: datetime,
) -> None:
    cursor.execute(
        """
        INSERT INTO risk_platform.portfolio_risk_limit_acknowledgements (
            acknowledgement_id,
            model_version,
            evaluation_calculation_id,
            request_id,
            acknowledged_at,
            acknowledged_by,
            disposition,
            reason
        )
        VALUES (
            %s, 'portfolio-risk-limit-ack-v1', %s, %s, %s,
            'risk-operations', 'investigating',
            'PostgreSQL follow-up view contract fixture'
        )
        """,
        (
            f"follow-up-ack-{event['event_id']}",
            event["evaluation_id"],
            f"FOLLOW-UP-ACK-{event['event_id']}",
            acknowledged_at,
        ),
    )


def _insert_execution_record(cursor: Any, *, record: Mapping[str, Any], jsonb: Any) -> None:
    digest = hashlib.sha256(canonical_retry_execution_record_bytes(record)).hexdigest()
    cursor.execute(
        """
        INSERT INTO risk_platform.portfolio_risk_notification_retry_executions (
            record_id,
            model_version,
            request_id,
            execution_id,
            plan_id,
            terminal_status,
            failure_code,
            channel,
            endpoint_host,
            started_at,
            finished_at,
            recorded_at,
            request_count,
            attempts_persisted,
            succeeded_count,
            failed_count,
            attempt_ids_json,
            requested_event_ids_json,
            persisted_event_ids_json,
            delivery_fingerprint,
            retry_policy_fingerprint,
            retry_execution_policy_fingerprint,
            lock_model_version,
            lock_key_fingerprint,
            lock_acquired,
            lock_released,
            execution_summary_json,
            record_json,
            document_sha256
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            record["record_id"],
            record["model_version"],
            record["request_id"],
            record["execution_id"],
            record["plan_id"],
            record["terminal_status"],
            record["failure_code"],
            record["channel"],
            record["endpoint_host"],
            record["started_at"],
            record["finished_at"],
            record["recorded_at"],
            record["request_count"],
            record["attempts_persisted"],
            record["succeeded_count"],
            record["failed_count"],
            jsonb(record["attempt_ids"]),
            jsonb(record["requested_event_ids"]),
            jsonb(record["persisted_event_ids"]),
            record["delivery_fingerprint"],
            record["retry_policy_fingerprint"],
            record["retry_execution_policy_fingerprint"],
            record["lock_model_version"],
            record["lock_key_fingerprint"],
            record["lock_acquired"],
            record["lock_released"],
            None,
            jsonb(record),
            digest,
        ),
    )


def _failed_record(
    *,
    request_id: str,
    plan_id: str,
    event_id: str,
    started_at: datetime,
    finished_at: datetime,
    terminal_status: str,
    attempt_id: str | None,
) -> dict[str, Any]:
    persisted = attempt_id is not None
    return build_retry_execution_record(
        request_id=request_id,
        plan_id=plan_id,
        started_at=started_at,
        finished_at=finished_at,
        recorded_at=finished_at + timedelta(seconds=1),
        terminal_status=terminal_status,
        failure_code="storage_error" if terminal_status == "persistence_uncertain"
        else "validation_error",
        request_count=1,
        attempts_persisted=1 if persisted else 0,
        succeeded_count=0,
        failed_count=1 if persisted else 0,
        attempt_ids=[] if attempt_id is None else [attempt_id],
        requested_event_ids=[event_id],
        persisted_event_ids=[] if attempt_id is None else [event_id],
        execution_summary=None,
        endpoint_host="alerts.example.test",
        delivery_fingerprint="follow-up-delivery-fingerprint",
        retry_policy_fingerprint="follow-up-retry-policy-fingerprint",
        retry_execution_policy_fingerprint="follow-up-execution-policy-fingerprint",
        lock_model_version=LOCK_MODEL_VERSION,
        lock_key_fingerprint=LOCK_KEY_FINGERPRINT,
        lock_acquired=True,
        lock_released=None,
    )


def _assert_follow_up_rows(cursor: Any) -> dict[str, str]:
    cursor.execute(
        """
        SELECT event_id, follow_up_reason
        FROM risk_platform.current_notification_retry_follow_up
        WHERE event_id LIKE 'follow-up-event-%'
        ORDER BY event_id
        """
    )
    actual = {str(event_id): str(reason) for event_id, reason in cursor.fetchall()}
    expected = {
        "follow-up-event-acknowledged": "acknowledged",
        "follow-up-event-delivered": "delivered",
        "follow-up-event-execution": "execution_review_required",
        "follow-up-event-initial": "initial_delivery_required",
        "follow-up-event-retry": "retry_plan_required",
        "follow-up-event-superseded": "execution_review_required",
        "follow-up-event-uncertain": "persistence_review_required",
    }
    if actual != expected:
        raise AssertionError(f"notification follow-up classifications changed: {actual!r}")

    cursor.execute(
        """
        SELECT event_id
        FROM risk_platform.current_notification_ambiguous_outcomes
        WHERE event_id LIKE 'follow-up-event-%'
        ORDER BY event_id
        """
    )
    ambiguous = [str(row[0]) for row in cursor.fetchall()]
    if ambiguous != ["follow-up-event-uncertain"]:
        raise AssertionError(f"current ambiguity selection changed: {ambiguous!r}")

    cursor.execute(
        """
        SELECT terminal_status
        FROM risk_platform.latest_notification_retry_execution_by_event
        WHERE event_id = 'follow-up-event-superseded'
        """
    )
    latest = cursor.fetchone()
    if latest != ("failed_after_request",):
        raise AssertionError(f"superseded uncertainty remained current: {latest!r}")

    cursor.execute(
        """
        SELECT COUNT(*)
        FROM risk_platform.current_notification_delivery_failures
        WHERE event_id LIKE 'follow-up-event-%'
        """
    )
    failure_row = cursor.fetchone()
    if failure_row is None:
        raise AssertionError("delivery failure queue count is unavailable")
    failure_count = int(failure_row[0])
    if failure_count != 4:
        raise AssertionError(f"delivery failure queue changed: {failure_count}")
    return actual
