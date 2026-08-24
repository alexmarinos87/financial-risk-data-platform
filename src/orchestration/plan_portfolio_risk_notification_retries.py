from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.config import load_yaml
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    CHANNEL,
    WebhookDeliveryConfig,
    parse_webhook_delivery_config,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "portfolio-risk-dead-letter-retry-plan-v1"
POLICY_MODEL_VERSION = "portfolio-risk-dead-letter-retry-policy-v1"
OUTBOX_MODEL_VERSION = "portfolio-risk-notification-outbox-v1"
MAX_CANDIDATE_ROWS = 10_000
MAX_PLAN_EVENTS = 100
MAX_EVENT_AGE_SECONDS = 30 * 24 * 60 * 60
MAX_RETRY_BACKOFF_SECONDS = 24 * 60 * 60
CLASSIFICATIONS = (
    "retryable",
    "not_yet_eligible",
    "attempts_exhausted",
    "expired",
    "acknowledged",
    "invalid",
)
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

CandidateReader = Callable[..., list[dict[str, Any]]]


@dataclass(frozen=True, slots=True)
class RetryPlanningPolicy:
    max_candidate_rows: int
    max_plan_events: int
    max_event_age_seconds: int
    max_backoff_seconds: int
    retryable_http_statuses: tuple[int, ...]
    retryable_error_codes: tuple[str, ...]

    @property
    def fingerprint(self) -> str:
        payload = {
            "channel": CHANNEL,
            "max_backoff_seconds": self.max_backoff_seconds,
            "max_candidate_rows": self.max_candidate_rows,
            "max_event_age_seconds": self.max_event_age_seconds,
            "max_plan_events": self.max_plan_events,
            "model_version": POLICY_MODEL_VERSION,
            "retryable_error_codes": list(self.retryable_error_codes),
            "retryable_http_statuses": list(self.retryable_http_statuses),
        }
        digest = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"{POLICY_MODEL_VERSION}-policy-{digest}"


def _bounded_integer(
    value: Any,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def _safe_segment(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:
        raise ValidationError(f"{label} must be timezone-aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _sorted_unique_http_statuses(value: Any) -> tuple[int, ...]:
    if not isinstance(value, list) or not value:
        raise ValidationError("retryable_http_statuses must be a non-empty array")
    statuses: list[int] = []
    for item in value:
        if type(item) is not int or not 100 <= item <= 599 or 200 <= item <= 299:
            raise ValidationError(
                "retryable_http_statuses must contain non-success HTTP statuses"
            )
        statuses.append(item)
    if len(statuses) != len(set(statuses)) or statuses != sorted(statuses):
        raise ValidationError(
            "retryable_http_statuses must be sorted and contain no duplicates"
        )
    return tuple(statuses)


def _sorted_unique_error_codes(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValidationError("retryable_error_codes must be a non-empty array")
    codes: list[str] = []
    for item in value:
        if not isinstance(item, str) or not ERROR_CODE_PATTERN.fullmatch(item):
            raise ValidationError("retryable_error_codes contains an invalid value")
        codes.append(item)
    if len(codes) != len(set(codes)) or codes != sorted(codes):
        raise ValidationError(
            "retryable_error_codes must be sorted and contain no duplicates"
        )
    return tuple(codes)


def parse_retry_planning_policy(
    payload: Mapping[str, Any],
    delivery_config: WebhookDeliveryConfig,
) -> RetryPlanningPolicy:
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    delivery = payload.get("delivery")
    if not isinstance(delivery, Mapping):
        raise ValidationError("notification delivery configuration is missing delivery")
    planning = delivery.get("retry_planning")
    if not isinstance(planning, Mapping):
        raise ValidationError(
            "notification delivery configuration is missing retry_planning"
        )
    policy = RetryPlanningPolicy(
        max_candidate_rows=_bounded_integer(
            planning.get("max_candidate_rows"),
            "max_candidate_rows",
            minimum=1,
            maximum=MAX_CANDIDATE_ROWS,
        ),
        max_plan_events=_bounded_integer(
            planning.get("max_plan_events"),
            "max_plan_events",
            minimum=1,
            maximum=MAX_PLAN_EVENTS,
        ),
        max_event_age_seconds=_bounded_integer(
            planning.get("max_event_age_seconds"),
            "max_event_age_seconds",
            minimum=1,
            maximum=MAX_EVENT_AGE_SECONDS,
        ),
        max_backoff_seconds=_bounded_integer(
            planning.get("max_backoff_seconds"),
            "max_backoff_seconds",
            minimum=1,
            maximum=MAX_RETRY_BACKOFF_SECONDS,
        ),
        retryable_http_statuses=_sorted_unique_http_statuses(
            planning.get("retryable_http_statuses")
        ),
        retryable_error_codes=_sorted_unique_error_codes(
            planning.get("retryable_error_codes")
        ),
    )
    if policy.max_plan_events > delivery_config.max_batch_events:
        raise ValidationError(
            "max_plan_events must not exceed webhook max_batch_events"
        )
    if policy.max_backoff_seconds < delivery_config.initial_backoff_seconds:
        raise ValidationError(
            "max_backoff_seconds must not be below initial_backoff_seconds"
        )
    return policy


def load_retry_planning_contract(
    path: Path,
) -> tuple[WebhookDeliveryConfig, RetryPlanningPolicy]:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "notification delivery configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    delivery_config = parse_webhook_delivery_config(payload)
    policy = parse_retry_planning_policy(payload, delivery_config)
    return delivery_config, policy


def read_notification_retry_candidates(
    *,
    dsn: str,
    planned_at: datetime | str,
    max_candidate_rows: int,
    policy_id: str | None = None,
    portfolio_id: str | None = None,
    schema_name: str = "risk_platform",
) -> list[dict[str, Any]]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    as_of = _aware_utc(planned_at, "planned_at")
    row_limit = _bounded_integer(
        max_candidate_rows,
        "max_candidate_rows",
        minimum=1,
        maximum=MAX_CANDIDATE_ROWS,
    )
    selected_policy = _safe_segment(policy_id, "policy_id", optional=True)
    selected_portfolio = _safe_segment(
        portfolio_id,
        "portfolio_id",
        optional=True,
    )
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL retry planning requires psycopg. Run `make setup` first."
        ) from exc

    schema = '"' + schema_name.replace('"', '""') + '"'
    filter_clauses: list[str] = []
    filter_parameters: list[Any] = []
    if selected_policy is not None:
        filter_clauses.append("AND pending.policy_id = %s")
        filter_parameters.append(selected_policy)
    if selected_portfolio is not None:
        filter_clauses.append("AND pending.portfolio_id = %s")
        filter_parameters.append(selected_portfolio)
    filter_sql = "\n          ".join(filter_clauses)
    statement = f"""
        SELECT
            pending.event_id,
            pending.model_version AS outbox_model_version,
            pending.event_type,
            pending.transition_type,
            pending.delivery_disposition,
            pending.source_evaluation_calculation_id,
            pending.policy_id,
            pending.policy_fingerprint,
            pending.portfolio_id,
            pending.definition_fingerprint,
            pending.metric_name,
            pending.subject_type,
            pending.subject_key,
            pending.current_status,
            pending.ts_event,
            pending.ts_ingest,
            pending.payload_json,
            COALESCE(attempt_count.attempt_count, 0)::INTEGER AS attempt_count,
            latest_attempt.attempt_id AS last_attempt_id,
            latest_attempt.attempt_number AS last_attempt_number,
            latest_attempt.attempted_at AS last_attempted_at,
            latest_attempt.outcome AS last_attempt_outcome,
            latest_attempt.http_status AS last_http_status,
            latest_attempt.error_code AS last_error_code,
            latest_ack.acknowledgement_id,
            latest_ack.acknowledged_at,
            latest_ack.disposition AS acknowledgement_disposition
        FROM {schema}.portfolio_risk_notification_pending pending
        LEFT JOIN LATERAL (
            SELECT COUNT(*) AS attempt_count
            FROM {schema}.portfolio_risk_notification_delivery_attempts attempt
            WHERE attempt.event_id = pending.event_id
              AND attempt.channel = %s
              AND attempt.attempted_at <= %s
        ) attempt_count ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                attempt.attempt_id,
                attempt.attempt_number,
                attempt.attempted_at,
                attempt.outcome,
                attempt.http_status,
                attempt.error_code
            FROM {schema}.portfolio_risk_notification_delivery_attempts attempt
            WHERE attempt.event_id = pending.event_id
              AND attempt.channel = %s
              AND attempt.attempted_at <= %s
            ORDER BY
                attempt.attempt_number DESC,
                attempt.attempted_at DESC,
                attempt.attempt_id DESC
            LIMIT 1
        ) latest_attempt ON TRUE
        LEFT JOIN LATERAL (
            SELECT
                acknowledgement.acknowledgement_id,
                acknowledgement.acknowledged_at,
                acknowledgement.disposition
            FROM {schema}.portfolio_risk_limit_acknowledgements acknowledgement
            WHERE acknowledgement.evaluation_calculation_id
                    = pending.source_evaluation_calculation_id
              AND acknowledgement.acknowledged_at <= %s
            ORDER BY
                acknowledgement.acknowledged_at DESC,
                acknowledgement.acknowledgement_id DESC
            LIMIT 1
        ) latest_ack ON TRUE
        WHERE pending.ts_ingest <= %s
          {filter_sql}
          AND NOT EXISTS (
              SELECT 1
              FROM {schema}.portfolio_risk_notification_delivery_attempts success
              WHERE success.event_id = pending.event_id
                AND success.channel = %s
                AND success.outcome = 'succeeded'
                AND success.attempted_at <= %s
          )
        ORDER BY pending.ts_event, pending.event_id
        LIMIT %s
    """
    parameters = (
        CHANNEL,
        as_of,
        CHANNEL,
        as_of,
        as_of,
        as_of,
        *filter_parameters,
        CHANNEL,
        as_of,
        row_limit + 1,
    )
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, parameters)
                records = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError("Unable to read notification retry evidence") from None
    if len(records) > row_limit:
        raise ValidationError(
            "notification retry evidence exceeds max_candidate_rows; narrow the plan"
        )
    return records


def _required_candidate_text(
    candidate: Mapping[str, Any],
    key: str,
    *,
    maximum: int = 512,
) -> str:
    value = candidate.get(key)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValidationError(f"candidate {key} must be non-empty canonical text")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValidationError(f"candidate {key} is invalid")
    return value


def _optional_candidate_text(
    candidate: Mapping[str, Any],
    key: str,
    *,
    maximum: int = 512,
) -> str | None:
    value = candidate.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValidationError(f"candidate {key} must be canonical text")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValidationError(f"candidate {key} is invalid")
    return value


def _canonical_payload(candidate: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    payload = candidate.get("payload_json")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except ValueError:
            raise ValidationError("candidate payload_json is invalid") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("candidate payload_json must be an object")
    canonical_payload = dict(payload)
    try:
        encoded = json.dumps(
            canonical_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("candidate payload_json is not canonical JSON") from None
    return canonical_payload, hashlib.sha256(encoded).hexdigest()


def _retry_failure_is_supported(
    *,
    http_status: int | None,
    error_code: str | None,
    policy: RetryPlanningPolicy,
) -> bool:
    if http_status is not None:
        return (
            http_status in policy.retryable_http_statuses
            and error_code == f"http_{http_status}"
        )
    return error_code in policy.retryable_error_codes


def _retry_delay_seconds(
    *,
    attempt_count: int,
    delivery_config: WebhookDeliveryConfig,
    policy: RetryPlanningPolicy,
) -> int:
    exponent = max(attempt_count - 1, 0)
    return min(
        delivery_config.initial_backoff_seconds * (2**exponent),
        policy.max_backoff_seconds,
    )


def _classify_candidate(
    candidate: Mapping[str, Any],
    *,
    planned_at: datetime,
    delivery_config: WebhookDeliveryConfig,
    policy: RetryPlanningPolicy,
) -> dict[str, Any]:
    event_id = _required_candidate_text(candidate, "event_id")
    event_type = _required_candidate_text(candidate, "event_type", maximum=128)
    transition_type = _required_candidate_text(
        candidate,
        "transition_type",
        maximum=128,
    )
    source_evaluation_id = _required_candidate_text(
        candidate,
        "source_evaluation_calculation_id",
    )
    policy_id = _required_candidate_text(candidate, "policy_id", maximum=128)
    policy_fingerprint = _required_candidate_text(
        candidate,
        "policy_fingerprint",
    )
    portfolio_id = _required_candidate_text(
        candidate,
        "portfolio_id",
        maximum=128,
    )
    definition_fingerprint = _required_candidate_text(
        candidate,
        "definition_fingerprint",
    )
    metric_name = _required_candidate_text(candidate, "metric_name", maximum=128)
    subject_type = _required_candidate_text(candidate, "subject_type", maximum=128)
    subject_key = _required_candidate_text(candidate, "subject_key")
    current_status = _required_candidate_text(
        candidate,
        "current_status",
        maximum=128,
    )
    ts_event = _aware_utc(candidate.get("ts_event"), "candidate.ts_event")
    ts_ingest = _aware_utc(candidate.get("ts_ingest"), "candidate.ts_ingest")
    payload, payload_sha256 = _canonical_payload(candidate)
    attempt_count = candidate.get("attempt_count")
    if type(attempt_count) is not int or attempt_count < 0:
        raise ValidationError("candidate attempt_count must be non-negative")
    last_attempt_id = _optional_candidate_text(candidate, "last_attempt_id")
    last_attempt_number = candidate.get("last_attempt_number")
    last_attempted_at_raw = candidate.get("last_attempted_at")
    last_attempted_at = (
        None
        if last_attempted_at_raw is None
        else _aware_utc(last_attempted_at_raw, "candidate.last_attempted_at")
    )
    last_attempt_outcome = _optional_candidate_text(
        candidate,
        "last_attempt_outcome",
        maximum=32,
    )
    last_http_status = candidate.get("last_http_status")
    if last_http_status is not None and (
        type(last_http_status) is not int or not 100 <= last_http_status <= 599
    ):
        raise ValidationError("candidate last_http_status is invalid")
    last_error_code = _optional_candidate_text(
        candidate,
        "last_error_code",
        maximum=128,
    )
    acknowledgement_id = _optional_candidate_text(
        candidate,
        "acknowledgement_id",
    )
    acknowledged_at_raw = candidate.get("acknowledged_at")
    acknowledged_at = (
        None
        if acknowledged_at_raw is None
        else _aware_utc(acknowledged_at_raw, "candidate.acknowledged_at")
    )
    acknowledgement_disposition = _optional_candidate_text(
        candidate,
        "acknowledgement_disposition",
        maximum=64,
    )

    exact_event = {
        "current_status": current_status,
        "definition_fingerprint": definition_fingerprint,
        "event_id": event_id,
        "event_type": event_type,
        "metric_name": metric_name,
        "payload_sha256": payload_sha256,
        "policy_fingerprint": policy_fingerprint,
        "policy_id": policy_id,
        "portfolio_id": portfolio_id,
        "source_evaluation_calculation_id": source_evaluation_id,
        "subject_key": subject_key,
        "subject_type": subject_type,
        "transition_type": transition_type,
        "ts_event": ts_event.isoformat(),
        "ts_ingest": ts_ingest.isoformat(),
    }
    event_document_sha256 = hashlib.sha256(
        json.dumps(
            exact_event,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
    event_age_seconds = (planned_at - ts_event).total_seconds()
    next_eligible_at: datetime | None = None
    classification = "invalid"
    reason = "candidate_contract_invalid"

    structural_reason: str | None = None
    if candidate.get("outbox_model_version") != OUTBOX_MODEL_VERSION:
        structural_reason = "outbox_model_version_unsupported"
    elif candidate.get("delivery_disposition") != "pending":
        structural_reason = "delivery_disposition_not_pending"
    elif ts_ingest < ts_event:
        structural_reason = "event_ingest_precedes_event"
    elif ts_event > planned_at or ts_ingest > planned_at:
        structural_reason = "event_timestamp_future"
    elif payload.get("event_id") not in {None, event_id}:
        structural_reason = "payload_event_identity_mismatch"
    elif attempt_count == 0 and any(
        value is not None
        for value in (
            last_attempt_id,
            last_attempt_number,
            last_attempted_at,
            last_attempt_outcome,
            last_http_status,
            last_error_code,
        )
    ):
        structural_reason = "attempt_summary_inconsistent"
    elif attempt_count > 0 and (
        last_attempt_id is None
        or type(last_attempt_number) is not int
        or last_attempt_number != attempt_count
        or last_attempted_at is None
        or last_attempt_outcome is None
    ):
        structural_reason = "attempt_summary_inconsistent"
    elif last_attempted_at is not None and (
        last_attempted_at < ts_event or last_attempted_at > planned_at
    ):
        structural_reason = "attempt_timestamp_invalid"
    elif acknowledgement_id is None and any(
        value is not None
        for value in (acknowledged_at, acknowledgement_disposition)
    ):
        structural_reason = "acknowledgement_summary_inconsistent"
    elif acknowledgement_id is not None and (
        acknowledged_at is None
        or acknowledgement_disposition
        not in {"investigating", "accepted", "false_positive"}
        or acknowledged_at < ts_event
        or acknowledged_at > planned_at
    ):
        structural_reason = "acknowledgement_summary_inconsistent"

    if structural_reason is not None:
        reason = structural_reason
    elif acknowledgement_id is not None:
        classification = "acknowledged"
        reason = "source_breach_acknowledged"
    elif event_age_seconds > policy.max_event_age_seconds:
        classification = "expired"
        reason = "event_age_exceeds_policy"
    elif attempt_count >= delivery_config.max_attempts_per_event:
        classification = "attempts_exhausted"
        reason = "maximum_attempts_reached"
    elif attempt_count == 0:
        reason = "failed_attempt_missing"
    elif last_attempt_outcome != "failed":
        reason = "latest_attempt_not_failed"
    elif not _retry_failure_is_supported(
        http_status=last_http_status,
        error_code=last_error_code,
        policy=policy,
    ):
        reason = "last_failure_not_retryable"
    else:
        assert last_attempted_at is not None
        delay_seconds = _retry_delay_seconds(
            attempt_count=attempt_count,
            delivery_config=delivery_config,
            policy=policy,
        )
        next_eligible_at = last_attempted_at + timedelta(seconds=delay_seconds)
        if planned_at < next_eligible_at:
            classification = "not_yet_eligible"
            reason = "retry_backoff_active"
        else:
            classification = "retryable"
            reason = "eligible_retryable_failure"

    acknowledgement_evidence: dict[str, Any] | None = None
    if acknowledgement_id is not None:
        assert acknowledged_at is not None
        acknowledgement_evidence = {
            "acknowledged_at": acknowledged_at.isoformat(),
            "acknowledgement_id": acknowledgement_id,
            "disposition": acknowledgement_disposition,
        }
    last_attempt_evidence: dict[str, Any] | None = None
    if last_attempt_id is not None:
        assert last_attempted_at is not None
        last_attempt_evidence = {
            "attempt_id": last_attempt_id,
            "attempt_number": last_attempt_number,
            "attempted_at": last_attempted_at.isoformat(),
            "error_code": last_error_code,
            "http_status": last_http_status,
            "outcome": last_attempt_outcome,
        }

    return {
        "acknowledgement": acknowledgement_evidence,
        "attempt_count": attempt_count,
        "classification": classification,
        "event_age_seconds": max(0.0, event_age_seconds),
        "event_document_sha256": event_document_sha256,
        "event_id": event_id,
        "last_attempt": last_attempt_evidence,
        "next_eligible_at": (
            next_eligible_at.isoformat() if next_eligible_at is not None else None
        ),
        "policy_id": policy_id,
        "portfolio_id": portfolio_id,
        "reason": reason,
        "source_evaluation_calculation_id": source_evaluation_id,
        "ts_event": ts_event.isoformat(),
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


def plan_portfolio_risk_notification_retries(
    *,
    config_path: Path,
    dsn: str,
    planned_at: datetime | str,
    policy_id: str | None = None,
    portfolio_id: str | None = None,
    reader: CandidateReader | None = None,
) -> dict[str, Any]:
    as_of = _aware_utc(planned_at, "planned_at")
    selected_policy_id = _safe_segment(policy_id, "policy_id", optional=True)
    selected_portfolio_id = _safe_segment(
        portfolio_id,
        "portfolio_id",
        optional=True,
    )
    delivery_config, policy = load_retry_planning_contract(config_path)
    selected_reader = reader or read_notification_retry_candidates
    candidates = selected_reader(
        dsn=dsn,
        planned_at=as_of,
        max_candidate_rows=policy.max_candidate_rows,
        policy_id=selected_policy_id,
        portfolio_id=selected_portfolio_id,
    )
    if not isinstance(candidates, list):
        raise StorageError("notification retry reader returned invalid evidence")
    if len(candidates) > policy.max_candidate_rows:
        raise ValidationError(
            "notification retry evidence exceeds max_candidate_rows"
        )

    events = [
        _classify_candidate(
            candidate,
            planned_at=as_of,
            delivery_config=delivery_config,
            policy=policy,
        )
        for candidate in candidates
    ]
    events.sort(key=lambda event: (event["ts_event"], event["event_id"]))
    if len({event["event_id"] for event in events}) != len(events):
        raise ValidationError("notification retry evidence contains duplicate event_id")
    retryable_event_ids = [
        event["event_id"]
        for event in events
        if event["classification"] == "retryable"
    ]
    if len(retryable_event_ids) > policy.max_plan_events:
        raise ValidationError(
            "retryable notification count exceeds max_plan_events; narrow the plan"
        )
    classification_counts = {
        classification: sum(
            event["classification"] == classification for event in events
        )
        for classification in CLASSIFICATIONS
    }
    identity = {
        "channel": CHANNEL,
        "delivery_config_fingerprint": delivery_config.fingerprint,
        "events": events,
        "filters": {
            "policy_id": selected_policy_id,
            "portfolio_id": selected_portfolio_id,
        },
        "model_version": MODEL_VERSION,
        "planned_at": as_of.isoformat(),
        "retry_policy_fingerprint": policy.fingerprint,
    }
    return {
        "plan_id": _plan_id(identity),
        "model_version": MODEL_VERSION,
        "planned_at": as_of.isoformat(),
        "channel": CHANNEL,
        "delivery_config": {
            "enabled": delivery_config.enabled,
            "fingerprint": delivery_config.fingerprint,
            "max_attempts_per_event": delivery_config.max_attempts_per_event,
        },
        "retry_policy": {
            "fingerprint": policy.fingerprint,
            "max_backoff_seconds": policy.max_backoff_seconds,
            "max_candidate_rows": policy.max_candidate_rows,
            "max_event_age_seconds": policy.max_event_age_seconds,
            "max_plan_events": policy.max_plan_events,
            "retryable_error_codes": list(policy.retryable_error_codes),
            "retryable_http_statuses": list(policy.retryable_http_statuses),
        },
        "filters": {
            "policy_id": selected_policy_id,
            "portfolio_id": selected_portfolio_id,
        },
        "selection": {
            "candidates_examined": len(events),
            "classification_counts": classification_counts,
            "retryable_events": len(retryable_event_ids),
        },
        "events": events,
        "retryable_event_ids": retryable_event_ids,
        "delivery_performed": False,
        "delivery_attempt_written": False,
        "dead_letter_mutated": False,
        "external_request_performed": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a deterministic, delivery-free retry plan from retained "
            "portfolio risk notification attempt evidence."
        )
    )
    parser.add_argument("--planned-at", required=True)
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--policy-id")
    parser.add_argument("--portfolio-id")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("notification retry summary must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except (OSError, TypeError, ValueError):
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write notification retry plan summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = plan_portfolio_risk_notification_retries(
            config_path=args.config,
            dsn=args.dsn,
            planned_at=args.planned_at,
            policy_id=args.policy_id,
            portfolio_id=args.portfolio_id,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError as exc:
        print(f"Notification retry plan rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError as exc:
        print(f"Notification retry plan failed: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print("Notification retry plan failed: unexpected local failure", file=sys.stderr)
        return 1
    print(json.dumps(summary, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
