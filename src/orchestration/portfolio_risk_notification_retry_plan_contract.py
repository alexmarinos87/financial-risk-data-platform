from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import CHANNEL
from src.orchestration.plan_portfolio_risk_notification_retries import (
    CLASSIFICATIONS,
    MODEL_VERSION as RETRY_PLAN_MODEL_VERSION,
    _plan_id,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    ERROR_CODE_PATTERN,
    MAX_PLAN_FILE_BYTES,
    SAFE_SEGMENT_PATTERN,
    SHA256_PATTERN,
    aware_utc,
    bounded_integer,
    exact_mapping,
    nonnegative_number,
    safe_segment,
    safe_text,
)

PLAN_FIELDS = frozenset(
    {
        "plan_id",
        "model_version",
        "planned_at",
        "channel",
        "delivery_config",
        "retry_policy",
        "filters",
        "selection",
        "events",
        "retryable_event_ids",
        "delivery_performed",
        "delivery_attempt_written",
        "dead_letter_mutated",
        "external_request_performed",
    }
)
PLAN_EVENT_FIELDS = frozenset(
    {
        "acknowledgement",
        "attempt_count",
        "classification",
        "event_age_seconds",
        "event_document_sha256",
        "event_id",
        "last_attempt",
        "next_eligible_at",
        "policy_id",
        "portfolio_id",
        "reason",
        "source_evaluation_calculation_id",
        "ts_event",
    }
)
LAST_ATTEMPT_FIELDS = frozenset(
    {
        "attempt_id",
        "attempt_number",
        "attempted_at",
        "error_code",
        "http_status",
        "outcome",
    }
)
ACKNOWLEDGEMENT_FIELDS = frozenset(
    {"acknowledged_at", "acknowledgement_id", "disposition"}
)


def _validate_last_attempt(value: Any, attempt_count: int, label: str) -> None:
    if value is None:
        if attempt_count != 0:
            raise ValidationError(f"{label} is required when attempt_count is positive")
        return
    mapping = exact_mapping(value, LAST_ATTEMPT_FIELDS, label)
    if attempt_count == 0:
        raise ValidationError(f"{label} must be null when attempt_count is zero")
    safe_text(mapping.get("attempt_id"), f"{label}.attempt_id")
    attempt_number = bounded_integer(
        mapping.get("attempt_number"),
        f"{label}.attempt_number",
        minimum=1,
        maximum=10,
    )
    if attempt_number != attempt_count:
        raise ValidationError(f"{label}.attempt_number must equal attempt_count")
    aware_utc(mapping.get("attempted_at"), f"{label}.attempted_at")
    if mapping.get("outcome") != "failed":
        raise ValidationError(f"{label}.outcome must be failed")
    http_status = mapping.get("http_status")
    if http_status is not None and (
        type(http_status) is not int or not 100 <= http_status <= 599
    ):
        raise ValidationError(f"{label}.http_status is invalid")
    error_code = mapping.get("error_code")
    if error_code is not None and (
        not isinstance(error_code, str) or not ERROR_CODE_PATTERN.fullmatch(error_code)
    ):
        raise ValidationError(f"{label}.error_code is invalid")
    if error_code is None:
        raise ValidationError(f"{label}.error_code is required for a failed attempt")


def _validate_acknowledgement(value: Any, label: str) -> None:
    if value is None:
        return
    mapping = exact_mapping(value, ACKNOWLEDGEMENT_FIELDS, label)
    safe_text(mapping.get("acknowledgement_id"), f"{label}.acknowledgement_id")
    aware_utc(mapping.get("acknowledged_at"), f"{label}.acknowledged_at")
    if mapping.get("disposition") not in {
        "investigating",
        "accepted",
        "false_positive",
    }:
        raise ValidationError(f"{label}.disposition is invalid")


def _validate_plan_event(
    value: Any,
    *,
    planned_at: datetime,
    max_attempts_per_event: int,
    max_event_age_seconds: int,
    label: str,
) -> Mapping[str, Any]:
    event = exact_mapping(value, PLAN_EVENT_FIELDS, label)
    event_id = safe_text(event.get("event_id"), f"{label}.event_id")
    safe_text(event.get("policy_id"), f"{label}.policy_id", maximum=128)
    safe_text(event.get("portfolio_id"), f"{label}.portfolio_id", maximum=128)
    safe_text(
        event.get("source_evaluation_calculation_id"),
        f"{label}.source_evaluation_calculation_id",
    )
    event_sha = event.get("event_document_sha256")
    if not isinstance(event_sha, str) or not SHA256_PATTERN.fullmatch(event_sha):
        raise ValidationError(f"{label}.event_document_sha256 is invalid")
    classification = event.get("classification")
    if classification not in CLASSIFICATIONS:
        raise ValidationError(f"{label}.classification is invalid")
    safe_text(event.get("reason"), f"{label}.reason", maximum=128)
    ts_event = aware_utc(event.get("ts_event"), f"{label}.ts_event")
    event_age = nonnegative_number(
        event.get("event_age_seconds"),
        f"{label}.event_age_seconds",
    )
    expected_age = max(0.0, (planned_at - ts_event).total_seconds())
    if abs(event_age - expected_age) > 0.000001:
        raise ValidationError(f"{label}.event_age_seconds does not reconcile")
    attempt_count = bounded_integer(
        event.get("attempt_count"),
        f"{label}.attempt_count",
        minimum=0,
        maximum=10,
    )
    _validate_last_attempt(event.get("last_attempt"), attempt_count, f"{label}.last_attempt")
    _validate_acknowledgement(event.get("acknowledgement"), f"{label}.acknowledgement")
    next_eligible_raw = event.get("next_eligible_at")
    next_eligible = (
        None
        if next_eligible_raw is None
        else aware_utc(next_eligible_raw, f"{label}.next_eligible_at")
    )
    if classification in {"retryable", "not_yet_eligible"}:
        if next_eligible is None or event.get("last_attempt") is None:
            raise ValidationError(
                f"{label} retry eligibility requires a failed attempt and timestamp"
            )
        if classification == "retryable" and planned_at < next_eligible:
            raise ValidationError(f"{label} retryable event is still in backoff")
        if classification == "not_yet_eligible" and planned_at >= next_eligible:
            raise ValidationError(f"{label} not_yet_eligible event has completed backoff")
    elif next_eligible is not None:
        raise ValidationError(
            f"{label}.next_eligible_at is only valid for retryable failures"
        )
    if classification == "acknowledged" and event.get("acknowledgement") is None:
        raise ValidationError(f"{label} acknowledged event lacks acknowledgement evidence")
    if classification == "attempts_exhausted" and attempt_count < max_attempts_per_event:
        raise ValidationError(f"{label} exhausted event has remaining attempt capacity")
    if classification == "expired" and event_age <= max_event_age_seconds:
        raise ValidationError(f"{label} expired event is within the age policy")
    if classification == "retryable":
        if event.get("acknowledgement") is not None:
            raise ValidationError(f"{label} retryable event must be unacknowledged")
        if attempt_count >= max_attempts_per_event:
            raise ValidationError(f"{label} retryable event has no attempt capacity")
    if not SAFE_SEGMENT_PATTERN.fullmatch(event_id):
        raise ValidationError(f"{label}.event_id must be a safe text segment")
    return event


def validate_retry_plan_document(payload: Any) -> dict[str, Any]:
    plan = exact_mapping(payload, PLAN_FIELDS, "retry plan")
    if plan.get("model_version") != RETRY_PLAN_MODEL_VERSION:
        raise ValidationError("retry plan model_version is unsupported")
    if plan.get("channel") != CHANNEL:
        raise ValidationError("retry plan channel is unsupported")
    planned_at = aware_utc(plan.get("planned_at"), "retry plan planned_at")
    plan_id = safe_text(plan.get("plan_id"), "retry plan plan_id")

    delivery = exact_mapping(
        plan.get("delivery_config"),
        frozenset({"enabled", "fingerprint", "max_attempts_per_event"}),
        "retry plan delivery_config",
    )
    if type(delivery.get("enabled")) is not bool:
        raise ValidationError("retry plan delivery_config.enabled must be boolean")
    safe_text(delivery.get("fingerprint"), "retry plan delivery_config.fingerprint")
    max_attempts = bounded_integer(
        delivery.get("max_attempts_per_event"),
        "retry plan delivery_config.max_attempts_per_event",
        minimum=1,
        maximum=10,
    )

    retry_policy = exact_mapping(
        plan.get("retry_policy"),
        frozenset(
            {
                "fingerprint",
                "max_backoff_seconds",
                "max_candidate_rows",
                "max_event_age_seconds",
                "max_plan_events",
                "retryable_error_codes",
                "retryable_http_statuses",
            }
        ),
        "retry plan retry_policy",
    )
    safe_text(retry_policy.get("fingerprint"), "retry plan retry_policy.fingerprint")
    max_event_age = bounded_integer(
        retry_policy.get("max_event_age_seconds"),
        "retry plan retry_policy.max_event_age_seconds",
        minimum=1,
        maximum=30 * 24 * 60 * 60,
    )
    max_plan_events = bounded_integer(
        retry_policy.get("max_plan_events"),
        "retry plan retry_policy.max_plan_events",
        minimum=1,
        maximum=100,
    )
    bounded_integer(
        retry_policy.get("max_backoff_seconds"),
        "retry plan retry_policy.max_backoff_seconds",
        minimum=1,
        maximum=24 * 60 * 60,
    )
    bounded_integer(
        retry_policy.get("max_candidate_rows"),
        "retry plan retry_policy.max_candidate_rows",
        minimum=1,
        maximum=10_000,
    )
    statuses = retry_policy.get("retryable_http_statuses")
    if (
        not isinstance(statuses, list)
        or not statuses
        or any(type(status) is not int for status in statuses)
        or statuses != sorted(set(statuses))
    ):
        raise ValidationError(
            "retry plan retryable_http_statuses must be sorted and unique"
        )
    error_codes = retry_policy.get("retryable_error_codes")
    if (
        not isinstance(error_codes, list)
        or not error_codes
        or any(
            not isinstance(code, str) or not ERROR_CODE_PATTERN.fullmatch(code)
            for code in error_codes
        )
        or error_codes != sorted(set(error_codes))
    ):
        raise ValidationError(
            "retry plan retryable_error_codes must be sorted and unique"
        )

    filters = exact_mapping(
        plan.get("filters"),
        frozenset({"policy_id", "portfolio_id"}),
        "retry plan filters",
    )
    safe_segment(filters.get("policy_id"), "retry plan policy_id", optional=True)
    safe_segment(filters.get("portfolio_id"), "retry plan portfolio_id", optional=True)

    events_raw = plan.get("events")
    if not isinstance(events_raw, list):
        raise ValidationError("retry plan events must be an array")
    events = [
        _validate_plan_event(
            event,
            planned_at=planned_at,
            max_attempts_per_event=max_attempts,
            max_event_age_seconds=max_event_age,
            label=f"retry plan events[{index}]",
        )
        for index, event in enumerate(events_raw)
    ]
    event_ids = [cast(str, event["event_id"]) for event in events]
    if len(event_ids) != len(set(event_ids)):
        raise ValidationError("retry plan contains duplicate event_id")
    expected_order = sorted(
        events,
        key=lambda event: (
            cast(str, event["ts_event"]),
            cast(str, event["event_id"]),
        ),
    )
    if events != expected_order:
        raise ValidationError("retry plan events are not in canonical order")

    retryable_ids = plan.get("retryable_event_ids")
    if not isinstance(retryable_ids, list) or any(
        not isinstance(event_id, str) for event_id in retryable_ids
    ):
        raise ValidationError("retry plan retryable_event_ids must be a text array")
    expected_retryable_ids = [
        cast(str, event["event_id"])
        for event in events
        if event["classification"] == "retryable"
    ]
    if retryable_ids != expected_retryable_ids:
        raise ValidationError(
            "retry plan retryable_event_ids do not match event classifications"
        )
    if len(retryable_ids) > max_plan_events:
        raise ValidationError("retry plan exceeds its max_plan_events bound")

    selection = exact_mapping(
        plan.get("selection"),
        frozenset(
            {"candidates_examined", "classification_counts", "retryable_events"}
        ),
        "retry plan selection",
    )
    if selection.get("candidates_examined") != len(events):
        raise ValidationError("retry plan candidates_examined does not reconcile")
    if selection.get("retryable_events") != len(retryable_ids):
        raise ValidationError("retry plan retryable_events does not reconcile")
    counts = exact_mapping(
        selection.get("classification_counts"),
        frozenset(CLASSIFICATIONS),
        "retry plan classification_counts",
    )
    for classification in CLASSIFICATIONS:
        expected_count = sum(
            event["classification"] == classification for event in events
        )
        if counts.get(classification) != expected_count:
            raise ValidationError(
                f"retry plan classification count for {classification} does not reconcile"
            )

    for side_effect_field in (
        "delivery_performed",
        "delivery_attempt_written",
        "dead_letter_mutated",
        "external_request_performed",
    ):
        if plan.get(side_effect_field) is not False:
            raise ValidationError(f"retry plan {side_effect_field} must be false")

    identity = {
        "channel": plan["channel"],
        "delivery_config_fingerprint": delivery["fingerprint"],
        "events": events,
        "filters": dict(filters),
        "model_version": plan["model_version"],
        "planned_at": planned_at.isoformat(),
        "retry_policy_fingerprint": retry_policy["fingerprint"],
    }
    if plan_id != _plan_id(identity):
        raise ValidationError("retry plan plan_id does not match canonical evidence")
    return dict(plan)


def load_retry_plan(path: Path) -> dict[str, Any]:
    try:
        if path.is_symlink():
            raise ValidationError("retry plan must not be a symbolic link")
        if not path.exists() or not path.is_file():
            raise ValidationError("retry plan must be one regular JSON file")
        if path.stat().st_size > MAX_PLAN_FILE_BYTES:
            raise ValidationError("retry plan exceeds the 1 MB file bound")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except ValidationError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError):
        raise ValidationError("retry plan could not be read as JSON") from None
    return validate_retry_plan_document(payload)


def _event_revalidation_identity(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: event[key]
        for key in sorted(PLAN_EVENT_FIELDS - {"event_age_seconds"})
    }


def assert_retry_plan_is_current(
    retained_plan: Mapping[str, Any],
    current_plan: Mapping[str, Any],
) -> None:
    current_delivery = cast(Mapping[str, Any], current_plan.get("delivery_config"))
    retained_delivery = cast(Mapping[str, Any], retained_plan.get("delivery_config"))
    if current_delivery.get("fingerprint") != retained_delivery.get("fingerprint"):
        raise ValidationError("delivery configuration changed after retry planning")
    current_policy = cast(Mapping[str, Any], current_plan.get("retry_policy"))
    retained_policy = cast(Mapping[str, Any], retained_plan.get("retry_policy"))
    if current_policy.get("fingerprint") != retained_policy.get("fingerprint"):
        raise ValidationError("retry policy changed after retry planning")
    if current_plan.get("filters") != retained_plan.get("filters"):
        raise ValidationError("retry plan filters changed during revalidation")

    retained_events = cast(list[Mapping[str, Any]], retained_plan.get("events"))
    current_events = cast(list[Mapping[str, Any]], current_plan.get("events"))
    retained_ids = [cast(str, event["event_id"]) for event in retained_events]
    current_ids = [cast(str, event["event_id"]) for event in current_events]
    if current_ids != retained_ids:
        raise ValidationError("notification event set changed after retry planning")
    for retained, current in zip(retained_events, current_events, strict=True):
        if _event_revalidation_identity(current) != _event_revalidation_identity(
            retained
        ):
            raise ValidationError(
                f"notification retry evidence changed for event {retained['event_id']}"
            )
    if current_plan.get("retryable_event_ids") != retained_plan.get(
        "retryable_event_ids"
    ):
        raise ValidationError("notification retry eligibility changed after planning")
