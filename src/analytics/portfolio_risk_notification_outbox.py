from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any, TypeAlias

from ..common.exceptions import ValidationError
from .portfolio_risk_limits import (
    CONCENTRATION_METRIC,
    MODEL_VERSION as RISK_LIMIT_MODEL_VERSION,
    VOLATILITY_METRIC,
)

MODEL_VERSION = "portfolio-risk-notification-outbox-v1"
MAX_NOTIFICATION_EVENTS = 10_000
ACTIONABLE_TRANSITIONS = frozenset(
    {"opened", "escalated", "deescalated", "resolved"}
)
PENDING_TRANSITIONS = frozenset({"opened", "escalated", "resolved"})
EVENT_TYPES = {
    "opened": "breach_opened",
    "escalated": "breach_escalated",
    "deescalated": "breach_deescalated",
    "resolved": "breach_resolved",
}

TransitionInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PortfolioRiskNotificationOutput:
    events: tuple[dict[str, Any], ...]
    diagnostics: Mapping[str, Any]


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _aware_utc(value: Any, label: str) -> datetime:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def _finite_number(
    value: Any,
    label: str,
    *,
    minimum: float | None = None,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValidationError(f"{label} must be a finite number")
    if minimum is not None and parsed < minimum:
        raise ValidationError(f"{label} must be at least {minimum}")
    return parsed


def _positive_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return _required_text(value, label)


def _transition_contract(
    previous_status: str | None,
    current_status: str,
    transition_type: str,
) -> None:
    if current_status not in {"ok", "warning", "critical"}:
        raise ValidationError("status is invalid")
    if previous_status is not None and previous_status not in {
        "ok",
        "warning",
        "critical",
    }:
        raise ValidationError("previous_status is invalid")
    valid = (
        transition_type == "opened"
        and previous_status in {None, "ok"}
        and current_status in {"warning", "critical"}
    ) or (
        transition_type == "escalated"
        and previous_status == "warning"
        and current_status == "critical"
    ) or (
        transition_type == "deescalated"
        and previous_status == "critical"
        and current_status == "warning"
    ) or (
        transition_type == "resolved"
        and previous_status in {"warning", "critical"}
        and current_status == "ok"
    )
    if not valid:
        raise ValidationError(
            "transition statuses do not match transition_type"
        )


def _normalise_transition(candidate: TransitionInput) -> dict[str, Any] | None:
    if not isinstance(candidate, Mapping):
        raise ValidationError(
            "notification input must contain transition mappings"
        )
    transition_type = _required_text(
        candidate.get("transition_type"),
        "transition_type",
    )
    if transition_type not in ACTIONABLE_TRANSITIONS:
        if transition_type in {"initial_ok", "unchanged"}:
            return None
        raise ValidationError("transition_type is invalid")

    previous_status = _optional_text(
        candidate.get("previous_status"),
        "previous_status",
    )
    current_status = _required_text(candidate.get("status"), "status")
    _transition_contract(previous_status, current_status, transition_type)

    event_ts = _aware_utc(candidate.get("ts_event"), "ts_event")
    ingest_ts = _aware_utc(candidate.get("ts_ingest"), "ts_ingest")
    if ingest_ts < event_ts:
        raise ValidationError("ts_ingest must be on or after ts_event")

    metric_name = _required_text(candidate.get("metric_name"), "metric_name")
    subject_type = _required_text(
        candidate.get("subject_type"),
        "subject_type",
    )
    subject_key = _required_text(candidate.get("subject_key"), "subject_key")
    unit = _required_text(candidate.get("unit"), "unit")
    observed_value = _finite_number(
        candidate.get("observed_value"),
        "observed_value",
        minimum=0.0,
    )
    observed_signed_value = _finite_number(
        candidate.get("observed_signed_value"),
        "observed_signed_value",
    )
    if metric_name == VOLATILITY_METRIC:
        if (
            subject_type != "portfolio"
            or unit != "annualized_decimal"
            or not math.isclose(
                observed_signed_value,
                observed_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise ValidationError(
                "portfolio volatility transition has invalid subject evidence"
            )
    elif metric_name == CONCENTRATION_METRIC:
        if (
            subject_type != "constituent"
            or unit != "absolute_share"
            or not math.isclose(
                abs(observed_signed_value),
                observed_value,
                rel_tol=0.0,
                abs_tol=1e-12,
            )
        ):
            raise ValidationError(
                "component concentration transition has invalid subject evidence"
            )
    else:
        raise ValidationError("metric_name is unsupported")

    warning_threshold = _finite_number(
        candidate.get("warning_threshold"),
        "warning_threshold",
        minimum=0.0,
    )
    critical_threshold = _finite_number(
        candidate.get("critical_threshold"),
        "critical_threshold",
        minimum=0.0,
    )
    if warning_threshold <= 0 or critical_threshold <= warning_threshold:
        raise ValidationError(
            "thresholds must satisfy 0 < warning < critical"
        )

    breach_excess = _finite_number(
        candidate.get("breach_excess"),
        "breach_excess",
        minimum=0.0,
    )
    severity_rank = candidate.get("severity_rank")
    expected_rank = {"ok": 0, "warning": 1, "critical": 2}[current_status]
    if type(severity_rank) is not int or severity_rank != expected_rank:
        raise ValidationError("severity_rank does not match status")

    subject_changed = candidate.get("subject_changed")
    if type(subject_changed) is not bool:
        raise ValidationError("subject_changed must be boolean")
    previous_subject_key = _optional_text(
        candidate.get("previous_subject_key"),
        "previous_subject_key",
    )
    if subject_changed != (
        previous_subject_key is not None
        and previous_subject_key != subject_key
    ):
        raise ValidationError(
            "subject_changed does not match subject evidence"
        )

    covariance_window = candidate.get("covariance_window")
    annualization_days = candidate.get("annualization_days")
    if type(covariance_window) is not int or covariance_window < 2:
        raise ValidationError(
            "covariance_window must be an integer of at least 2"
        )
    if type(annualization_days) is not int or annualization_days < 1:
        raise ValidationError(
            "annualization_days must be a positive integer"
        )

    risk_limit_model_version = _required_text(
        candidate.get("model_version"),
        "risk-limit model_version",
    )
    if risk_limit_model_version != RISK_LIMIT_MODEL_VERSION:
        raise ValidationError("risk-limit model_version is unsupported")

    source_calculation_id = _required_text(
        candidate.get("calculation_id"),
        "source calculation_id",
    )
    previous_calculation_id = _optional_text(
        candidate.get("previous_calculation_id"),
        "previous_calculation_id",
    )
    if previous_status is None and previous_calculation_id is not None:
        raise ValidationError(
            "previous calculation requires previous_status"
        )
    if previous_status is not None and previous_calculation_id is None:
        raise ValidationError(
            "previous_status requires previous calculation"
        )

    return {
        "source_evaluation_calculation_id": source_calculation_id,
        "source_previous_evaluation_calculation_id": previous_calculation_id,
        "risk_limit_model_version": risk_limit_model_version,
        "policy_id": _required_text(candidate.get("policy_id"), "policy_id"),
        "policy_fingerprint": _required_text(
            candidate.get("policy_fingerprint"),
            "policy_fingerprint",
        ),
        "portfolio_id": _required_text(
            candidate.get("portfolio_id"),
            "portfolio_id",
        ),
        "base_currency": _required_text(
            candidate.get("base_currency"),
            "base_currency",
        ),
        "definition_fingerprint": _required_text(
            candidate.get("definition_fingerprint"),
            "definition_fingerprint",
        ),
        "attribution_model_version": _required_text(
            candidate.get("attribution_model_version"),
            "attribution_model_version",
        ),
        "weighting_method": _required_text(
            candidate.get("weighting_method"),
            "weighting_method",
        ),
        "covariance_method": _required_text(
            candidate.get("covariance_method"),
            "covariance_method",
        ),
        "correlation_method": _required_text(
            candidate.get("correlation_method"),
            "correlation_method",
        ),
        "covariance_window": covariance_window,
        "annualization_days": annualization_days,
        "ts_event": event_ts,
        "ts_ingest": ingest_ts,
        "metric_name": metric_name,
        "subject_type": subject_type,
        "subject_key": subject_key,
        "previous_subject_key": previous_subject_key,
        "subject_changed": subject_changed,
        "unit": unit,
        "previous_status": previous_status,
        "current_status": current_status,
        "severity_rank": severity_rank,
        "observed_value": observed_value,
        "observed_signed_value": observed_signed_value,
        "warning_threshold": warning_threshold,
        "critical_threshold": critical_threshold,
        "breach_excess": breach_excess,
        "transition_type": transition_type,
    }


def _event_id(record: Mapping[str, Any]) -> str:
    payload = {
        "metric_name": record["metric_name"],
        "model_version": MODEL_VERSION,
        "policy_fingerprint": record["policy_fingerprint"],
        "source_evaluation_calculation_id": record[
            "source_evaluation_calculation_id"
        ],
        "source_previous_evaluation_calculation_id": record[
            "source_previous_evaluation_calculation_id"
        ],
        "transition_type": record["transition_type"],
    }
    digest = hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-event-{digest}"


def _payload(record: Mapping[str, Any], event_id: str) -> str:
    payload = {
        "event_id": event_id,
        "event_type": EVENT_TYPES[record["transition_type"]],
        "transition_type": record["transition_type"],
        "policy": {
            "policy_id": record["policy_id"],
            "policy_fingerprint": record["policy_fingerprint"],
        },
        "portfolio": {
            "portfolio_id": record["portfolio_id"],
            "definition_fingerprint": record["definition_fingerprint"],
            "base_currency": record["base_currency"],
        },
        "metric": {
            "name": record["metric_name"],
            "subject_type": record["subject_type"],
            "subject_key": record["subject_key"],
            "previous_subject_key": record["previous_subject_key"],
            "subject_changed": record["subject_changed"],
            "unit": record["unit"],
            "previous_status": record["previous_status"],
            "current_status": record["current_status"],
            "observed_value": record["observed_value"],
            "observed_signed_value": record["observed_signed_value"],
            "warning_threshold": record["warning_threshold"],
            "critical_threshold": record["critical_threshold"],
            "breach_excess": record["breach_excess"],
        },
        "source": {
            "evaluation_calculation_id": record[
                "source_evaluation_calculation_id"
            ],
            "previous_evaluation_calculation_id": record[
                "source_previous_evaluation_calculation_id"
            ],
            "ts_event": record["ts_event"].isoformat(),
            "ts_ingest": record["ts_ingest"].isoformat(),
        },
    }
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _event_record(record: Mapping[str, Any]) -> dict[str, Any]:
    event_id = _event_id(record)
    pending = record["transition_type"] in PENDING_TRANSITIONS
    return {
        "event_id": event_id,
        "model_version": MODEL_VERSION,
        "event_type": EVENT_TYPES[record["transition_type"]],
        "transition_type": record["transition_type"],
        "delivery_disposition": "pending" if pending else "suppressed",
        "suppression_reason": (
            None if pending else "deescalation_not_routed"
        ),
        **dict(record),
        "payload_json": _payload(record, event_id),
    }


def _event_signature(event: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple((key, event[key]) for key in sorted(event))


def build_portfolio_risk_notification_outbox(
    records: Iterable[TransitionInput],
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    max_events: int = MAX_NOTIFICATION_EVENTS,
) -> PortfolioRiskNotificationOutput:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    max_events = _positive_integer(
        max_events,
        "max_events",
        MAX_NOTIFICATION_EVENTS,
    )

    events_by_id: dict[str, dict[str, Any]] = {}
    signatures: dict[str, tuple[Any, ...]] = {}
    matched_transitions = 0
    skipped_non_actionable = 0
    skipped_date_range = 0
    for candidate in records:
        record = _normalise_transition(candidate)
        if record is None:
            skipped_non_actionable += 1
            continue
        matched_transitions += 1
        event_date = record["ts_event"].date()
        if (
            (start_date is not None and event_date < start_date)
            or (end_date is not None and event_date > end_date)
        ):
            skipped_date_range += 1
            continue

        event = _event_record(record)
        event_id = event["event_id"]
        signature = _event_signature(event)
        previous_signature = signatures.get(event_id)
        if previous_signature is not None:
            if previous_signature != signature:
                raise ValidationError(
                    "notification event IDs must not contain conflicting records"
                )
            continue
        signatures[event_id] = signature
        events_by_id[event_id] = event
        if len(events_by_id) > max_events:
            raise ValidationError(
                "notification outbox exceeds max_events; split the date range"
            )

    if not events_by_id:
        raise ValidationError(
            "no actionable risk-limit transitions matched the requested range"
        )

    events = tuple(
        sorted(
            events_by_id.values(),
            key=lambda item: (
                item["ts_event"],
                item["metric_name"],
                item["transition_type"],
                item["event_id"],
            ),
        )
    )
    diagnostics = {
        "matched_actionable_transitions": matched_transitions,
        "events_selected": len(events),
        "events_pending": sum(
            1
            for event in events
            if event["delivery_disposition"] == "pending"
        ),
        "events_suppressed": sum(
            1
            for event in events
            if event["delivery_disposition"] == "suppressed"
        ),
        "skipped_non_actionable_transitions": skipped_non_actionable,
        "skipped_outside_date_range": skipped_date_range,
        "event_type_counts": {
            event_type: sum(
                1
                for event in events
                if event["event_type"] == event_type
            )
            for event_type in sorted(set(EVENT_TYPES.values()))
        },
        "first_event_date": events[0]["ts_event"].date().isoformat(),
        "last_event_date": events[-1]["ts_event"].date().isoformat(),
        "max_events": max_events,
    }
    return PortfolioRiskNotificationOutput(
        events=events,
        diagnostics=diagnostics,
    )
