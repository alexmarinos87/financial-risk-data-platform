from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import ValidationError

MODEL_VERSION = "operational-service-levels-v1"
METRIC_NAMES = (
    "schedule_lag_sessions",
    "market_freshness_exception_count",
    "notification_retry_exhausted_count",
    "notification_oldest_dead_letter_age_seconds",
)
STATUS_RANK = {"ok": 0, "warning": 1, "critical": 2}
MAX_POLICY_ID_LENGTH = 128
MAX_NOTIFICATION_ROWS = 5_000
MAX_FRESHNESS_ROWS = 1_000


@dataclass(frozen=True, slots=True)
class ServiceLevelThreshold:
    warning: float
    critical: float


@dataclass(frozen=True, slots=True)
class OperationalServiceLevelPolicy:
    policy_id: str
    schedule_id: str
    metrics: Mapping[str, ServiceLevelThreshold]

    @property
    def fingerprint(self) -> str:
        payload = {
            "metrics": {
                name: {
                    "critical": self.metrics[name].critical,
                    "warning": self.metrics[name].warning,
                }
                for name in METRIC_NAMES
            },
            "model_version": MODEL_VERSION,
            "policy_id": self.policy_id,
            "schedule_id": self.schedule_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"operational-slo-policy-{digest}"


@dataclass(frozen=True, slots=True)
class OperationalServiceLevelOutput:
    report: Mapping[str, Any]
    metrics: tuple[Mapping[str, Any], ...]


def _required_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if (
        len(parsed) > MAX_POLICY_ID_LENGTH
        or parsed in {".", ".."}
        or "/" in parsed
        or "\\" in parsed
        or any(ord(character) < 32 for character in parsed)
    ):
        raise ValidationError(f"{label} must be one safe text segment")
    return parsed


def _finite_nonnegative(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def _parse_threshold(value: Any, label: str) -> ServiceLevelThreshold:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    warning = _finite_nonnegative(value.get("warning"), f"{label}.warning")
    critical = _finite_nonnegative(value.get("critical"), f"{label}.critical")
    if critical <= warning:
        raise ValidationError(f"{label}.critical must be greater than warning")
    return ServiceLevelThreshold(warning=warning, critical=critical)


def parse_operational_service_level_policy(
    payload: Mapping[str, Any],
    policy_id: str,
) -> OperationalServiceLevelPolicy:
    if not isinstance(payload, Mapping):
        raise ValidationError("operational service-level configuration must be a mapping")
    policy_id = _required_segment(policy_id, "policy_id")
    policies = payload.get("policies")
    if not isinstance(policies, Mapping):
        raise ValidationError(
            "operational service-level configuration must define policies"
        )
    candidate = policies.get(policy_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(
            f"operational service-level policy '{policy_id}' is not configured"
        )
    metrics = candidate.get("metrics")
    if not isinstance(metrics, Mapping):
        raise ValidationError("operational service-level policy must define metrics")
    if set(metrics) != set(METRIC_NAMES):
        raise ValidationError(
            "operational service-level metrics must match the supported metric set"
        )
    return OperationalServiceLevelPolicy(
        policy_id=policy_id,
        schedule_id=_required_segment(candidate.get("schedule_id"), "schedule_id"),
        metrics={
            name: _parse_threshold(metrics[name], f"metrics.{name}")
            for name in METRIC_NAMES
        },
    )


def load_operational_service_level_policy(
    path: Path,
    policy_id: str,
) -> OperationalServiceLevelPolicy:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "operational service-level configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational service-level configuration must be a mapping")
    return parse_operational_service_level_policy(payload, policy_id)


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be an ISO-8601 timestamp") from None
    else:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    return parsed.astimezone(timezone.utc)


def _calendar_date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if parsed.isoformat() != value.strip():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def _status(value: float | None, threshold: ServiceLevelThreshold) -> str:
    if value is None:
        return "critical"
    if value >= threshold.critical:
        return "critical"
    if value >= threshold.warning:
        return "warning"
    return "ok"


def _metric(
    *,
    name: str,
    value: float | None,
    threshold: ServiceLevelThreshold,
    unit: str,
    reason: str | None = None,
) -> dict[str, Any]:
    status = _status(value, threshold)
    return {
        "metric_name": name,
        "observed_value": value,
        "unit": unit,
        "warning_threshold": threshold.warning,
        "critical_threshold": threshold.critical,
        "status": status,
        "reason": reason,
    }


def _freshness_exception_count(
    records: Iterable[Mapping[str, Any]],
    *,
    expected_constituents: tuple[str, ...],
    calendar_id: str,
    latest_expected_session: date,
) -> tuple[int, tuple[str, ...]]:
    expected = set(expected_constituents)
    seen: dict[str, Mapping[str, Any]] = {}
    record_count = 0
    for record in records:
        record_count += 1
        if record_count > MAX_FRESHNESS_ROWS:
            raise ValidationError("market freshness evidence exceeds the row limit")
        if not isinstance(record, Mapping):
            raise ValidationError("market freshness evidence must contain mappings")
        source = _required_segment(record.get("source"), "freshness source")
        symbol = _required_segment(record.get("symbol"), "freshness symbol")
        key = f"{source}:{symbol}"
        if key not in expected:
            continue
        if key in seen:
            raise ValidationError("market freshness evidence contains duplicate constituents")
        if _required_segment(record.get("calendar_id"), "freshness calendar_id") != calendar_id:
            raise ValidationError("market freshness evidence uses another calendar")
        seen[key] = record

    exceptions: list[str] = []
    for key in sorted(expected):
        record = seen.get(key)
        if record is None:
            exceptions.append(f"{key}:missing")
            continue
        status = record.get("freshness_status")
        if status not in {"current", "gap_detected", "stale"}:
            raise ValidationError("market freshness status is invalid")
        as_of_date = _calendar_date(record.get("as_of_date"), "freshness as_of_date")
        trailing = record.get("trailing_missing_session_count")
        if type(trailing) is not int or trailing < 0:
            raise ValidationError(
                "trailing_missing_session_count must be a non-negative integer"
            )
        if (
            status != "current"
            or as_of_date != latest_expected_session
            or trailing != 0
        ):
            exceptions.append(f"{key}:{status}")
    return len(exceptions), tuple(exceptions)


def _notification_metrics(
    records: Iterable[Mapping[str, Any]],
    *,
    as_of: datetime,
    maximum_attempts: int,
) -> tuple[int, float, tuple[str, ...]]:
    if type(maximum_attempts) is not int or not 1 <= maximum_attempts <= 10:
        raise ValidationError("maximum_attempts must be an integer between 1 and 10")
    seen: set[str] = set()
    exhausted: list[tuple[str, float]] = []
    count = 0
    for record in records:
        count += 1
        if count > MAX_NOTIFICATION_ROWS:
            raise ValidationError("notification delivery evidence exceeds the row limit")
        if not isinstance(record, Mapping):
            raise ValidationError("notification delivery evidence must contain mappings")
        event_id = _required_segment(record.get("event_id"), "notification event_id")
        if event_id in seen:
            raise ValidationError("notification delivery evidence contains duplicate events")
        seen.add(event_id)
        delivered = record.get("delivered")
        if type(delivered) is not bool:
            raise ValidationError("notification delivered must be boolean")
        attempt_count = record.get("attempt_count")
        if type(attempt_count) is not int or not 0 <= attempt_count <= 10:
            raise ValidationError("notification attempt_count must be between 0 and 10")
        ts_event = _aware_utc(record.get("ts_event"), "notification ts_event")
        last_attempt = record.get("last_attempted_at")
        reference = (
            _aware_utc(last_attempt, "notification last_attempted_at")
            if last_attempt is not None
            else ts_event
        )
        if reference > as_of:
            raise ValidationError("notification evidence must not be in the future")
        if not delivered and attempt_count >= maximum_attempts:
            exhausted.append((event_id, (as_of - reference).total_seconds()))
    oldest_age = max((age for _, age in exhausted), default=0.0)
    return len(exhausted), oldest_age, tuple(event for event, _ in exhausted)


def evaluate_operational_service_levels(
    *,
    policy: OperationalServiceLevelPolicy,
    as_of: datetime,
    schedule_fingerprint: str,
    latest_expected_session: date,
    schedule_checkpoint: date | None,
    schedule_lag_sessions: int | None,
    expected_constituents: tuple[str, ...],
    calendar_id: str,
    freshness_records: Iterable[Mapping[str, Any]],
    notification_records: Iterable[Mapping[str, Any]],
    maximum_notification_attempts: int,
) -> OperationalServiceLevelOutput:
    as_of = _aware_utc(as_of, "as_of")
    schedule_fingerprint = _required_segment(
        schedule_fingerprint,
        "schedule_fingerprint",
    )
    calendar_id = _required_segment(calendar_id, "calendar_id")
    if not expected_constituents or len(set(expected_constituents)) != len(
        expected_constituents
    ):
        raise ValidationError("expected_constituents must be a unique non-empty tuple")
    for key in expected_constituents:
        if not isinstance(key, str) or ":" not in key:
            raise ValidationError("expected constituent keys must use source:symbol")
    if schedule_lag_sessions is not None and (
        type(schedule_lag_sessions) is not int or schedule_lag_sessions < 0
    ):
        raise ValidationError("schedule_lag_sessions must be a non-negative integer")
    if schedule_checkpoint is not None and schedule_checkpoint > latest_expected_session:
        raise ValidationError("schedule checkpoint is after the latest expected session")

    freshness_count, freshness_exceptions = _freshness_exception_count(
        freshness_records,
        expected_constituents=expected_constituents,
        calendar_id=calendar_id,
        latest_expected_session=latest_expected_session,
    )
    exhausted_count, oldest_dead_letter_age, exhausted_events = _notification_metrics(
        notification_records,
        as_of=as_of,
        maximum_attempts=maximum_notification_attempts,
    )
    metrics = (
        _metric(
            name="schedule_lag_sessions",
            value=(
                float(schedule_lag_sessions)
                if schedule_lag_sessions is not None
                else None
            ),
            threshold=policy.metrics["schedule_lag_sessions"],
            unit="sessions",
            reason=("checkpoint_missing" if schedule_checkpoint is None else None),
        ),
        _metric(
            name="market_freshness_exception_count",
            value=float(freshness_count),
            threshold=policy.metrics["market_freshness_exception_count"],
            unit="constituents",
        ),
        _metric(
            name="notification_retry_exhausted_count",
            value=float(exhausted_count),
            threshold=policy.metrics["notification_retry_exhausted_count"],
            unit="events",
        ),
        _metric(
            name="notification_oldest_dead_letter_age_seconds",
            value=oldest_dead_letter_age,
            threshold=policy.metrics[
                "notification_oldest_dead_letter_age_seconds"
            ],
            unit="seconds",
        ),
    )
    overall_status = max(
        (str(metric["status"]) for metric in metrics),
        key=STATUS_RANK.__getitem__,
    )
    identity_payload = {
        "as_of": as_of.isoformat(),
        "calendar_id": calendar_id,
        "expected_constituents": sorted(expected_constituents),
        "freshness_exceptions": list(freshness_exceptions),
        "latest_expected_session": latest_expected_session.isoformat(),
        "maximum_notification_attempts": maximum_notification_attempts,
        "metrics": list(metrics),
        "model_version": MODEL_VERSION,
        "notification_retry_exhausted_events": list(exhausted_events),
        "policy_fingerprint": policy.fingerprint,
        "schedule_checkpoint": (
            schedule_checkpoint.isoformat() if schedule_checkpoint is not None else None
        ),
        "schedule_fingerprint": schedule_fingerprint,
    }
    digest = hashlib.sha256(
        json.dumps(
            identity_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    report = {
        "calculation_id": f"{MODEL_VERSION}-report-{digest}",
        "model_version": MODEL_VERSION,
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "schedule_id": policy.schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "as_of": as_of.isoformat(),
        "latest_expected_session": latest_expected_session.isoformat(),
        "schedule_checkpoint": (
            schedule_checkpoint.isoformat() if schedule_checkpoint is not None else None
        ),
        "expected_constituent_count": len(expected_constituents),
        "freshness_exceptions": list(freshness_exceptions),
        "notification_retry_exhausted_events": list(exhausted_events),
        "maximum_notification_attempts": maximum_notification_attempts,
        "overall_status": overall_status,
        "metrics": list(metrics),
    }
    return OperationalServiceLevelOutput(report=report, metrics=metrics)
