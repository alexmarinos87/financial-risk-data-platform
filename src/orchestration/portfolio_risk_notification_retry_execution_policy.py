from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.config import load_yaml
from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    CHANNEL,
    WebhookDeliveryConfig,
    parse_webhook_delivery_config,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    RetryPlanningPolicy,
    parse_retry_planning_policy,
)

EXECUTION_MODEL_VERSION = "portfolio-risk-manual-retry-execution-v1"
POLICY_MODEL_VERSION = "portfolio-risk-manual-retry-execution-policy-v1"
MAX_PLAN_FILE_BYTES = 1_048_576
MAX_PLAN_AGE_SECONDS = 24 * 60 * 60
MAX_EXECUTION_EVENTS = 100
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
ERROR_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True, slots=True)
class RetryExecutionPolicy:
    enabled: bool
    max_plan_age_seconds: int
    max_events: int

    @property
    def fingerprint(self) -> str:
        payload = {
            "channel": CHANNEL,
            "enabled": self.enabled,
            "max_events": self.max_events,
            "max_plan_age_seconds": self.max_plan_age_seconds,
            "model_version": POLICY_MODEL_VERSION,
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


def exact_mapping(
    value: Any,
    expected: frozenset[str],
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be an object")
    fields = set(value)
    if fields != expected:
        missing = sorted(expected - fields)
        unknown = sorted(fields - expected)
        raise ValidationError(
            f"{label} fields are not exact; missing={missing}, unknown={unknown}"
        )
    return value


def bounded_integer(
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


def safe_text(value: Any, label: str, *, maximum: int = 512) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValidationError(f"{label} must be non-empty canonical text")
    if len(value) > maximum or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} is invalid")
    return value


def safe_segment(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def aware_utc(value: Any, label: str) -> datetime:
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


def nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def parse_retry_execution_policy(
    payload: Mapping[str, Any],
    delivery_config: WebhookDeliveryConfig,
    retry_policy: RetryPlanningPolicy,
) -> RetryExecutionPolicy:
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    delivery = payload.get("delivery")
    if not isinstance(delivery, Mapping):
        raise ValidationError("notification delivery configuration is missing delivery")
    execution = delivery.get("retry_execution")
    if not isinstance(execution, Mapping):
        raise ValidationError(
            "notification delivery configuration is missing retry_execution"
        )
    enabled = execution.get("enabled")
    if type(enabled) is not bool:
        raise ValidationError("retry_execution enabled must be boolean")
    policy = RetryExecutionPolicy(
        enabled=enabled,
        max_plan_age_seconds=bounded_integer(
            execution.get("max_plan_age_seconds"),
            "max_plan_age_seconds",
            minimum=1,
            maximum=MAX_PLAN_AGE_SECONDS,
        ),
        max_events=bounded_integer(
            execution.get("max_events"),
            "max_events",
            minimum=1,
            maximum=MAX_EXECUTION_EVENTS,
        ),
    )
    if policy.max_events > retry_policy.max_plan_events:
        raise ValidationError("retry execution max_events exceeds retry planning limit")
    if policy.max_events > delivery_config.max_batch_events:
        raise ValidationError("retry execution max_events exceeds webhook batch limit")
    if policy.max_plan_age_seconds > retry_policy.max_event_age_seconds:
        raise ValidationError(
            "retry execution max_plan_age_seconds exceeds retry event age policy"
        )
    return policy


def load_retry_execution_contract(
    path: Path,
) -> tuple[WebhookDeliveryConfig, RetryPlanningPolicy, RetryExecutionPolicy]:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "notification delivery configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("notification delivery configuration must be a mapping")
    delivery_config = parse_webhook_delivery_config(payload)
    retry_policy = parse_retry_planning_policy(payload, delivery_config)
    execution_policy = parse_retry_execution_policy(
        payload,
        delivery_config,
        retry_policy,
    )
    return delivery_config, retry_policy, execution_policy
