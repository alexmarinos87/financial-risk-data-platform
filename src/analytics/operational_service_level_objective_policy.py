from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import ValidationError

MODEL_VERSION = "operational-slo-attainment-v1"
OBJECTIVE_NAMES = (
    "schedule_completion_attainment",
    "market_freshness_attainment",
    "notification_retry_exhaustion_free_attainment",
    "notification_dead_letter_duration_attainment",
)
OBJECTIVE_SOURCE_METRICS = {
    "schedule_completion_attainment": "schedule_lag_sessions",
    "market_freshness_attainment": "market_freshness_exception_count",
    "notification_retry_exhaustion_free_attainment": (
        "notification_retry_exhausted_count"
    ),
    "notification_dead_letter_duration_attainment": (
        "notification_oldest_dead_letter_age_seconds"
    ),
}
EXPECTED_UNITS = {
    "schedule_lag_sessions": "sessions",
    "market_freshness_exception_count": "constituents",
    "notification_retry_exhausted_count": "events",
    "notification_oldest_dead_letter_age_seconds": "seconds",
}
MAX_WINDOW_SESSIONS = 10 * 252
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class ObjectiveTarget:
    source_metric_name: str
    success_threshold: float
    target_ratio: float


@dataclass(frozen=True, slots=True)
class OperationalObjectivePolicy:
    objective_policy_id: str
    operational_policy_id: str
    window_sessions: int
    minimum_observations: int
    objectives: Mapping[str, ObjectiveTarget]

    @property
    def fingerprint(self) -> str:
        payload = {
            "minimum_observations": self.minimum_observations,
            "model_version": MODEL_VERSION,
            "objective_policy_id": self.objective_policy_id,
            "objectives": {
                name: {
                    "source_metric_name": self.objectives[name].source_metric_name,
                    "success_threshold": self.objectives[name].success_threshold,
                    "target_ratio": self.objectives[name].target_ratio,
                }
                for name in OBJECTIVE_NAMES
            },
            "operational_policy_id": self.operational_policy_id,
            "window_sessions": self.window_sessions,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"operational-slo-objective-policy-{digest}"


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _nonnegative_number(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def _target(name: str, raw: Any) -> ObjectiveTarget:
    if not isinstance(raw, Mapping) or set(raw) != {
        "source_metric_name",
        "success_threshold",
        "target_ratio",
    }:
        raise ValidationError(f"targets.{name} has an invalid contract")
    metric_name = OBJECTIVE_SOURCE_METRICS[name]
    if raw.get("source_metric_name") != metric_name:
        raise ValidationError(f"targets.{name} uses the wrong source metric")
    target_ratio = _nonnegative_number(raw.get("target_ratio"), "target_ratio")
    if not 0 < target_ratio <= 1:
        raise ValidationError("target_ratio must be greater than zero and at most one")
    return ObjectiveTarget(
        source_metric_name=metric_name,
        success_threshold=_nonnegative_number(
            raw.get("success_threshold"),
            "success_threshold",
        ),
        target_ratio=target_ratio,
    )


def parse_operational_objective_policy(
    payload: Mapping[str, Any],
    objective_policy_id: str,
) -> OperationalObjectivePolicy:
    if not isinstance(payload, Mapping):
        raise ValidationError("operational objective configuration must be a mapping")
    objective_policy_id = _safe_segment(
        objective_policy_id,
        "objective_policy_id",
    )
    policies = payload.get("objective_policies")
    if not isinstance(policies, Mapping):
        raise ValidationError("objective_policies must be configured")
    raw = policies.get(objective_policy_id)
    if not isinstance(raw, Mapping) or set(raw) != {
        "operational_policy_id",
        "window_sessions",
        "minimum_observations",
        "targets",
    }:
        raise ValidationError("operational objective policy has an invalid contract")
    window_sessions = raw.get("window_sessions")
    if type(window_sessions) is not int or not 2 <= window_sessions <= MAX_WINDOW_SESSIONS:
        raise ValidationError("window_sessions is outside the supported range")
    minimum_observations = raw.get("minimum_observations")
    if (
        type(minimum_observations) is not int
        or not 2 <= minimum_observations <= window_sessions
    ):
        raise ValidationError("minimum_observations must fit inside the window")
    targets = raw.get("targets")
    if not isinstance(targets, Mapping) or tuple(targets) != OBJECTIVE_NAMES:
        raise ValidationError("targets must use the canonical objective order")
    return OperationalObjectivePolicy(
        objective_policy_id=objective_policy_id,
        operational_policy_id=_safe_segment(
            raw.get("operational_policy_id"),
            "operational_policy_id",
        ),
        window_sessions=window_sessions,
        minimum_observations=minimum_observations,
        objectives={name: _target(name, targets[name]) for name in OBJECTIVE_NAMES},
    )


def load_operational_objective_policy(
    path: Path,
    objective_policy_id: str,
) -> OperationalObjectivePolicy:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("operational objective configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational objective configuration must be a mapping")
    return parse_operational_objective_policy(payload, objective_policy_id)
