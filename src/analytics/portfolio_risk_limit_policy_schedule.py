from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .portfolio_risk_limits import (
    MAX_LIMIT_EVALUATIONS,
    MODEL_VERSION as EVALUATION_MODEL_VERSION,
    AttributionInput,
    PortfolioRiskLimitOutput,
    PortfolioRiskLimitPolicy,
    _current_attributions,
    evaluate_portfolio_risk_limits,
    parse_portfolio_risk_limit_policy,
)

POLICY_SCHEDULE_MODEL_VERSION = "portfolio-risk-limit-policy-v1"
MAX_POLICY_VERSIONS = 100


@dataclass(frozen=True, slots=True)
class ScheduledRiskLimitPolicy:
    policy: PortfolioRiskLimitPolicy
    effective_from: date
    effective_to: date | None
    period_source: str = "configured"

    @property
    def fingerprint(self) -> str:
        payload = {
            "base_policy_fingerprint": self.policy.fingerprint,
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                self.effective_to.isoformat() if self.effective_to is not None else None
            ),
            "model_version": POLICY_SCHEDULE_MODEL_VERSION,
            "period_source": self.period_source,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        return f"risk-limit-policy-version-{digest}"

    def covers(self, event_date: date) -> bool:
        return self.effective_from <= event_date and (
            self.effective_to is None or event_date < self.effective_to
        )


def legacy_policy_schedule(
    policy: PortfolioRiskLimitPolicy,
) -> tuple[ScheduledRiskLimitPolicy, ...]:
    return (
        ScheduledRiskLimitPolicy(
            policy=policy,
            effective_from=date.min,
            effective_to=None,
            period_source="legacy_unbounded",
        ),
    )


def _calendar_date(value: Any, label: str) -> date:
    if type(value) is date:
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a calendar date")
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if value.strip() != parsed.isoformat():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def _policy_candidate(payload: Mapping[str, Any], policy_id: str) -> Mapping[str, Any]:
    policies = payload.get("policies")
    if not isinstance(policies, Mapping):
        raise ValidationError("risk-limit configuration must define a policies mapping")
    candidate = policies.get(policy_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"risk-limit policy '{policy_id}' is not configured")
    return candidate


def _base_payload(
    *,
    policy_id: str,
    candidate: Mapping[str, Any],
    limits: Any,
) -> dict[str, Any]:
    return {
        "policies": {
            policy_id: {
                "portfolio_id": candidate.get("portfolio_id"),
                "covariance_window": candidate.get("covariance_window"),
                "annualization_days": candidate.get("annualization_days"),
                "limits": limits,
            }
        }
    }


def parse_portfolio_risk_limit_policy_schedule(
    payload: Mapping[str, Any],
    policy_id: str,
) -> tuple[ScheduledRiskLimitPolicy, ...]:
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    candidate = _policy_candidate(payload, policy_id)
    versions = candidate.get("versions")
    if versions is None:
        return legacy_policy_schedule(
            parse_portfolio_risk_limit_policy(payload, policy_id)
        )
    if not isinstance(versions, Sequence) or isinstance(versions, (str, bytes)):
        raise ValidationError("risk-limit policy versions must be a sequence")
    if not 1 <= len(versions) <= MAX_POLICY_VERSIONS:
        raise ValidationError(
            f"risk-limit policy versions must contain between 1 and {MAX_POLICY_VERSIONS} entries"
        )

    scheduled: list[ScheduledRiskLimitPolicy] = []
    for index, version in enumerate(versions):
        if not isinstance(version, Mapping):
            raise ValidationError(f"policy version {index} must be a mapping")
        effective_from = _calendar_date(
            version.get("effective_from"),
            f"policy version {index}.effective_from",
        )
        raw_effective_to = version.get("effective_to")
        effective_to = (
            None
            if raw_effective_to in {None, ""}
            else _calendar_date(
                raw_effective_to,
                f"policy version {index}.effective_to",
            )
        )
        if effective_to is not None and effective_to <= effective_from:
            raise ValidationError("policy effective_to must be after effective_from")
        policy = parse_portfolio_risk_limit_policy(
            _base_payload(
                policy_id=policy_id,
                candidate=candidate,
                limits=version.get("limits"),
            ),
            policy_id,
        )
        scheduled.append(
            ScheduledRiskLimitPolicy(
                policy=policy,
                effective_from=effective_from,
                effective_to=effective_to,
                period_source="configured",
            )
        )

    scheduled.sort(key=lambda item: item.effective_from)
    for index, version in enumerate(scheduled):
        if version.effective_to is None and index != len(scheduled) - 1:
            raise ValidationError("only the final policy version may be open-ended")
        if index == 0:
            continue
        previous = scheduled[index - 1]
        if previous.effective_to is None:
            raise ValidationError("open-ended policy versions must be final")
        if previous.effective_to > version.effective_from:
            raise ValidationError("risk-limit policy versions must not overlap")
        if previous.effective_to < version.effective_from:
            raise ValidationError("risk-limit policy versions must be contiguous")
    return tuple(scheduled)


def load_portfolio_risk_limit_policy_schedule(
    path: Path,
    policy_id: str,
) -> tuple[ScheduledRiskLimitPolicy, ...]:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("risk-limit configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    return parse_portfolio_risk_limit_policy_schedule(payload, policy_id)


def _validate_schedule(
    schedule: Sequence[ScheduledRiskLimitPolicy],
) -> tuple[ScheduledRiskLimitPolicy, ...]:
    if not schedule:
        raise ValidationError("risk-limit policy schedule must not be empty")
    if len(schedule) > MAX_POLICY_VERSIONS:
        raise ValidationError("risk-limit policy schedule is too large")
    ordered = tuple(sorted(schedule, key=lambda item: item.effective_from))
    anchor = ordered[0].policy
    for version in ordered:
        policy = version.policy
        if (
            policy.policy_id != anchor.policy_id
            or policy.portfolio_id != anchor.portfolio_id
            or policy.covariance_window != anchor.covariance_window
            or policy.annualization_days != anchor.annualization_days
        ):
            raise ValidationError(
                "all risk-limit policy versions must share policy, portfolio, "
                "window and annualisation identity"
            )
    for index, version in enumerate(ordered):
        if version.effective_to is not None and version.effective_to <= version.effective_from:
            raise ValidationError("policy effective_to must be after effective_from")
        if version.effective_to is None and index != len(ordered) - 1:
            raise ValidationError("only the final policy version may be open-ended")
        if index:
            previous = ordered[index - 1]
            if previous.effective_to != version.effective_from:
                raise ValidationError(
                    "risk-limit policy versions must be contiguous and non-overlapping"
                )
    return ordered


def _scheduled_evaluation_id(
    *,
    policy_fingerprint: str,
    attribution_calculation_id: str,
    metric_name: str,
) -> str:
    payload = {
        "attribution_calculation_id": attribution_calculation_id,
        "metric_name": metric_name,
        "model_version": EVALUATION_MODEL_VERSION,
        "policy_fingerprint": policy_fingerprint,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{EVALUATION_MODEL_VERSION}-evaluation-{digest}"


def _apply_schedule_identity(
    evaluation: Mapping[str, Any],
    version: ScheduledRiskLimitPolicy,
) -> dict[str, Any]:
    result = dict(evaluation)
    result["policy_fingerprint"] = version.fingerprint
    result["policy_effective_from"] = version.effective_from
    result["policy_effective_to"] = version.effective_to
    result["policy_period_source"] = version.period_source
    result["calculation_id"] = _scheduled_evaluation_id(
        policy_fingerprint=version.fingerprint,
        attribution_calculation_id=str(result["attribution_calculation_id"]),
        metric_name=str(result["metric_name"]),
    )
    return result


def evaluate_portfolio_risk_limit_schedule(
    records: Iterable[AttributionInput],
    *,
    schedule: Sequence[ScheduledRiskLimitPolicy],
    definition_fingerprint: str,
    start_date: date | None = None,
    end_date: date | None = None,
    max_evaluations: int = MAX_LIMIT_EVALUATIONS,
) -> PortfolioRiskLimitOutput:
    ordered = _validate_schedule(schedule)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    if type(max_evaluations) is not int or not 1 <= max_evaluations <= MAX_LIMIT_EVALUATIONS:
        raise ValidationError(
            f"max_evaluations must be an integer between 1 and {MAX_LIMIT_EVALUATIONS}"
        )

    materialized_records = tuple(records)
    current, matched_records = _current_attributions(
        materialized_records,
        policy=ordered[0].policy,
        definition_fingerprint=definition_fingerprint,
        end_date=end_date,
    )
    selected = [
        record
        for record in current
        if start_date is None or record["ts_event"].date() >= start_date
    ]
    if not selected:
        raise ValidationError("no risk-limit evaluations matched the requested date range")
    if len(selected) * 2 > max_evaluations:
        raise ValidationError(
            "risk-limit evaluation exceeds max_evaluations; split the date range"
        )

    versions_by_fingerprint = {version.fingerprint: version for version in ordered}
    dates_by_fingerprint: dict[str, list[date]] = {}
    for record in selected:
        event_date = record["ts_event"].date()
        matches = [version for version in ordered if version.covers(event_date)]
        if len(matches) != 1:
            raise ValidationError(
                f"risk-limit policy schedule does not cover {event_date.isoformat()} exactly once"
            )
        dates_by_fingerprint.setdefault(matches[0].fingerprint, []).append(event_date)

    evaluations: list[dict[str, Any]] = []
    status_counts = {"ok": 0, "warning": 0, "critical": 0}
    for fingerprint, dates in sorted(
        dates_by_fingerprint.items(),
        key=lambda item: min(item[1]),
    ):
        version = versions_by_fingerprint[fingerprint]
        output = evaluate_portfolio_risk_limits(
            materialized_records,
            policy=version.policy,
            definition_fingerprint=definition_fingerprint,
            start_date=min(dates),
            end_date=max(dates),
            max_evaluations=max_evaluations,
        )
        selected_dates = set(dates)
        for evaluation in output.evaluations:
            if evaluation["ts_event"].date() not in selected_dates:
                continue
            scheduled_evaluation = _apply_schedule_identity(evaluation, version)
            evaluations.append(scheduled_evaluation)
            status_counts[str(scheduled_evaluation["status"])] += 1

    evaluations.sort(
        key=lambda item: (
            item["ts_event"],
            item["metric_name"],
            item["calculation_id"],
        )
    )
    if len(evaluations) != len(selected) * 2:
        raise ValidationError("risk-limit policy schedule produced incomplete evaluation evidence")

    used_versions = [
        version
        for version in ordered
        if version.fingerprint in dates_by_fingerprint
    ]
    diagnostics = {
        "policy_id": ordered[0].policy.policy_id,
        "policy_fingerprints": [version.fingerprint for version in used_versions],
        "policy_versions_used": [
            {
                "policy_fingerprint": version.fingerprint,
                "effective_from": version.effective_from.isoformat(),
                "period_source": version.period_source,
                "effective_to": (
                    version.effective_to.isoformat()
                    if version.effective_to is not None
                    else None
                ),
            }
            for version in used_versions
        ],
        "matched_input_records": matched_records,
        "current_attribution_snapshots": len(current),
        "snapshots_selected": len(selected),
        "evaluations_selected": len(evaluations),
        "status_counts": status_counts,
        "first_evaluation_date": selected[0]["ts_event"].date().isoformat(),
        "last_evaluation_date": selected[-1]["ts_event"].date().isoformat(),
        "start_date": start_date.isoformat() if start_date is not None else None,
        "end_date": (
            end_date.isoformat()
            if end_date is not None
            else selected[-1]["ts_event"].date().isoformat()
        ),
        "max_evaluations": max_evaluations,
    }
    return PortfolioRiskLimitOutput(
        evaluations=tuple(evaluations),
        diagnostics=diagnostics,
    )
