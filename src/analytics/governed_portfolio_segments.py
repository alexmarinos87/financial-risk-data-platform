from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any

from ..common.exceptions import ValidationError
from .portfolio_mandates import MAX_MANDATES, PortfolioMandate
from .portfolio_risk import TRADING_DAYS_PER_YEAR
from .portfolio_risk_limit_policies import (
    MAX_POLICY_VERSIONS,
    EffectiveDatedPortfolioRiskLimitPolicy,
)

MODEL_VERSION = "governed-portfolio-segments-v1"
MAX_GOVERNED_SEGMENTS = 100


@dataclass(frozen=True, slots=True)
class GovernedPortfolioSegment:
    start_date: date
    end_date: date
    mandate: PortfolioMandate
    policy: EffectiveDatedPortfolioRiskLimitPolicy

    @property
    def calendar_days(self) -> int:
        return (self.end_date - self.start_date).days + 1

    @property
    def segment_id(self) -> str:
        payload = {
            "end_date": self.end_date.isoformat(),
            "mandate_fingerprint": self.mandate.fingerprint,
            "model_version": MODEL_VERSION,
            "policy_fingerprint": self.policy.fingerprint,
            "start_date": self.start_date.isoformat(),
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"governed-segment-{digest}"


@dataclass(frozen=True, slots=True)
class GovernedPortfolioSegmentPlan:
    segments: tuple[GovernedPortfolioSegment, ...]
    diagnostics: Mapping[str, Any]


def _calendar_date(value: Any, label: str) -> date:
    if isinstance(value, datetime) or not isinstance(value, date):
        raise ValidationError(f"{label} must be a calendar date")
    return value


def _bounded_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


def _validate_mandates(
    mandates: Sequence[PortfolioMandate],
    *,
    portfolio_id: str,
) -> tuple[PortfolioMandate, ...]:
    if (
        isinstance(mandates, (str, bytes))
        or not 1 <= len(mandates) <= MAX_MANDATES
        or any(not isinstance(item, PortfolioMandate) for item in mandates)
    ):
        raise ValidationError(
            "portfolio mandates must contain bounded PortfolioMandate entries"
        )
    ordered = tuple(
        sorted(mandates, key=lambda item: (item.effective_from, item.mandate_id))
    )
    if any(item.portfolio_id != portfolio_id for item in ordered):
        raise ValidationError(
            "all portfolio mandates must match the requested portfolio"
        )
    if len({item.mandate_id for item in ordered}) != len(ordered):
        raise ValidationError("portfolio mandate IDs must be unique")
    for previous, current in zip(ordered, ordered[1:], strict=False):
        if previous.effective_to is None:
            raise ValidationError(
                "an open-ended portfolio mandate must be the final mandate"
            )
        if current.effective_from < previous.effective_to:
            raise ValidationError("portfolio mandates must not overlap")
    return ordered


def _validate_policies(
    policies: Sequence[EffectiveDatedPortfolioRiskLimitPolicy],
    *,
    policy_id: str,
    portfolio_id: str,
) -> tuple[EffectiveDatedPortfolioRiskLimitPolicy, ...]:
    if (
        isinstance(policies, (str, bytes))
        or not 1 <= len(policies) <= MAX_POLICY_VERSIONS
        or any(
            not isinstance(item, EffectiveDatedPortfolioRiskLimitPolicy)
            for item in policies
        )
    ):
        raise ValidationError(
            "risk-limit policies must contain bounded effective-dated entries"
        )
    ordered = tuple(
        sorted(
            policies,
            key=lambda item: (item.effective_from, item.policy_version_id),
        )
    )
    if any(item.policy_id != policy_id for item in ordered):
        raise ValidationError(
            "all risk-limit policy versions must match the requested policy"
        )
    if any(item.portfolio_id != portfolio_id for item in ordered):
        raise ValidationError(
            "all risk-limit policy versions must match the requested portfolio"
        )
    if len({item.policy_version_id for item in ordered}) != len(ordered):
        raise ValidationError("risk-limit policy version IDs must be unique")
    for previous, current in zip(ordered, ordered[1:], strict=False):
        if previous.effective_to is None:
            raise ValidationError(
                "an open-ended risk-limit policy version must be final"
            )
        if current.effective_from < previous.effective_to:
            raise ValidationError("risk-limit policy versions must not overlap")
    return ordered


def _inclusive_end(effective_to: date | None, request_end: date) -> date:
    if effective_to is None:
        return request_end
    return min(request_end, effective_to - timedelta(days=1))


def _plan_id(
    *,
    portfolio_id: str,
    policy_id: str,
    start_date: date,
    end_date: date,
    segment_ids: list[str],
) -> str:
    payload = {
        "end_date": end_date.isoformat(),
        "model_version": MODEL_VERSION,
        "policy_id": policy_id,
        "portfolio_id": portfolio_id,
        "segment_ids": segment_ids,
        "start_date": start_date.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()[:24]
    return f"governed-plan-{digest}"


def plan_governed_portfolio_segments(
    mandates: Sequence[PortfolioMandate],
    policies: Sequence[EffectiveDatedPortfolioRiskLimitPolicy],
    *,
    portfolio_id: str,
    policy_id: str,
    start_date: date,
    end_date: date,
    covariance_window: int,
    annualization_days: int = TRADING_DAYS_PER_YEAR,
    max_segments: int = MAX_GOVERNED_SEGMENTS,
) -> GovernedPortfolioSegmentPlan:
    start_date = _calendar_date(start_date, "start_date")
    end_date = _calendar_date(end_date, "end_date")
    if start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    if not isinstance(portfolio_id, str) or not portfolio_id.strip():
        raise ValidationError("portfolio_id must be non-empty text")
    if not isinstance(policy_id, str) or not policy_id.strip():
        raise ValidationError("policy_id must be non-empty text")
    if type(covariance_window) is not int or covariance_window < 2:
        raise ValidationError("covariance_window must be an integer of at least 2")
    if type(annualization_days) is not int or annualization_days <= 0:
        raise ValidationError("annualization_days must be a positive integer")
    max_segments = _bounded_integer(
        max_segments,
        "max_segments",
        MAX_GOVERNED_SEGMENTS,
    )

    ordered_mandates = _validate_mandates(
        mandates,
        portfolio_id=portfolio_id,
    )
    ordered_policies = _validate_policies(
        policies,
        policy_id=policy_id,
        portfolio_id=portfolio_id,
    )

    segments: list[GovernedPortfolioSegment] = []
    for mandate in ordered_mandates:
        mandate_end = _inclusive_end(mandate.effective_to, end_date)
        for policy in ordered_policies:
            segment_start = max(
                start_date,
                mandate.effective_from,
                policy.effective_from,
            )
            segment_end = min(
                end_date,
                mandate_end,
                _inclusive_end(policy.effective_to, end_date),
            )
            if segment_start > segment_end:
                continue
            if policy.covariance_window != covariance_window:
                raise ValidationError(
                    "requested covariance_window does not match policy version "
                    f"'{policy.policy_version_id}'"
                )
            if policy.annualization_days != annualization_days:
                raise ValidationError(
                    "requested annualization_days does not match policy version "
                    f"'{policy.policy_version_id}'"
                )
            segments.append(
                GovernedPortfolioSegment(
                    start_date=segment_start,
                    end_date=segment_end,
                    mandate=mandate,
                    policy=policy,
                )
            )

    segments.sort(
        key=lambda item: (
            item.start_date,
            item.end_date,
            item.mandate.mandate_id,
            item.policy.policy_version_id,
        )
    )
    if not segments:
        raise ValidationError(
            "requested range is not covered by any mandate-policy intersection"
        )
    if len(segments) > max_segments:
        raise ValidationError(
            "governed portfolio plan exceeds max_segments; split the request"
        )

    expected_start = start_date
    for segment in segments:
        if segment.start_date < expected_start:
            raise ValidationError(
                "governed portfolio segments overlap within the requested range"
            )
        if segment.start_date > expected_start:
            raise ValidationError(
                "requested range contains a mandate or policy coverage gap at "
                f"{expected_start.isoformat()}"
            )
        if segment.end_date == end_date:
            expected_start = end_date
            break
        expected_start = segment.end_date + timedelta(days=1)
    if segments[-1].end_date != end_date:
        raise ValidationError(
            "requested range contains a mandate or policy coverage gap at "
            f"{expected_start.isoformat()}"
        )

    segment_ids = [segment.segment_id for segment in segments]
    diagnostics = {
        "annualization_days": annualization_days,
        "calendar_days": (end_date - start_date).days + 1,
        "covariance_window": covariance_window,
        "end_date": end_date.isoformat(),
        "mandates_used": len(
            {segment.mandate.fingerprint for segment in segments}
        ),
        "model_version": MODEL_VERSION,
        "plan_id": _plan_id(
            portfolio_id=portfolio_id,
            policy_id=policy_id,
            start_date=start_date,
            end_date=end_date,
            segment_ids=segment_ids,
        ),
        "policy_id": policy_id,
        "policy_versions_used": len(
            {segment.policy.fingerprint for segment in segments}
        ),
        "portfolio_id": portfolio_id,
        "segment_count": len(segments),
        "start_date": start_date.isoformat(),
    }
    return GovernedPortfolioSegmentPlan(
        segments=tuple(segments),
        diagnostics=diagnostics,
    )
