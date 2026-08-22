from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, TypeAlias

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .portfolio_risk_limits import (
    PortfolioRiskLimitPolicy,
    parse_portfolio_risk_limit_policy,
)

MAX_POLICY_VERSIONS = 100
POLICY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
POLICY_VERSION_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
DIRECT_POLICY_FIELDS = frozenset(
    {
        "policy_version_id",
        "effective_from",
        "effective_to",
        "portfolio_id",
        "covariance_window",
        "annualization_days",
        "limits",
    }
)

PolicyVersionInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class EffectiveDatedPortfolioRiskLimitPolicy(PortfolioRiskLimitPolicy):
    policy_version_id: str
    effective_from: date
    effective_to: date | None

    @property
    def limit_definition_fingerprint(self) -> str:
        """Identity of thresholds and analytical parameters, excluding dates."""

        base = PortfolioRiskLimitPolicy(
            policy_id=self.policy_id,
            portfolio_id=self.portfolio_id,
            covariance_window=self.covariance_window,
            annualization_days=self.annualization_days,
            portfolio_volatility=self.portfolio_volatility,
            component_concentration=self.component_concentration,
        )
        return base.fingerprint

    @property
    def fingerprint(self) -> str:
        """Identity of one effective-dated policy version."""

        payload = {
            "effective_from": self.effective_from.isoformat(),
            "effective_to": (
                self.effective_to.isoformat()
                if self.effective_to is not None
                else None
            ),
            "limit_definition_fingerprint": self.limit_definition_fingerprint,
            "policy_id": self.policy_id,
            "policy_version_id": self.policy_version_id,
            "portfolio_id": self.portfolio_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"risk-limit-policy-version-{digest}"

    def contains(self, event_date: date) -> bool:
        return self.effective_from <= event_date and (
            self.effective_to is None or event_date < self.effective_to
        )


def _identifier(value: Any, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip().lower()
    if pattern.fullmatch(parsed) is None:
        raise ValidationError(f"{label} has an invalid format")
    return parsed


def _strict_date(value: Any, label: str, *, allow_none: bool = False) -> date | None:
    if value is None and allow_none:
        return None
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a calendar date")
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        ) from None
    if value.strip() != parsed.isoformat():
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _policy_candidate(
    payload: Mapping[str, Any],
    policy_id: str,
) -> Mapping[str, Any]:
    policies = payload.get("policies")
    if not isinstance(policies, Mapping):
        raise ValidationError(
            "risk-limit configuration must define a policies mapping"
        )
    candidate = policies.get(policy_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"risk-limit policy '{policy_id}' is not configured")
    return candidate


def _build_policy(
    *,
    policy_id: str,
    raw: PolicyVersionInput,
) -> EffectiveDatedPortfolioRiskLimitPolicy:
    policy_version_id = _identifier(
        raw.get("policy_version_id"),
        "policy_version_id",
        POLICY_VERSION_ID_PATTERN,
    )
    effective_from = _strict_date(raw.get("effective_from"), "effective_from")
    effective_to = _strict_date(
        raw.get("effective_to"),
        "effective_to",
        allow_none=True,
    )
    if effective_from is None:  # pragma: no cover - required above.
        raise ValidationError("effective_from is required")
    if effective_to is not None and effective_to <= effective_from:
        raise ValidationError("effective_to must be after effective_from")

    base = parse_portfolio_risk_limit_policy(
        {
            "policies": {
                policy_id: {
                    "portfolio_id": raw.get("portfolio_id"),
                    "covariance_window": raw.get("covariance_window"),
                    "annualization_days": raw.get("annualization_days"),
                    "limits": raw.get("limits"),
                }
            }
        },
        policy_id,
    )
    return EffectiveDatedPortfolioRiskLimitPolicy(
        policy_id=base.policy_id,
        portfolio_id=base.portfolio_id,
        covariance_window=base.covariance_window,
        annualization_days=base.annualization_days,
        portfolio_volatility=base.portfolio_volatility,
        component_concentration=base.component_concentration,
        policy_version_id=policy_version_id,
        effective_from=effective_from,
        effective_to=effective_to,
    )


def parse_portfolio_risk_limit_policies(
    payload: Mapping[str, Any],
    policy_id: str,
) -> tuple[EffectiveDatedPortfolioRiskLimitPolicy, ...]:
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    canonical_policy_id = _identifier(
        policy_id,
        "policy_id",
        POLICY_ID_PATTERN,
    )
    candidate = _policy_candidate(payload, canonical_policy_id)
    raw_versions = candidate.get("versions")

    if raw_versions is None:
        raw_entries: list[PolicyVersionInput] = [candidate]
    else:
        mixed_fields = sorted(DIRECT_POLICY_FIELDS.intersection(candidate))
        if mixed_fields:
            raise ValidationError(
                "risk-limit policy must not mix direct and version-list fields"
            )
        if (
            not isinstance(raw_versions, list)
            or not 1 <= len(raw_versions) <= MAX_POLICY_VERSIONS
        ):
            raise ValidationError(
                "risk-limit policy versions must contain between 1 and "
                f"{MAX_POLICY_VERSIONS} entries"
            )
        if any(not isinstance(entry, Mapping) for entry in raw_versions):
            raise ValidationError("each risk-limit policy version must be a mapping")
        raw_entries = list(raw_versions)

    versions = tuple(
        sorted(
            (
                _build_policy(policy_id=canonical_policy_id, raw=entry)
                for entry in raw_entries
            ),
            key=lambda item: (item.effective_from, item.policy_version_id),
        )
    )
    version_ids = [item.policy_version_id for item in versions]
    if len(version_ids) != len(set(version_ids)):
        raise ValidationError("risk-limit policy version IDs must be unique")
    if len({item.portfolio_id for item in versions}) != 1:
        raise ValidationError(
            "all versions of one risk-limit policy must target the same portfolio"
        )

    for previous, current in zip(versions, versions[1:], strict=False):
        if previous.effective_to is None:
            raise ValidationError(
                "an open-ended risk-limit policy version must be final"
            )
        if current.effective_from < previous.effective_to:
            raise ValidationError("risk-limit policy versions must not overlap")
    return versions


def select_portfolio_risk_limit_policy(
    payload: Mapping[str, Any],
    policy_id: str,
    as_of_date: date,
) -> EffectiveDatedPortfolioRiskLimitPolicy:
    if isinstance(as_of_date, datetime) or not isinstance(as_of_date, date):
        raise ValidationError("as_of_date must be a calendar date")
    versions = parse_portfolio_risk_limit_policies(payload, policy_id)
    matches = [version for version in versions if version.contains(as_of_date)]
    if len(matches) != 1:
        raise ValidationError(
            f"risk-limit policy '{policy_id}' has no unique version for "
            f"{as_of_date.isoformat()}"
        )
    return matches[0]


def load_effective_portfolio_risk_limit_policy(
    path: Path,
    policy_id: str,
    as_of_date: date,
) -> EffectiveDatedPortfolioRiskLimitPolicy:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "risk-limit configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    return select_portfolio_risk_limit_policy(payload, policy_id, as_of_date)


def validate_policy_range(
    policy: EffectiveDatedPortfolioRiskLimitPolicy,
    *,
    start_date: date | None,
    end_date: date,
) -> None:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    if not policy.contains(end_date):
        raise ValidationError("end_date is outside the selected risk-limit policy")
    if start_date is not None and not policy.contains(start_date):
        raise ValidationError(
            "requested date range crosses a risk-limit policy boundary; "
            "split the request by policy version"
        )


def policy_metadata(
    policy: EffectiveDatedPortfolioRiskLimitPolicy,
) -> dict[str, Any]:
    return {
        "policy_id": policy.policy_id,
        "policy_version_id": policy.policy_version_id,
        "policy_fingerprint": policy.fingerprint,
        "limit_definition_fingerprint": policy.limit_definition_fingerprint,
        "effective_from": policy.effective_from.isoformat(),
        "effective_to": (
            policy.effective_to.isoformat()
            if policy.effective_to is not None
            else None
        ),
    }
