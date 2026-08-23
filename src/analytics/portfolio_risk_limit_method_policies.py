from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .portfolio_attribution import (
    CORRELATION_METHOD as SAMPLE_CORRELATION_METHOD,
    COVARIANCE_METHOD as SAMPLE_COVARIANCE_METHOD,
    MODEL_VERSION as SAMPLE_ATTRIBUTION_MODEL_VERSION,
)
from .portfolio_attribution_ewma import (
    CORRELATION_METHOD as EWMA_CORRELATION_METHOD,
    COVARIANCE_METHOD as EWMA_COVARIANCE_METHOD,
    MODEL_VERSION as EWMA_ATTRIBUTION_MODEL_VERSION,
)
from .portfolio_risk import WEIGHTING_METHOD
from .portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
    load_effective_portfolio_risk_limit_policy,
    validate_policy_range,
)
from .portfolio_risk_limits import RiskLimitThresholds

MODEL_VERSION = "portfolio-risk-limits-v2"
MAX_METHOD_POLICIES = 100
METHOD_POLICY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class AttributionMethodContract:
    attribution_model_version: str
    weighting_method: str
    covariance_method: str
    correlation_method: str

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self.attribution_model_version,
            self.weighting_method,
            self.covariance_method,
            self.correlation_method,
        )


SAMPLE_METHOD_CONTRACT = AttributionMethodContract(
    attribution_model_version=SAMPLE_ATTRIBUTION_MODEL_VERSION,
    weighting_method=WEIGHTING_METHOD,
    covariance_method=SAMPLE_COVARIANCE_METHOD,
    correlation_method=SAMPLE_CORRELATION_METHOD,
)
EWMA_METHOD_CONTRACT = AttributionMethodContract(
    attribution_model_version=EWMA_ATTRIBUTION_MODEL_VERSION,
    weighting_method=WEIGHTING_METHOD,
    covariance_method=EWMA_COVARIANCE_METHOD,
    correlation_method=EWMA_CORRELATION_METHOD,
)
SUPPORTED_METHOD_CONTRACTS = frozenset(
    {
        SAMPLE_METHOD_CONTRACT.key,
        EWMA_METHOD_CONTRACT.key,
    }
)


@dataclass(frozen=True, slots=True)
class MethodAwarePortfolioRiskLimitPolicy:
    method_policy_id: str
    base_policy: EffectiveDatedPortfolioRiskLimitPolicy
    method: AttributionMethodContract

    @property
    def policy_id(self) -> str:
        return self.base_policy.policy_id

    @property
    def policy_version_id(self) -> str:
        return self.base_policy.policy_version_id

    @property
    def portfolio_id(self) -> str:
        return self.base_policy.portfolio_id

    @property
    def covariance_window(self) -> int:
        return self.base_policy.covariance_window

    @property
    def annualization_days(self) -> int:
        return self.base_policy.annualization_days

    @property
    def portfolio_volatility(self) -> RiskLimitThresholds:
        return self.base_policy.portfolio_volatility

    @property
    def component_concentration(self) -> RiskLimitThresholds:
        return self.base_policy.component_concentration

    @property
    def effective_from(self) -> date:
        return self.base_policy.effective_from

    @property
    def effective_to(self) -> date | None:
        return self.base_policy.effective_to

    @property
    def fingerprint(self) -> str:
        payload = {
            "base_policy_fingerprint": self.base_policy.fingerprint,
            "method_policy_id": self.method_policy_id,
            "method": {
                "attribution_model_version": self.method.attribution_model_version,
                "correlation_method": self.method.correlation_method,
                "covariance_method": self.method.covariance_method,
                "weighting_method": self.method.weighting_method,
            },
            "model_version": MODEL_VERSION,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        return f"risk-limit-method-policy-{digest}"


def _identifier(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip().lower()
    if METHOD_POLICY_ID_PATTERN.fullmatch(parsed) is None:
        raise ValidationError(f"{label} has an invalid format")
    return parsed


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _method_contract(value: Any) -> AttributionMethodContract:
    if not isinstance(value, Mapping):
        raise ValidationError("method policy attribution must be a mapping")
    expected_keys = {
        "model_version",
        "weighting_method",
        "covariance_method",
        "correlation_method",
    }
    if set(value) != expected_keys:
        raise ValidationError(
            "method policy attribution must contain exactly model_version, "
            "weighting_method, covariance_method and correlation_method"
        )
    contract = AttributionMethodContract(
        attribution_model_version=_required_text(
            value.get("model_version"), "attribution.model_version"
        ),
        weighting_method=_required_text(
            value.get("weighting_method"), "attribution.weighting_method"
        ),
        covariance_method=_required_text(
            value.get("covariance_method"), "attribution.covariance_method"
        ),
        correlation_method=_required_text(
            value.get("correlation_method"), "attribution.correlation_method"
        ),
    )
    if contract.key not in SUPPORTED_METHOD_CONTRACTS:
        raise ValidationError("method policy selects an unsupported model/method contract")
    return contract


def _parse_method_bindings(
    payload: Mapping[str, Any],
) -> dict[str, tuple[str, AttributionMethodContract]]:
    method_policies = payload.get("method_policies")
    if not isinstance(method_policies, Mapping):
        raise ValidationError(
            "method policy configuration must define a method_policies mapping"
        )
    if not 1 <= len(method_policies) <= MAX_METHOD_POLICIES:
        raise ValidationError(
            f"method_policies must contain between 1 and {MAX_METHOD_POLICIES} entries"
        )

    bindings: dict[str, tuple[str, AttributionMethodContract]] = {}
    seen_contracts: set[tuple[str, tuple[str, str, str, str]]] = set()
    for raw_id, raw_candidate in method_policies.items():
        parsed_id = _identifier(raw_id, "method_policy_id")
        if parsed_id in bindings:
            raise ValidationError("method policy IDs must be unique")
        if not isinstance(raw_candidate, Mapping):
            raise ValidationError("each method policy must be a mapping")
        if set(raw_candidate) != {"base_policy_id", "attribution"}:
            raise ValidationError(
                "method policy must contain exactly base_policy_id and attribution"
            )
        base_policy_id = _identifier(
            raw_candidate.get("base_policy_id"),
            "base_policy_id",
        )
        contract = _method_contract(raw_candidate.get("attribution"))
        uniqueness_key = (base_policy_id, contract.key)
        if uniqueness_key in seen_contracts:
            raise ValidationError(
                "one base policy may define at most one binding per method contract"
            )
        seen_contracts.add(uniqueness_key)
        bindings[parsed_id] = (base_policy_id, contract)
    return bindings


def load_method_aware_portfolio_risk_limit_policy(
    *,
    method_policy_config_path: Path,
    limits_config_path: Path,
    method_policy_id: str,
    as_of_date: date,
) -> MethodAwarePortfolioRiskLimitPolicy:
    canonical_id = _identifier(method_policy_id, "method_policy_id")
    try:
        payload = load_yaml(method_policy_config_path)
    except Exception:
        raise ValidationError("method policy configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("method policy configuration must be a mapping")

    selected = _parse_method_bindings(payload).get(canonical_id)
    if selected is None:
        raise ValidationError(f"method policy '{canonical_id}' is not configured")
    base_policy_id, contract = selected
    base_policy = load_effective_portfolio_risk_limit_policy(
        limits_config_path,
        base_policy_id,
        as_of_date,
    )
    return MethodAwarePortfolioRiskLimitPolicy(
        method_policy_id=canonical_id,
        base_policy=base_policy,
        method=contract,
    )


def validate_method_policy_range(
    policy: MethodAwarePortfolioRiskLimitPolicy,
    *,
    start_date: date | None,
    end_date: date,
) -> None:
    validate_policy_range(
        policy.base_policy,
        start_date=start_date,
        end_date=end_date,
    )


def method_policy_metadata(
    policy: MethodAwarePortfolioRiskLimitPolicy,
) -> dict[str, Any]:
    return {
        "method_policy_id": policy.method_policy_id,
        "policy_id": policy.policy_id,
        "policy_version_id": policy.policy_version_id,
        "policy_fingerprint": policy.fingerprint,
        "base_policy_fingerprint": policy.base_policy.fingerprint,
        "limit_definition_fingerprint": (
            policy.base_policy.limit_definition_fingerprint
        ),
        "effective_from": policy.effective_from.isoformat(),
        "effective_to": (
            policy.effective_to.isoformat()
            if policy.effective_to is not None
            else None
        ),
        "attribution_model_version": policy.method.attribution_model_version,
        "weighting_method": policy.method.weighting_method,
        "covariance_method": policy.method.covariance_method,
        "correlation_method": policy.method.correlation_method,
    }
