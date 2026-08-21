from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, TypeAlias

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .portfolio_attribution import (
    CORRELATION_METHOD,
    COVARIANCE_METHOD,
    MODEL_VERSION as ATTRIBUTION_MODEL_VERSION,
    VARIANCE_EPSILON,
)
from .portfolio_risk import TRADING_DAYS_PER_YEAR, WEIGHTING_METHOD

MODEL_VERSION = "portfolio-risk-limits-v1"
VOLATILITY_METRIC = "portfolio_volatility_annualized"
CONCENTRATION_METRIC = "largest_absolute_component_contribution_share"
MAX_LIMIT_EVALUATIONS = 10_000
POLICY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
PORTFOLIO_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

AttributionInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RiskLimitThresholds:
    warning: float
    critical: float


@dataclass(frozen=True, slots=True)
class PortfolioRiskLimitPolicy:
    policy_id: str
    portfolio_id: str
    covariance_window: int
    annualization_days: int
    portfolio_volatility: RiskLimitThresholds
    component_concentration: RiskLimitThresholds

    @property
    def fingerprint(self) -> str:
        payload = {
            "annualization_days": self.annualization_days,
            "covariance_window": self.covariance_window,
            "limits": {
                CONCENTRATION_METRIC: {
                    "critical": self.component_concentration.critical,
                    "warning": self.component_concentration.warning,
                },
                VOLATILITY_METRIC: {
                    "critical": self.portfolio_volatility.critical,
                    "warning": self.portfolio_volatility.warning,
                },
            },
            "policy_id": self.policy_id,
            "portfolio_id": self.portfolio_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"risk-limit-policy-{digest}"


@dataclass(frozen=True, slots=True)
class PortfolioRiskLimitOutput:
    evaluations: tuple[dict[str, Any], ...]
    diagnostics: Mapping[str, Any]


def _required_identifier(
    value: Any,
    label: str,
    pattern: re.Pattern[str],
) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip()
    if pattern.fullmatch(parsed) is None:
        raise ValidationError(f"{label} has an invalid format")
    return parsed


def _positive_integer(value: Any, label: str, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 1 and {maximum}"
        )
    return value


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


def _thresholds(value: Any, label: str) -> RiskLimitThresholds:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    warning = _finite_number(value.get("warning"), f"{label}.warning", minimum=0.0)
    critical = _finite_number(
        value.get("critical"), f"{label}.critical", minimum=0.0
    )
    if warning <= 0 or critical <= warning:
        raise ValidationError(
            f"{label} must satisfy 0 < warning < critical"
        )
    return RiskLimitThresholds(warning=warning, critical=critical)


def parse_portfolio_risk_limit_policy(
    payload: Mapping[str, Any],
    policy_id: str,
) -> PortfolioRiskLimitPolicy:
    canonical_policy_id = _required_identifier(
        policy_id,
        "policy_id",
        POLICY_ID_PATTERN,
    )
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    policies = payload.get("policies")
    if not isinstance(policies, Mapping):
        raise ValidationError(
            "risk-limit configuration must define a policies mapping"
        )
    candidate = policies.get(canonical_policy_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(
            f"risk-limit policy '{canonical_policy_id}' is not configured"
        )
    limits = candidate.get("limits")
    if not isinstance(limits, Mapping):
        raise ValidationError("risk-limit policy must define a limits mapping")

    covariance_window = _positive_integer(
        candidate.get("covariance_window"),
        "covariance_window",
        10 * TRADING_DAYS_PER_YEAR,
    )
    if covariance_window < 2:
        raise ValidationError("covariance_window must be at least 2")
    annualization_days = _positive_integer(
        candidate.get("annualization_days"),
        "annualization_days",
        10 * TRADING_DAYS_PER_YEAR,
    )
    return PortfolioRiskLimitPolicy(
        policy_id=canonical_policy_id,
        portfolio_id=_required_identifier(
            candidate.get("portfolio_id"),
            "portfolio_id",
            PORTFOLIO_ID_PATTERN,
        ),
        covariance_window=covariance_window,
        annualization_days=annualization_days,
        portfolio_volatility=_thresholds(
            limits.get(VOLATILITY_METRIC),
            VOLATILITY_METRIC,
        ),
        component_concentration=_thresholds(
            limits.get(CONCENTRATION_METRIC),
            CONCENTRATION_METRIC,
        ),
    )


def load_portfolio_risk_limit_policy(
    path: Path,
    policy_id: str,
) -> PortfolioRiskLimitPolicy:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "risk-limit configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("risk-limit configuration must be a mapping")
    return parse_portfolio_risk_limit_policy(payload, policy_id)


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


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _json_number_object(value: Any, label: str) -> dict[str, float]:
    if isinstance(value, Mapping):
        parsed: Any = dict(value)
    elif isinstance(value, str):
        def reject_duplicate_keys(
            pairs: list[tuple[str, Any]],
        ) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, item in pairs:
                if key in result:
                    raise ValidationError(
                        f"{label} must not contain duplicate keys"
                    )
                result[key] = item
            return result

        try:
            parsed = json.loads(value, object_pairs_hook=reject_duplicate_keys)
        except ValidationError:
            raise
        except (TypeError, ValueError):
            raise ValidationError(f"{label} must be valid JSON") from None
    else:
        raise ValidationError(f"{label} must be a JSON object")
    if not isinstance(parsed, dict) or len(parsed) < 2:
        raise ValidationError(
            f"{label} must contain at least two constituent values"
        )

    values: dict[str, float] = {}
    for key, item in parsed.items():
        if not isinstance(key, str) or not key:
            raise ValidationError(f"{label} must use non-empty text keys")
        values[key] = _finite_number(item, f"{label}.{key}")
    return dict(sorted(values.items()))


def _normalise_attribution_record(
    candidate: AttributionInput,
    *,
    policy: PortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    end_date: date | None,
) -> dict[str, Any] | None:
    if not isinstance(candidate, Mapping):
        raise ValidationError("risk-limit input must contain attribution mappings")

    portfolio_id = candidate.get("portfolio_id")
    fingerprint = candidate.get("definition_fingerprint")
    covariance_window = candidate.get("covariance_window")
    if portfolio_id != policy.portfolio_id or fingerprint != definition_fingerprint:
        return None
    if type(covariance_window) is not int:
        raise ValidationError("covariance_window must be an integer")
    if covariance_window != policy.covariance_window:
        return None

    ts_event = _aware_utc(candidate.get("ts_event"), "ts_event")
    if ts_event.time() != time.min:
        raise ValidationError("attribution timestamps must use UTC midnight")
    if end_date is not None and ts_event.date() > end_date:
        return None
    ts_ingest = _aware_utc(candidate.get("ts_ingest"), "ts_ingest")
    if ts_ingest < ts_event:
        raise ValidationError("ts_ingest must be on or after ts_event")

    model_version = _required_text(
        candidate.get("model_version"), "attribution model_version"
    )
    weighting_method = _required_text(
        candidate.get("weighting_method"), "weighting_method"
    )
    covariance_method = _required_text(
        candidate.get("covariance_method"), "covariance_method"
    )
    correlation_method = _required_text(
        candidate.get("correlation_method"), "correlation_method"
    )
    annualization_days = candidate.get("annualization_days")
    if (
        model_version != ATTRIBUTION_MODEL_VERSION
        or weighting_method != WEIGHTING_METHOD
        or covariance_method != COVARIANCE_METHOD
        or correlation_method != CORRELATION_METHOD
        or annualization_days != policy.annualization_days
    ):
        raise ValidationError(
            "attribution record does not match the supported policy model contract"
        )

    volatility = _finite_number(
        candidate.get("portfolio_volatility_annualized"),
        "portfolio_volatility_annualized",
        minimum=0.0,
    )
    volatility_status = _required_text(
        candidate.get("volatility_status"), "volatility_status"
    )
    if volatility_status not in {"positive", "zero"}:
        raise ValidationError("volatility_status is invalid")
    if (
        volatility_status == "zero"
        and volatility > VARIANCE_EPSILON
    ) or (
        volatility_status == "positive"
        and volatility <= VARIANCE_EPSILON
    ):
        raise ValidationError(
            "volatility_status does not match portfolio volatility"
        )

    component_shares = _json_number_object(
        candidate.get("component_contribution_share_json"),
        "component_contribution_share_json",
    )
    return {
        "calculation_id": _required_text(
            candidate.get("calculation_id"), "attribution calculation_id"
        ),
        "model_version": model_version,
        "portfolio_id": policy.portfolio_id,
        "base_currency": _required_text(
            candidate.get("base_currency"), "base_currency"
        ),
        "definition_fingerprint": definition_fingerprint,
        "weighting_method": weighting_method,
        "covariance_method": covariance_method,
        "correlation_method": correlation_method,
        "covariance_window": policy.covariance_window,
        "annualization_days": policy.annualization_days,
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "portfolio_volatility_annualized": volatility,
        "component_contribution_shares": component_shares,
    }


def _record_signature(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record["model_version"],
        record["portfolio_id"],
        record["base_currency"],
        record["definition_fingerprint"],
        record["weighting_method"],
        record["covariance_method"],
        record["correlation_method"],
        record["covariance_window"],
        record["annualization_days"],
        record["ts_event"],
        record["ts_ingest"],
        record["portfolio_volatility_annualized"],
        tuple(record["component_contribution_shares"].items()),
    )


def _current_attributions(
    records: Iterable[AttributionInput],
    *,
    policy: PortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    end_date: date | None,
) -> tuple[list[dict[str, Any]], int]:
    current: dict[date, dict[str, Any]] = {}
    seen_calculations: dict[str, tuple[Any, ...]] = {}
    matched_records = 0
    for candidate in records:
        record = _normalise_attribution_record(
            candidate,
            policy=policy,
            definition_fingerprint=definition_fingerprint,
            end_date=end_date,
        )
        if record is None:
            continue
        matched_records += 1
        calculation_id = record["calculation_id"]
        signature = _record_signature(record)
        previous_signature = seen_calculations.get(calculation_id)
        if previous_signature is not None:
            if previous_signature != signature:
                raise ValidationError(
                    "attribution calculation IDs must not contain conflicting records"
                )
            continue
        seen_calculations[calculation_id] = signature

        event_date = record["ts_event"].date()
        existing = current.get(event_date)
        if existing is None or (
            record["ts_ingest"],
            calculation_id,
        ) > (
            existing["ts_ingest"],
            existing["calculation_id"],
        ):
            current[event_date] = record

    if not current:
        raise ValidationError(
            "no attribution snapshots matched the selected policy and definition"
        )
    return [current[event_date] for event_date in sorted(current)], matched_records


def _status(
    observed_value: float,
    thresholds: RiskLimitThresholds,
) -> tuple[str, bool, float | None, float]:
    if observed_value >= thresholds.critical:
        return (
            "critical",
            True,
            thresholds.critical,
            observed_value - thresholds.critical,
        )
    if observed_value >= thresholds.warning:
        return (
            "warning",
            True,
            thresholds.warning,
            observed_value - thresholds.warning,
        )
    return "ok", False, None, 0.0


def _evaluation_id(
    *,
    policy: PortfolioRiskLimitPolicy,
    attribution_calculation_id: str,
    metric_name: str,
) -> str:
    payload = {
        "attribution_calculation_id": attribution_calculation_id,
        "metric_name": metric_name,
        "model_version": MODEL_VERSION,
        "policy_fingerprint": policy.fingerprint,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-evaluation-{digest}"


def _evaluation_record(
    *,
    record: Mapping[str, Any],
    policy: PortfolioRiskLimitPolicy,
    metric_name: str,
    subject_type: str,
    subject_key: str,
    unit: str,
    observed_value: float,
    observed_signed_value: float,
    thresholds: RiskLimitThresholds,
) -> dict[str, Any]:
    status, is_breach, breach_threshold, breach_excess = _status(
        observed_value,
        thresholds,
    )
    return {
        "model_version": MODEL_VERSION,
        "calculation_id": _evaluation_id(
            policy=policy,
            attribution_calculation_id=record["calculation_id"],
            metric_name=metric_name,
        ),
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "portfolio_id": policy.portfolio_id,
        "base_currency": record["base_currency"],
        "definition_fingerprint": record["definition_fingerprint"],
        "attribution_calculation_id": record["calculation_id"],
        "attribution_model_version": record["model_version"],
        "weighting_method": record["weighting_method"],
        "covariance_method": record["covariance_method"],
        "correlation_method": record["correlation_method"],
        "covariance_window": record["covariance_window"],
        "annualization_days": record["annualization_days"],
        "ts_event": record["ts_event"],
        "ts_ingest": record["ts_ingest"],
        "metric_name": metric_name,
        "subject_type": subject_type,
        "subject_key": subject_key,
        "unit": unit,
        "observed_value": observed_value,
        "observed_signed_value": observed_signed_value,
        "warning_threshold": thresholds.warning,
        "critical_threshold": thresholds.critical,
        "status": status,
        "is_breach": is_breach,
        "breach_threshold": breach_threshold,
        "breach_excess": breach_excess,
    }


def evaluate_portfolio_risk_limits(
    records: Iterable[AttributionInput],
    *,
    policy: PortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    start_date: date | None = None,
    end_date: date | None = None,
    max_evaluations: int = MAX_LIMIT_EVALUATIONS,
) -> PortfolioRiskLimitOutput:
    if not isinstance(definition_fingerprint, str) or not definition_fingerprint:
        raise ValidationError("definition_fingerprint must be non-empty text")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    max_evaluations = _positive_integer(
        max_evaluations,
        "max_evaluations",
        MAX_LIMIT_EVALUATIONS,
    )

    current, matched_records = _current_attributions(
        records,
        policy=policy,
        definition_fingerprint=definition_fingerprint,
        end_date=end_date,
    )
    selected = [
        record
        for record in current
        if start_date is None or record["ts_event"].date() >= start_date
    ]
    if not selected:
        raise ValidationError(
            "no risk-limit evaluations matched the requested date range"
        )
    if len(selected) * 2 > max_evaluations:
        raise ValidationError(
            "risk-limit evaluation exceeds max_evaluations; split the date range"
        )

    evaluations: list[dict[str, Any]] = []
    for record in selected:
        evaluations.append(
            _evaluation_record(
                record=record,
                policy=policy,
                metric_name=VOLATILITY_METRIC,
                subject_type="portfolio",
                subject_key=policy.portfolio_id,
                unit="annualized_decimal",
                observed_value=record["portfolio_volatility_annualized"],
                observed_signed_value=record[
                    "portfolio_volatility_annualized"
                ],
                thresholds=policy.portfolio_volatility,
            )
        )
        component_shares = record["component_contribution_shares"]
        largest_component = min(
            component_shares,
            key=lambda key: (-abs(component_shares[key]), key),
        )
        signed_share = component_shares[largest_component]
        evaluations.append(
            _evaluation_record(
                record=record,
                policy=policy,
                metric_name=CONCENTRATION_METRIC,
                subject_type="constituent",
                subject_key=largest_component,
                unit="absolute_share",
                observed_value=abs(signed_share),
                observed_signed_value=signed_share,
                thresholds=policy.component_concentration,
            )
        )

    status_counts = {
        status: sum(1 for item in evaluations if item["status"] == status)
        for status in ("ok", "warning", "critical")
    }
    diagnostics = {
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
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
