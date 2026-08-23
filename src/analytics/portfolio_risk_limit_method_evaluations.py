from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, TypeAlias

from ..common.exceptions import ValidationError
from .portfolio_attribution import VARIANCE_EPSILON
from .portfolio_risk_limit_method_policies import (
    MODEL_VERSION,
    MethodAwarePortfolioRiskLimitPolicy,
)
from .portfolio_risk_limits import (
    CONCENTRATION_METRIC,
    MAX_LIMIT_EVALUATIONS,
    VOLATILITY_METRIC,
    RiskLimitThresholds,
)

AttributionInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MethodAwarePortfolioRiskLimitOutput:
    evaluations: tuple[dict[str, Any], ...]
    diagnostics: Mapping[str, Any]


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


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
        raise ValidationError(f"{label} must be an integer between 1 and {maximum}")
    return value


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


def _json_number_object(value: Any, label: str) -> dict[str, float]:
    if isinstance(value, Mapping):
        parsed: Any = dict(value)
    elif isinstance(value, str):

        def reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            result: dict[str, Any] = {}
            for key, item in pairs:
                if key in result:
                    raise ValidationError(f"{label} must not contain duplicate keys")
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
        raise ValidationError(f"{label} must contain at least two constituent values")

    values: dict[str, float] = {}
    for key, item in parsed.items():
        if not isinstance(key, str) or not key:
            raise ValidationError(f"{label} must use non-empty text keys")
        values[key] = _finite_number(item, f"{label}.{key}")
    return dict(sorted(values.items()))


def _normalise_attribution_record(
    candidate: AttributionInput,
    *,
    policy: MethodAwarePortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    end_date: date | None,
) -> dict[str, Any] | None:
    if not isinstance(candidate, Mapping):
        raise ValidationError("risk-limit input must contain attribution mappings")

    if (
        candidate.get("portfolio_id") != policy.portfolio_id
        or candidate.get("definition_fingerprint") != definition_fingerprint
    ):
        return None

    covariance_window = candidate.get("covariance_window")
    if type(covariance_window) is not int:
        raise ValidationError("covariance_window must be an integer")
    if covariance_window != policy.covariance_window:
        return None

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
    if type(annualization_days) is not int:
        raise ValidationError("annualization_days must be an integer")

    candidate_contract = (
        model_version,
        weighting_method,
        covariance_method,
        correlation_method,
    )
    if (
        candidate_contract != policy.method.key
        or annualization_days != policy.annualization_days
    ):
        return None

    ts_event = _aware_utc(candidate.get("ts_event"), "ts_event")
    if ts_event.time() != time.min:
        raise ValidationError("attribution timestamps must use UTC midnight")
    if end_date is not None and ts_event.date() > end_date:
        return None
    ts_ingest = _aware_utc(candidate.get("ts_ingest"), "ts_ingest")
    if ts_ingest < ts_event:
        raise ValidationError("ts_ingest must be on or after ts_event")

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
        raise ValidationError("volatility_status does not match portfolio volatility")

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
        "covariance_window": covariance_window,
        "annualization_days": annualization_days,
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "portfolio_volatility_annualized": volatility,
        "component_contribution_shares": _json_number_object(
            candidate.get("component_contribution_share_json"),
            "component_contribution_share_json",
        ),
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
    policy: MethodAwarePortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    end_date: date | None,
) -> tuple[list[dict[str, Any]], int, int]:
    current: dict[date, dict[str, Any]] = {}
    seen_calculations: dict[str, tuple[Any, ...]] = {}
    matched_records = 0
    ignored_nonmatching_contract_records = 0
    for candidate in records:
        targeted = (
            isinstance(candidate, Mapping)
            and candidate.get("portfolio_id") == policy.portfolio_id
            and candidate.get("definition_fingerprint") == definition_fingerprint
            and candidate.get("covariance_window") == policy.covariance_window
        )
        record = _normalise_attribution_record(
            candidate,
            policy=policy,
            definition_fingerprint=definition_fingerprint,
            end_date=end_date,
        )
        if record is None:
            if targeted:
                ignored_nonmatching_contract_records += 1
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
            "no attribution snapshots matched the selected method policy and definition"
        )
    return (
        [current[event_date] for event_date in sorted(current)],
        matched_records,
        ignored_nonmatching_contract_records,
    )


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
    policy: MethodAwarePortfolioRiskLimitPolicy,
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
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-evaluation-{digest}"


def _evaluation_record(
    *,
    record: Mapping[str, Any],
    policy: MethodAwarePortfolioRiskLimitPolicy,
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


def evaluate_method_aware_portfolio_risk_limits(
    records: Iterable[AttributionInput],
    *,
    policy: MethodAwarePortfolioRiskLimitPolicy,
    definition_fingerprint: str,
    start_date: date | None = None,
    end_date: date | None = None,
    max_evaluations: int = MAX_LIMIT_EVALUATIONS,
) -> MethodAwarePortfolioRiskLimitOutput:
    if not isinstance(definition_fingerprint, str) or not definition_fingerprint:
        raise ValidationError("definition_fingerprint must be non-empty text")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    max_evaluations = _positive_integer(
        max_evaluations,
        "max_evaluations",
        MAX_LIMIT_EVALUATIONS,
    )

    current, matched_records, ignored_nonmatching_contract_records = _current_attributions(
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
            "no method-aware risk-limit evaluations matched the requested date range"
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
                observed_signed_value=record["portfolio_volatility_annualized"],
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
        "method_policy_id": policy.method_policy_id,
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "base_policy_fingerprint": policy.base_policy.fingerprint,
        "attribution_model_version": policy.method.attribution_model_version,
        "weighting_method": policy.method.weighting_method,
        "covariance_method": policy.method.covariance_method,
        "correlation_method": policy.method.correlation_method,
        "matched_input_records": matched_records,
        "ignored_nonmatching_contract_records": (
            ignored_nonmatching_contract_records
        ),
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
    return MethodAwarePortfolioRiskLimitOutput(
        evaluations=tuple(evaluations),
        diagnostics=diagnostics,
    )
