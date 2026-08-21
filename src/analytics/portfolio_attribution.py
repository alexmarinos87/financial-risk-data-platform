from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, TypeAlias, cast

import pandas as pd

from ..common.exceptions import ValidationError
from .portfolio_risk import (
    TRADING_DAYS_PER_YEAR,
    WEIGHTING_METHOD,
    PortfolioDefinition,
)

MODEL_VERSION = "portfolio-attribution-v1"
COVARIANCE_METHOD = "sample_annualized"
CORRELATION_METHOD = "pearson"
MAX_COVARIANCE_WINDOW = 10 * TRADING_DAYS_PER_YEAR
WEIGHT_TOLERANCE = 1e-9
RETURN_TOLERANCE = 1e-12
VARIANCE_EPSILON = 1e-12
EULER_TOLERANCE = 1e-10

PortfolioReturnInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PortfolioAttributionOutput:
    snapshot: dict[str, Any]
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


def _finite_number(value: Any, label: str, *, greater_than: float | None = None) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise ValidationError(f"{label} must be a finite number")
    if greater_than is not None and parsed <= greater_than:
        raise ValidationError(f"{label} must be greater than {greater_than}")
    return parsed


def _positive_integer(value: int, label: str, maximum: int) -> int:
    if type(value) is not int or not 2 <= value <= maximum:
        raise ValidationError(f"{label} must be an integer between 2 and {maximum}")
    return value


def _json_object(value: Any, label: str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        parsed = dict(value)
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
            raise ValidationError(f"{label} must be a valid JSON object") from None
    else:
        raise ValidationError(f"{label} must be a JSON object")
    if not isinstance(parsed, dict) or not all(isinstance(key, str) for key in parsed):
        raise ValidationError(f"{label} must be a JSON object with text keys")
    return parsed


def _json_document(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _definition_weights(definition: PortfolioDefinition) -> dict[str, float]:
    return {constituent.key: constituent.weight for constituent in definition.constituents}


def _normalise_record(
    candidate: PortfolioReturnInput,
    definition: PortfolioDefinition,
) -> dict[str, Any] | None:
    if not isinstance(candidate, Mapping):
        raise ValidationError("portfolio attribution input must contain mappings")

    portfolio_id = _required_text(candidate.get("portfolio_id"), "portfolio_id")
    definition_fingerprint = _required_text(
        candidate.get("definition_fingerprint"),
        "definition_fingerprint",
    )
    if (
        portfolio_id != definition.portfolio_id
        or definition_fingerprint != definition.fingerprint
    ):
        return None

    calculation_id = _required_text(candidate.get("calculation_id"), "calculation_id")
    model_version = _required_text(candidate.get("model_version"), "model_version")
    base_currency = _required_text(candidate.get("base_currency"), "base_currency")
    weighting_method = _required_text(candidate.get("weighting_method"), "weighting_method")
    if base_currency != definition.base_currency:
        raise ValidationError("portfolio return base currency does not match the definition")
    if weighting_method != WEIGHTING_METHOD:
        raise ValidationError("portfolio return weighting method is unsupported")

    constituent_count = candidate.get("constituent_count")
    if type(constituent_count) is not int or constituent_count != len(definition.constituents):
        raise ValidationError("portfolio return constituent count does not match the definition")

    ts_event = _aware_utc(candidate.get("ts_event"), "ts_event")
    if ts_event.time() != time.min:
        raise ValidationError("portfolio return timestamps must use UTC midnight")
    ts_ingest = _aware_utc(candidate.get("ts_ingest"), "ts_ingest")

    expected_weights = _definition_weights(definition)
    expected_keys = set(expected_weights)
    weights_raw = _json_object(candidate.get("weights_json"), "weights_json")
    component_ids_raw = _json_object(
        candidate.get("component_calculation_ids_json"),
        "component_calculation_ids_json",
    )
    component_returns_raw = _json_object(
        candidate.get("component_returns_json"),
        "component_returns_json",
    )
    if not (
        set(weights_raw) == expected_keys
        and set(component_ids_raw) == expected_keys
        and set(component_returns_raw) == expected_keys
    ):
        raise ValidationError("portfolio return evidence keys do not match the definition")

    weights: dict[str, float] = {}
    component_ids: dict[str, str] = {}
    component_returns: dict[str, float] = {}
    for key in sorted(expected_keys):
        weight = _finite_number(weights_raw[key], f"weight for {key}", greater_than=0.0)
        if not math.isclose(
            weight,
            expected_weights[key],
            rel_tol=0.0,
            abs_tol=WEIGHT_TOLERANCE,
        ):
            raise ValidationError("portfolio return weights do not match the definition")
        weights[key] = weight
        component_ids[key] = _required_text(
            component_ids_raw[key],
            f"component calculation ID for {key}",
        )
        component_returns[key] = _finite_number(
            component_returns_raw[key],
            f"component return for {key}",
            greater_than=-1.0,
        )

    portfolio_return = _finite_number(
        candidate.get("portfolio_return_1d"),
        "portfolio_return_1d",
        greater_than=-1.0,
    )
    reconstructed_return = math.fsum(
        weights[key] * component_returns[key] for key in sorted(expected_keys)
    )
    if not math.isclose(
        portfolio_return,
        reconstructed_return,
        rel_tol=0.0,
        abs_tol=RETURN_TOLERANCE,
    ):
        raise ValidationError(
            "portfolio return does not equal the configured weighted component returns"
        )

    return {
        "model_version": model_version,
        "calculation_id": calculation_id,
        "portfolio_id": portfolio_id,
        "base_currency": base_currency,
        "definition_fingerprint": definition_fingerprint,
        "weighting_method": weighting_method,
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "constituent_count": cast(int, constituent_count),
        "weights": weights,
        "component_calculation_ids": component_ids,
        "component_returns": component_returns,
        "portfolio_return_1d": portfolio_return,
    }


def _record_signature(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record["model_version"],
        record["portfolio_id"],
        record["base_currency"],
        record["definition_fingerprint"],
        record["weighting_method"],
        record["ts_event"],
        record["ts_ingest"],
        record["constituent_count"],
        tuple(record["weights"].items()),
        tuple(record["component_calculation_ids"].items()),
        tuple(record["component_returns"].items()),
        record["portfolio_return_1d"],
    )


def _current_records(
    records: Iterable[PortfolioReturnInput],
    definition: PortfolioDefinition,
    end_date: date | None,
) -> tuple[list[dict[str, Any]], int]:
    current: dict[date, dict[str, Any]] = {}
    seen_calculations: dict[str, tuple[Any, ...]] = {}
    matched_records = 0
    for candidate in records:
        record = _normalise_record(candidate, definition)
        if record is None:
            continue
        event_date = record["ts_event"].date()
        if end_date is not None and event_date > end_date:
            continue
        matched_records += 1

        calculation_id = record["calculation_id"]
        signature = _record_signature(record)
        previous_signature = seen_calculations.get(calculation_id)
        if previous_signature is not None:
            if previous_signature != signature:
                raise ValidationError(
                    "portfolio return calculation IDs must not contain conflicting records"
                )
            continue
        seen_calculations[calculation_id] = signature

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
        raise ValidationError("no portfolio return records matched the requested definition")
    return [current[event_date] for event_date in sorted(current)], matched_records


def _matrix_document(
    frame: pd.DataFrame,
    keys: tuple[str, ...],
    *,
    allow_undefined: bool,
) -> tuple[str, int]:
    matrix: dict[str, dict[str, float | None]] = {}
    undefined = 0
    for row_key in keys:
        row: dict[str, float | None] = {}
        for column_key in keys:
            value = frame.loc[row_key, column_key]
            if pd.isna(value) or not math.isfinite(float(value)):
                if not allow_undefined:
                    raise ValidationError("portfolio covariance contains a non-finite value")
                row[column_key] = None
                undefined += 1
            else:
                row[column_key] = float(value)
        matrix[row_key] = row
    return _json_document(matrix), undefined


def _calculation_id(
    definition: PortfolioDefinition,
    input_calculation_ids: list[str],
    covariance_window: int,
) -> str:
    payload = {
        "annualization_days": TRADING_DAYS_PER_YEAR,
        "correlation_method": CORRELATION_METHOD,
        "covariance_method": COVARIANCE_METHOD,
        "covariance_window": covariance_window,
        "definition_fingerprint": definition.fingerprint,
        "input_calculation_ids": input_calculation_ids,
        "model_version": MODEL_VERSION,
        "weighting_method": WEIGHTING_METHOD,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-snapshot-{digest}"


def build_portfolio_attribution(
    records: Iterable[PortfolioReturnInput],
    *,
    definition: PortfolioDefinition,
    covariance_window: int = 20,
    end_date: date | None = None,
) -> PortfolioAttributionOutput:
    covariance_window = _positive_integer(
        covariance_window,
        "covariance_window",
        MAX_COVARIANCE_WINDOW,
    )
    current, matched_records = _current_records(records, definition, end_date)
    if len(current) < covariance_window:
        raise ValidationError(
            "portfolio attribution requires at least "
            f"{covariance_window} current portfolio return observations"
        )
    window = current[-covariance_window:]
    keys = tuple(constituent.key for constituent in definition.constituents)
    component_frame = pd.DataFrame(
        [record["component_returns"] for record in window],
        columns=list(keys),
        dtype="float64",
    )
    annualized_covariance = component_frame.cov() * TRADING_DAYS_PER_YEAR
    correlation = component_frame.corr()
    covariance_json, _ = _matrix_document(
        annualized_covariance,
        keys,
        allow_undefined=False,
    )
    correlation_json, undefined_correlation_cells = _matrix_document(
        correlation,
        keys,
        allow_undefined=True,
    )

    weights = _definition_weights(definition)
    marginal_variance = {
        key: math.fsum(
            float(annualized_covariance.loc[key, other]) * weights[other]
            for other in keys
        )
        for key in keys
    }
    portfolio_variance = math.fsum(
        weights[key] * marginal_variance[key] for key in keys
    )
    if portfolio_variance < -VARIANCE_EPSILON:
        raise ValidationError("portfolio covariance produced a negative variance")
    portfolio_variance = max(0.0, portfolio_variance)
    portfolio_volatility = math.sqrt(portfolio_variance)

    constituent_volatility: dict[str, float] = {}
    for key in keys:
        diagonal = float(annualized_covariance.loc[key, key])
        if diagonal < -VARIANCE_EPSILON:
            raise ValidationError("portfolio covariance contains a negative diagonal")
        constituent_volatility[key] = math.sqrt(max(0.0, diagonal))

    if portfolio_volatility > VARIANCE_EPSILON:
        marginal_contribution = {
            key: marginal_variance[key] / portfolio_volatility for key in keys
        }
        component_contribution = {
            key: weights[key] * marginal_contribution[key] for key in keys
        }
        contribution_share = {
            key: component_contribution[key] / portfolio_volatility for key in keys
        }
        volatility_status = "positive"
    else:
        marginal_contribution = {key: 0.0 for key in keys}
        component_contribution = {key: 0.0 for key in keys}
        contribution_share = {key: 0.0 for key in keys}
        volatility_status = "zero"

    euler_total = math.fsum(component_contribution.values())
    if not math.isclose(
        euler_total,
        portfolio_volatility,
        rel_tol=0.0,
        abs_tol=EULER_TOLERANCE,
    ):
        raise ValidationError("component volatility contributions do not reconcile")

    input_calculation_ids = [record["calculation_id"] for record in window]
    snapshot = {
        "model_version": MODEL_VERSION,
        "calculation_id": _calculation_id(
            definition,
            input_calculation_ids,
            covariance_window,
        ),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "weighting_method": WEIGHTING_METHOD,
        "covariance_method": COVARIANCE_METHOD,
        "correlation_method": CORRELATION_METHOD,
        "covariance_window": covariance_window,
        "window_start": window[0]["ts_event"],
        "window_end": window[-1]["ts_event"],
        "window_observations": len(window),
        "annualization_days": TRADING_DAYS_PER_YEAR,
        "ts_event": window[-1]["ts_event"],
        "ts_ingest": max(record["ts_ingest"] for record in window),
        "constituent_count": len(keys),
        "weights_json": _json_document(weights),
        "input_calculation_ids_json": _json_document(input_calculation_ids),
        "input_first_calculation_id": input_calculation_ids[0],
        "input_last_calculation_id": input_calculation_ids[-1],
        "covariance_annualized_json": covariance_json,
        "correlation_json": correlation_json,
        "constituent_volatility_annualized_json": _json_document(
            constituent_volatility
        ),
        "marginal_volatility_contribution_json": _json_document(
            marginal_contribution
        ),
        "component_volatility_contribution_json": _json_document(
            component_contribution
        ),
        "component_contribution_share_json": _json_document(contribution_share),
        "portfolio_variance_annualized": portfolio_variance,
        "portfolio_volatility_annualized": portfolio_volatility,
        "volatility_status": volatility_status,
        "correlation_status": (
            "complete"
            if undefined_correlation_cells == 0
            else "undefined_zero_variance"
        ),
        "undefined_correlation_cells": undefined_correlation_cells,
        "euler_residual": euler_total - portfolio_volatility,
    }
    diagnostics = {
        "matched_input_records": matched_records,
        "current_portfolio_return_records": len(current),
        "covariance_window": covariance_window,
        "window_start": window[0]["ts_event"].date().isoformat(),
        "window_end": window[-1]["ts_event"].date().isoformat(),
        "volatility_status": volatility_status,
        "undefined_correlation_cells": undefined_correlation_cells,
    }
    return PortfolioAttributionOutput(snapshot=snapshot, diagnostics=diagnostics)
