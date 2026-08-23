from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from ..common.exceptions import ValidationError
from .portfolio_attribution import (
    EULER_TOLERANCE,
    MAX_COVARIANCE_WINDOW,
    VARIANCE_EPSILON,
    PortfolioAttributionOutput,
    PortfolioReturnInput,
    _current_records,
    _definition_weights,
    _json_document,
    _matrix_document,
    _positive_integer,
)
from .portfolio_risk import (
    TRADING_DAYS_PER_YEAR,
    WEIGHTING_METHOD,
    PortfolioDefinition,
)

MODEL_VERSION = "portfolio-attribution-ewma-v1"
COVARIANCE_METHOD = "ewma_zero_mean_lambda_0_94_annualized"
CORRELATION_METHOD = "implied_from_ewma_covariance"
EWMA_DECAY = 0.94


def _ewma_observation_weights(observations: int) -> np.ndarray[Any, Any]:
    observations = _positive_integer(
        observations,
        "observations",
        MAX_COVARIANCE_WINDOW,
    )
    exponents = np.arange(observations - 1, -1, -1, dtype="float64")
    raw_weights = np.power(EWMA_DECAY, exponents)
    total = float(raw_weights.sum())
    if not math.isfinite(total) or total <= 0:
        raise ValidationError("EWMA observation weights are invalid")
    weights = raw_weights / total
    if not np.isfinite(weights).all():
        raise ValidationError("EWMA observation weights are invalid")
    return weights


def _ewma_covariance_and_correlation(
    component_frame: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, np.ndarray[Any, Any]]:
    observations = len(component_frame)
    weights = _ewma_observation_weights(observations)
    values = component_frame.to_numpy(dtype="float64", copy=True)
    if values.ndim != 2 or values.shape[0] != observations:
        raise ValidationError("EWMA component returns have an invalid shape")
    if not np.isfinite(values).all():
        raise ValidationError("EWMA component returns must be finite")

    # The RiskMetrics-style assumption is a zero daily expected return. The
    # finite window is normalized so every published matrix is self-contained.
    daily_covariance = values.T @ (values * weights[:, None])
    daily_covariance = (daily_covariance + daily_covariance.T) / 2.0
    annualized_values = daily_covariance * TRADING_DAYS_PER_YEAR
    if not np.isfinite(annualized_values).all():
        raise ValidationError("EWMA covariance contains a non-finite value")

    labels = list(component_frame.columns)
    annualized_covariance = pd.DataFrame(
        annualized_values,
        index=labels,
        columns=labels,
        dtype="float64",
    )

    variances = np.diag(annualized_values).copy()
    if np.any(variances < -VARIANCE_EPSILON):
        raise ValidationError("EWMA covariance contains a negative diagonal")
    variances = np.maximum(variances, 0.0)
    denominator = np.sqrt(np.outer(variances, variances))
    correlation_values = np.full_like(annualized_values, np.nan)
    np.divide(
        annualized_values,
        denominator,
        out=correlation_values,
        where=denominator > VARIANCE_EPSILON,
    )
    correlation_values = np.clip(correlation_values, -1.0, 1.0)
    for index, variance in enumerate(variances):
        if variance > VARIANCE_EPSILON:
            correlation_values[index, index] = 1.0
    correlation = pd.DataFrame(
        correlation_values,
        index=labels,
        columns=labels,
        dtype="float64",
    )
    return annualized_covariance, correlation, weights


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


def build_portfolio_ewma_attribution(
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
            "EWMA portfolio attribution requires at least "
            f"{covariance_window} current portfolio return observations"
        )
    window = current[-covariance_window:]
    keys = tuple(constituent.key for constituent in definition.constituents)
    component_frame = pd.DataFrame(
        [record["component_returns"] for record in window],
        columns=list(keys),
        dtype="float64",
    )
    annualized_covariance, correlation, observation_weights = (
        _ewma_covariance_and_correlation(component_frame)
    )
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

    portfolio_weights = _definition_weights(definition)
    marginal_variance = {
        key: math.fsum(
            float(annualized_covariance.loc[key, other])
            * portfolio_weights[other]
            for other in keys
        )
        for key in keys
    }
    portfolio_variance = math.fsum(
        portfolio_weights[key] * marginal_variance[key] for key in keys
    )
    if portfolio_variance < -VARIANCE_EPSILON:
        raise ValidationError("EWMA covariance produced a negative portfolio variance")
    portfolio_variance = max(0.0, portfolio_variance)
    portfolio_volatility = math.sqrt(portfolio_variance)

    constituent_volatility: dict[str, float] = {}
    for key in keys:
        diagonal = float(annualized_covariance.loc[key, key])
        if diagonal < -VARIANCE_EPSILON:
            raise ValidationError("EWMA covariance contains a negative diagonal")
        constituent_volatility[key] = math.sqrt(max(0.0, diagonal))

    if portfolio_volatility > VARIANCE_EPSILON:
        marginal_contribution = {
            key: marginal_variance[key] / portfolio_volatility for key in keys
        }
        component_contribution = {
            key: portfolio_weights[key] * marginal_contribution[key] for key in keys
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
        raise ValidationError("EWMA component volatility contributions do not reconcile")

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
        "weights_json": _json_document(portfolio_weights),
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
    effective_observations = 1.0 / math.fsum(
        float(weight) ** 2 for weight in observation_weights
    )
    diagnostics = {
        "matched_input_records": matched_records,
        "current_portfolio_return_records": len(current),
        "covariance_method": COVARIANCE_METHOD,
        "correlation_method": CORRELATION_METHOD,
        "covariance_window": covariance_window,
        "ewma_decay": EWMA_DECAY,
        "oldest_observation_weight": float(observation_weights[0]),
        "newest_observation_weight": float(observation_weights[-1]),
        "effective_observations": effective_observations,
        "window_start": window[0]["ts_event"].date().isoformat(),
        "window_end": window[-1]["ts_event"].date().isoformat(),
        "volatility_status": volatility_status,
        "undefined_correlation_cells": undefined_correlation_cells,
    }
    return PortfolioAttributionOutput(snapshot=snapshot, diagnostics=diagnostics)
