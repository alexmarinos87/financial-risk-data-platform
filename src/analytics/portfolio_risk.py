from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, TypeAlias, cast

import pandas as pd

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from .risk_metrics import value_at_risk

MODEL_VERSION = "portfolio-risk-v1"
WEIGHTING_METHOD = "constant_weight_daily_rebalanced"
TRADING_DAYS_PER_YEAR = 252
MAX_CONSTITUENTS = 50
WEIGHT_TOLERANCE = 1e-9
PORTFOLIO_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
CURRENCY_PATTERN = re.compile(r"^[A-Z]{3}$")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
SOURCE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")

ReturnInput: TypeAlias = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class PortfolioConstituent:
    source: str
    symbol: str
    weight: float

    @property
    def key(self) -> str:
        return f"{self.source}:{self.symbol}"


@dataclass(frozen=True, slots=True)
class PortfolioDefinition:
    portfolio_id: str
    base_currency: str
    constituents: tuple[PortfolioConstituent, ...]

    @property
    def fingerprint(self) -> str:
        payload = {
            "base_currency": self.base_currency,
            "constituents": [
                {
                    "source": constituent.source,
                    "symbol": constituent.symbol,
                    "weight": constituent.weight,
                }
                for constituent in self.constituents
            ],
            "portfolio_id": self.portfolio_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        return f"portfolio-{digest}"


@dataclass(frozen=True, slots=True)
class PortfolioRiskOutputs:
    returns: tuple[dict[str, Any], ...]
    risk_summary: tuple[dict[str, Any], ...]
    diagnostics: Mapping[str, Any]


def _required_text(value: Any, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    parsed = value.strip()
    if pattern.fullmatch(parsed) is None:
        raise ValidationError(f"{label} has an invalid format")
    return parsed


def _symbol(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be text")
    return _required_text(value.strip().upper(), label, SYMBOL_PATTERN)


def _weight(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(
            "portfolio weights must be finite numbers greater than zero"
        )
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValidationError(
            "portfolio weights must be finite numbers greater than zero"
        )
    return parsed


def parse_portfolio_definition(
    payload: Mapping[str, Any],
    portfolio_id: str,
) -> PortfolioDefinition:
    canonical_id = _required_text(
        portfolio_id,
        "portfolio_id",
        PORTFOLIO_ID_PATTERN,
    )
    if not isinstance(payload, Mapping):
        raise ValidationError("portfolio configuration must be a mapping")
    portfolios = payload.get("portfolios")
    if not isinstance(portfolios, Mapping):
        raise ValidationError(
            "portfolio configuration must define a portfolios mapping"
        )
    candidate = portfolios.get(canonical_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"portfolio '{canonical_id}' is not configured")

    base_currency = _required_text(
        candidate.get("base_currency"),
        "base_currency",
        CURRENCY_PATTERN,
    )
    raw_constituents = candidate.get("constituents")
    if (
        not isinstance(raw_constituents, list)
        or not 2 <= len(raw_constituents) <= MAX_CONSTITUENTS
    ):
        raise ValidationError(
            "portfolio constituents must contain between "
            f"2 and {MAX_CONSTITUENTS} entries"
        )

    constituents: list[PortfolioConstituent] = []
    seen: set[tuple[str, str]] = set()
    for raw in raw_constituents:
        if not isinstance(raw, Mapping):
            raise ValidationError("each portfolio constituent must be a mapping")
        source = _required_text(
            raw.get("source"),
            "constituent source",
            SOURCE_PATTERN,
        ).lower()
        symbol = _symbol(raw.get("symbol"), "constituent symbol")
        key = (source, symbol)
        if key in seen:
            raise ValidationError(
                "portfolio constituents must be unique by source and symbol"
            )
        seen.add(key)
        constituents.append(
            PortfolioConstituent(
                source=source,
                symbol=symbol,
                weight=_weight(raw.get("weight")),
            )
        )

    total_weight = math.fsum(constituent.weight for constituent in constituents)
    if not math.isclose(
        total_weight,
        1.0,
        rel_tol=0.0,
        abs_tol=WEIGHT_TOLERANCE,
    ):
        raise ValidationError("portfolio weights must sum to 1")

    return PortfolioDefinition(
        portfolio_id=canonical_id,
        base_currency=base_currency,
        constituents=tuple(
            sorted(constituents, key=lambda item: (item.source, item.symbol))
        ),
    )


def load_portfolio_definition(path: Path, portfolio_id: str) -> PortfolioDefinition:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError("portfolio configuration could not be loaded") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("portfolio configuration must be a mapping")
    return parse_portfolio_definition(payload, portfolio_id)


def _positive_integer(value: int, label: str, maximum: int) -> int:
    if type(value) is not int or not 2 <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between 2 and {maximum}"
        )
    return value


def _confidence(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("var_confidence must be a number between 0 and 1")
    parsed = float(value)
    if not math.isfinite(parsed) or not 0 < parsed < 1:
        raise ValidationError("var_confidence must be a number between 0 and 1")
    return parsed


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


def _finite_return(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(
            "daily return values must be finite numbers greater than -1"
        )
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= -1:
        raise ValidationError(
            "daily return values must be finite numbers greater than -1"
        )
    return parsed


def _normalise_return_record(candidate: ReturnInput) -> dict[str, Any]:
    if not isinstance(candidate, Mapping):
        raise ValidationError(
            "portfolio input must contain daily return mappings"
        )
    source = _required_text(
        candidate.get("source"),
        "return source",
        SOURCE_PATTERN,
    ).lower()
    symbol = _symbol(candidate.get("symbol"), "return symbol")
    calculation_id = candidate.get("calculation_id")
    model_version = candidate.get("model_version")
    source_event_id = candidate.get("source_event_id")
    for label, value in (
        ("calculation_id", calculation_id),
        ("model_version", model_version),
        ("source_event_id", source_event_id),
    ):
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"{label} must be non-empty text")

    ts_event = _aware_utc(candidate.get("ts_event"), "ts_event")
    if ts_event.time() != time.min:
        raise ValidationError("daily return timestamps must use UTC midnight")
    ts_ingest = _aware_utc(candidate.get("ts_ingest"), "ts_ingest")
    return {
        "model_version": cast(str, model_version).strip(),
        "calculation_id": cast(str, calculation_id).strip(),
        "source": source,
        "symbol": symbol,
        "source_event_id": cast(str, source_event_id).strip(),
        "ts_event": ts_event,
        "ts_ingest": ts_ingest,
        "return_1d": _finite_return(candidate.get("return_1d")),
    }


def _record_signature(record: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        record["model_version"],
        record["source"],
        record["symbol"],
        record["source_event_id"],
        record["ts_event"],
        record["ts_ingest"],
        record["return_1d"],
    )


def _current_returns(
    records: Iterable[ReturnInput],
    definition: PortfolioDefinition,
    end_date: date | None,
) -> tuple[dict[tuple[str, str, date], dict[str, Any]], int]:
    constituent_keys = {
        (item.source, item.symbol) for item in definition.constituents
    }
    current: dict[tuple[str, str, date], dict[str, Any]] = {}
    seen_calculations: dict[str, tuple[Any, ...]] = {}
    matched_records = 0
    for candidate in records:
        record = _normalise_return_record(candidate)
        key = (record["source"], record["symbol"])
        if key not in constituent_keys:
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
                    "daily return calculation IDs must not contain "
                    "conflicting records"
                )
            continue
        seen_calculations[calculation_id] = signature

        grain = (record["source"], record["symbol"], event_date)
        existing = current.get(grain)
        if existing is None or (
            record["ts_ingest"],
            calculation_id,
        ) > (
            existing["ts_ingest"],
            existing["calculation_id"],
        ):
            current[grain] = record

    missing_constituents = [
        item.key
        for item in definition.constituents
        if not any(
            source == item.source and symbol == item.symbol
            for source, symbol, _event_date in current
        )
    ]
    if missing_constituents:
        raise ValidationError(
            "portfolio input is missing configured constituents: "
            + ", ".join(missing_constituents)
        )
    return current, matched_records


def _json_mapping(values: Mapping[str, Any]) -> str:
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


def _calculation_id(
    metric: str,
    definition: PortfolioDefinition,
    component_calculation_ids: Iterable[str],
    parameters: Mapping[str, Any],
) -> str:
    payload = {
        "component_calculation_ids": list(component_calculation_ids),
        "definition_fingerprint": definition.fingerprint,
        "metric": metric,
        "model_version": MODEL_VERSION,
        "parameters": dict(sorted(parameters.items())),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-{metric}-{digest}"


def build_portfolio_risk_outputs(
    records: Iterable[ReturnInput],
    *,
    definition: PortfolioDefinition,
    volatility_window: int = 20,
    var_window: int = 60,
    var_confidence: float = 0.95,
    start_date: date | None = None,
    end_date: date | None = None,
) -> PortfolioRiskOutputs:
    volatility_window = _positive_integer(
        volatility_window,
        "volatility_window",
        TRADING_DAYS_PER_YEAR,
    )
    var_window = _positive_integer(
        var_window,
        "var_window",
        10 * TRADING_DAYS_PER_YEAR,
    )
    confidence = _confidence(var_confidence)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    current, matched_records = _current_returns(records, definition, end_date)
    constituent_count = len(definition.constituents)
    candidate_dates = sorted({event_date for _, _, event_date in current})
    aligned_dates = [
        event_date
        for event_date in candidate_dates
        if all(
            (constituent.source, constituent.symbol, event_date) in current
            for constituent in definition.constituents
        )
    ]
    if len(aligned_dates) < 2:
        raise ValidationError(
            "portfolio risk requires at least two fully aligned return dates"
        )

    weights = {
        constituent.key: constituent.weight
        for constituent in definition.constituents
    }
    weights_json = _json_mapping(weights)
    return_records_all: list[dict[str, Any]] = []
    portfolio_returns: list[float] = []

    for event_date in aligned_dates:
        components = [
            current[(constituent.source, constituent.symbol, event_date)]
            for constituent in definition.constituents
        ]
        component_ids = [component["calculation_id"] for component in components]
        component_returns = {
            constituent.key: component["return_1d"]
            for constituent, component in zip(
                definition.constituents,
                components,
                strict=True,
            )
        }
        contributions = {
            constituent.key: constituent.weight * component["return_1d"]
            for constituent, component in zip(
                definition.constituents,
                components,
                strict=True,
            )
        }
        portfolio_return = math.fsum(contributions.values())
        portfolio_returns.append(portfolio_return)
        return_records_all.append(
            {
                "model_version": MODEL_VERSION,
                "calculation_id": _calculation_id(
                    "return",
                    definition,
                    component_ids,
                    {"weighting_method": WEIGHTING_METHOD},
                ),
                "portfolio_id": definition.portfolio_id,
                "base_currency": definition.base_currency,
                "definition_fingerprint": definition.fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "ts_event": components[0]["ts_event"],
                "ts_ingest": max(
                    component["ts_ingest"] for component in components
                ),
                "constituent_count": constituent_count,
                "weights_json": weights_json,
                "component_calculation_ids_json": _json_mapping(
                    {
                        constituent.key: component["calculation_id"]
                        for constituent, component in zip(
                            definition.constituents,
                            components,
                            strict=True,
                        )
                    }
                ),
                "component_returns_json": _json_mapping(component_returns),
                "contributions_json": _json_mapping(contributions),
                "portfolio_return_1d": portfolio_return,
            }
        )

    returns_series = pd.Series(portfolio_returns, dtype="float64")
    rolling_volatility = (
        returns_series.rolling(
            window=volatility_window,
            min_periods=volatility_window,
        ).std()
        * math.sqrt(TRADING_DAYS_PER_YEAR)
    )
    wealth = (1.0 + returns_series).cumprod()
    running_peak = wealth.cummax().clip(lower=1.0)
    maximum_drawdown = (wealth / running_peak - 1.0).cummin()

    emitted_returns: list[dict[str, Any]] = []
    summary_records: list[dict[str, Any]] = []
    for index, return_record in enumerate(return_records_all):
        event_date = return_record["ts_event"].date()
        if start_date is not None and event_date < start_date:
            continue
        emitted_returns.append(return_record)

        volatility_value = (
            float(rolling_volatility.iloc[index])
            if index + 1 >= volatility_window
            else None
        )
        var_start = max(0, index - var_window + 1)
        var_slice = returns_series.iloc[var_start : index + 1]
        var_loss = (
            max(0.0, -value_at_risk(var_slice, confidence=confidence))
            if len(var_slice) >= 2
            else None
        )
        history = return_records_all[: index + 1]
        history_ids = [record["calculation_id"] for record in history]
        ready = index + 1 >= max(volatility_window, var_window)
        summary_records.append(
            {
                "model_version": MODEL_VERSION,
                "calculation_id": _calculation_id(
                    "summary",
                    definition,
                    history_ids,
                    {
                        "annualization_days": TRADING_DAYS_PER_YEAR,
                        "var_confidence": confidence,
                        "var_window": var_window,
                        "volatility_window": volatility_window,
                        "weighting_method": WEIGHTING_METHOD,
                    },
                ),
                "portfolio_id": definition.portfolio_id,
                "base_currency": definition.base_currency,
                "definition_fingerprint": definition.fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "portfolio_return_calculation_id": return_record[
                    "calculation_id"
                ],
                "ts_event": return_record["ts_event"],
                "ts_ingest": max(record["ts_ingest"] for record in history),
                "portfolio_return_1d": return_record["portfolio_return_1d"],
                "volatility_annualized": volatility_value,
                "volatility_window": volatility_window,
                "annualization_days": TRADING_DAYS_PER_YEAR,
                "historical_var_loss": var_loss,
                "var_confidence": confidence,
                "var_window": var_window,
                "var_observations": len(var_slice),
                "maximum_drawdown": float(maximum_drawdown.iloc[index]),
                "aligned_observations": index + 1,
                "constituent_count": constituent_count,
                "weights_json": weights_json,
                "history_status": "ready" if ready else "partial",
                "input_first_calculation_id": history[0]["calculation_id"],
                "input_last_calculation_id": history[-1]["calculation_id"],
            }
        )

    if not emitted_returns or not summary_records:
        raise ValidationError(
            "no portfolio risk outputs matched the requested date range"
        )

    diagnostics = {
        "portfolio_id": definition.portfolio_id,
        "weighting_method": WEIGHTING_METHOD,
        "matched_input_records": matched_records,
        "current_component_records": len(current),
        "candidate_dates": len(candidate_dates),
        "aligned_dates": len(aligned_dates),
        "dropped_incomplete_dates": len(candidate_dates) - len(aligned_dates),
        "first_aligned_date": aligned_dates[0].isoformat(),
        "last_aligned_date": aligned_dates[-1].isoformat(),
        "constituent_count": constituent_count,
    }
    return PortfolioRiskOutputs(
        returns=tuple(emitted_returns),
        risk_summary=tuple(summary_records),
        diagnostics=diagnostics,
    )
