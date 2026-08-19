from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from typing import Any, TypeAlias

import pandas as pd
from pydantic import ValidationError as PydanticValidationError

from ..common.exceptions import ValidationError
from ..ingestion.schemas import MarketEvent
from .risk_metrics import value_at_risk

MODEL_VERSION = "daily-risk-v1"
TRADING_DAYS_PER_YEAR = 252

EventInput: TypeAlias = MarketEvent | Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class DailyRiskOutputs:
    returns: tuple[dict[str, Any], ...]
    volatility: tuple[dict[str, Any], ...]
    risk_summary: tuple[dict[str, Any], ...]


def _require_integer(value: int, label: str, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def _require_confidence(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("var_confidence must be a number between 0 and 1")
    parsed = float(value)
    if not math.isfinite(parsed) or not 0 < parsed < 1:
        raise ValidationError("var_confidence must be a number between 0 and 1")
    return parsed


def _normalise_events(events: Iterable[EventInput], end_date: date | None) -> list[MarketEvent]:
    validated: list[MarketEvent] = []
    try:
        for candidate in events:
            event = MarketEvent.model_validate(candidate)
            event_timestamp = event.ts_event.astimezone(timezone.utc)
            if event_timestamp.time() != time.min:
                raise ValidationError("Daily market events must use UTC midnight event timestamps")
            if end_date is not None and event_timestamp.date() > end_date:
                continue
            if not event.event_id.strip() or not event.symbol.strip() or not event.source.strip():
                raise ValidationError("Daily market event identity fields must not be empty")
            if not math.isfinite(event.price) or event.price <= 0:
                raise ValidationError(
                    "Daily market event prices must be finite and greater than zero"
                )
            if event.volume < 0:
                raise ValidationError("Daily market event volume must not be negative")
            validated.append(event)
    except PydanticValidationError:
        raise ValidationError("Daily risk input contains an invalid market event") from None

    if len(validated) < 2:
        raise ValidationError("Daily risk calculation requires at least two observations")

    event_ids: set[str] = set()
    grains: set[tuple[str, str, date]] = set()
    for event in validated:
        event_date = event.ts_event.astimezone(timezone.utc).date()
        grain = (event.source, event.symbol, event_date)
        if event.event_id in event_ids:
            raise ValidationError("Daily risk input contains a duplicate event ID")
        if grain in grains:
            raise ValidationError("Daily risk input contains more than one close for a source date")
        event_ids.add(event.event_id)
        grains.add(grain)

    return sorted(
        validated,
        key=lambda event: (event.source, event.symbol, event.ts_event, event.event_id),
    )


def _calculation_id(
    metric: str,
    events: list[MarketEvent],
    parameters: Mapping[str, Any],
) -> str:
    payload = {
        "event_ids": [event.event_id for event in events],
        "metric": metric,
        "model_version": MODEL_VERSION,
        "parameters": dict(sorted(parameters.items())),
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-{metric}-{digest}"


def _latest_ingest(events: list[MarketEvent]) -> datetime:
    return max(event.ts_ingest.astimezone(timezone.utc) for event in events)


def _in_output_range(event: MarketEvent, start_date: date | None) -> bool:
    return start_date is None or event.ts_event.astimezone(timezone.utc).date() >= start_date


def build_daily_risk_outputs(
    events: Iterable[EventInput],
    *,
    volatility_window: int = 20,
    var_window: int = 252,
    var_confidence: float = 0.95,
    start_date: date | None = None,
    end_date: date | None = None,
) -> DailyRiskOutputs:
    """Build versioned daily close analytics from immutable market events.

    Outputs are deterministic for the same ordered event history. A late historical
    backfill changes the input-event fingerprint and calculation timestamp, so a
    corrected analytical version remains distinguishable from an earlier result.
    """

    volatility_window = _require_integer(
        volatility_window, "volatility_window", 2, TRADING_DAYS_PER_YEAR
    )
    var_window = _require_integer(
        var_window, "var_window", 2, 10 * TRADING_DAYS_PER_YEAR
    )
    confidence = _require_confidence(var_confidence)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    validated = _normalise_events(events, end_date)
    returns_records: list[dict[str, Any]] = []
    volatility_records: list[dict[str, Any]] = []
    summary_records: list[dict[str, Any]] = []

    grouped: dict[tuple[str, str], list[MarketEvent]] = {}
    for event in validated:
        grouped.setdefault((event.source, event.symbol), []).append(event)

    for (source, symbol), group in sorted(grouped.items()):
        if len(group) < 2:
            continue

        prices = pd.Series([event.price for event in group], dtype="float64")
        returns = prices.pct_change(fill_method=None)
        rolling_volatility = (
            returns.rolling(
                window=volatility_window,
                min_periods=volatility_window,
            ).std()
            * math.sqrt(TRADING_DAYS_PER_YEAR)
        )
        drawdown = prices / prices.cummax() - 1.0
        maximum_drawdown = drawdown.cummin()

        for index in range(1, len(group)):
            current = group[index]
            if not _in_output_range(current, start_date):
                continue

            previous = group[index - 1]
            return_value = float(returns.iloc[index])
            return_inputs = [previous, current]
            returns_records.append(
                {
                    "model_version": MODEL_VERSION,
                    "calculation_id": _calculation_id("return", return_inputs, {}),
                    "source": source,
                    "symbol": symbol,
                    "source_event_id": current.event_id,
                    "previous_source_event_id": previous.event_id,
                    "ts_event": current.ts_event,
                    "ts_ingest": _latest_ingest(return_inputs),
                    "return_1d": return_value,
                }
            )

            volatility_value: float | None = None
            if index >= volatility_window:
                volatility_value = float(rolling_volatility.iloc[index])
                volatility_inputs = group[index - volatility_window : index + 1]
                volatility_records.append(
                    {
                        "model_version": MODEL_VERSION,
                        "calculation_id": _calculation_id(
                            "volatility",
                            volatility_inputs,
                            {
                                "annualization_days": TRADING_DAYS_PER_YEAR,
                                "window": volatility_window,
                            },
                        ),
                        "source": source,
                        "symbol": symbol,
                        "source_event_id": current.event_id,
                        "window_start": volatility_inputs[0].ts_event,
                        "window_end": current.ts_event,
                        "window_observations": volatility_window,
                        "annualization_days": TRADING_DAYS_PER_YEAR,
                        "ts_event": current.ts_event,
                        "ts_ingest": _latest_ingest(volatility_inputs),
                        "volatility_annualized": volatility_value,
                    }
                )

            var_start = max(1, index - var_window + 1)
            var_slice = returns.iloc[var_start : index + 1].dropna()
            var_inputs = group[var_start - 1 : index + 1]
            var_loss = (
                max(0.0, -value_at_risk(var_slice, confidence=confidence))
                if len(var_slice) >= 2
                else None
            )
            summary_inputs = group[: index + 1]
            full_history_ready = index >= max(volatility_window, var_window)
            summary_records.append(
                {
                    "model_version": MODEL_VERSION,
                    "calculation_id": _calculation_id(
                        "summary",
                        summary_inputs,
                        {
                            "annualization_days": TRADING_DAYS_PER_YEAR,
                            "var_confidence": confidence,
                            "var_window": var_window,
                            "volatility_window": volatility_window,
                        },
                    ),
                    "source": source,
                    "symbol": symbol,
                    "source_event_id": current.event_id,
                    "ts_event": current.ts_event,
                    "ts_ingest": _latest_ingest(summary_inputs),
                    "price_close": float(current.price),
                    "return_1d": return_value,
                    "volatility_annualized": volatility_value,
                    "historical_var_loss": var_loss,
                    "var_confidence": confidence,
                    "var_window": var_window,
                    "var_observations": len(var_slice),
                    "maximum_drawdown": float(maximum_drawdown.iloc[index]),
                    "price_observations": index + 1,
                    "return_observations": index,
                    "history_status": "ready" if full_history_ready else "partial",
                    "input_first_event_id": summary_inputs[0].event_id,
                    "input_last_event_id": summary_inputs[-1].event_id,
                    "var_input_first_event_id": var_inputs[0].event_id,
                }
            )

    if not returns_records or not summary_records:
        raise ValidationError("No daily risk outputs matched the requested date range")

    return DailyRiskOutputs(
        returns=tuple(returns_records),
        volatility=tuple(volatility_records),
        risk_summary=tuple(summary_records),
    )
