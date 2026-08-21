from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

from ..common.exceptions import ValidationError
from .portfolio_attribution import (
    MAX_COVARIANCE_WINDOW,
    PortfolioReturnInput,
    _current_records,
    build_portfolio_attribution,
)
from .portfolio_risk import PortfolioDefinition

MAX_HISTORY_SNAPSHOTS = 2_500


@dataclass(frozen=True, slots=True)
class PortfolioAttributionHistoryOutput:
    snapshots: tuple[dict[str, Any], ...]
    diagnostics: Mapping[str, Any]


def _snapshot_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_HISTORY_SNAPSHOTS:
        raise ValidationError(
            "max_snapshots must be an integer between "
            f"1 and {MAX_HISTORY_SNAPSHOTS}"
        )
    return value


def _covariance_window(value: int) -> int:
    if type(value) is not int or not 2 <= value <= MAX_COVARIANCE_WINDOW:
        raise ValidationError(
            "covariance_window must be an integer between "
            f"2 and {MAX_COVARIANCE_WINDOW}"
        )
    return value


def _normalised_input_record(record: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "model_version": record["model_version"],
        "calculation_id": record["calculation_id"],
        "portfolio_id": record["portfolio_id"],
        "base_currency": record["base_currency"],
        "definition_fingerprint": record["definition_fingerprint"],
        "weighting_method": record["weighting_method"],
        "ts_event": record["ts_event"],
        "ts_ingest": record["ts_ingest"],
        "constituent_count": record["constituent_count"],
        "weights_json": json.dumps(
            record["weights"],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "component_calculation_ids_json": json.dumps(
            record["component_calculation_ids"],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "component_returns_json": json.dumps(
            record["component_returns"],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "portfolio_return_1d": record["portfolio_return_1d"],
    }


def build_portfolio_attribution_history(
    records: Iterable[PortfolioReturnInput],
    *,
    definition: PortfolioDefinition,
    covariance_window: int = 20,
    start_date: date | None = None,
    end_date: date | None = None,
    max_snapshots: int = MAX_HISTORY_SNAPSHOTS,
) -> PortfolioAttributionHistoryOutput:
    covariance_window = _covariance_window(covariance_window)
    max_snapshots = _snapshot_limit(max_snapshots)
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    # Reuse the canonical validation and current-version selection contract.
    # This increment changes only window expansion, not input semantics.
    current, matched_records = _current_records(records, definition, end_date)
    if len(current) < covariance_window:
        raise ValidationError(
            "portfolio attribution history requires at least "
            f"{covariance_window} current portfolio return observations"
        )

    all_end_indexes = list(range(covariance_window - 1, len(current)))
    selected_end_indexes = [
        index
        for index in all_end_indexes
        if start_date is None or current[index]["ts_event"].date() >= start_date
    ]
    if not selected_end_indexes:
        raise ValidationError(
            "no portfolio attribution snapshots matched the requested date range"
        )
    if len(selected_end_indexes) > max_snapshots:
        raise ValidationError(
            "portfolio attribution history exceeds max_snapshots; provide a later "
            "start_date or split the bounded request"
        )

    snapshots: list[dict[str, Any]] = []
    for end_index in selected_end_indexes:
        window = current[
            end_index - covariance_window + 1 : end_index + 1
        ]
        output = build_portfolio_attribution(
            (_normalised_input_record(record) for record in window),
            definition=definition,
            covariance_window=covariance_window,
            end_date=window[-1]["ts_event"].date(),
        )
        snapshots.append(output.snapshot)

    first_snapshot = snapshots[0]
    last_snapshot = snapshots[-1]
    diagnostics = {
        "matched_input_records": matched_records,
        "current_portfolio_return_records": len(current),
        "covariance_window": covariance_window,
        "available_snapshot_dates": len(all_end_indexes),
        "snapshots_selected": len(snapshots),
        "snapshots_skipped_before_start_date": (
            len(all_end_indexes) - len(selected_end_indexes)
        ),
        "first_snapshot_date": first_snapshot["ts_event"].date().isoformat(),
        "last_snapshot_date": last_snapshot["ts_event"].date().isoformat(),
        "start_date": start_date.isoformat() if start_date is not None else None,
        "end_date": (
            end_date.isoformat()
            if end_date is not None
            else last_snapshot["ts_event"].date().isoformat()
        ),
        "max_snapshots": max_snapshots,
    }
    return PortfolioAttributionHistoryOutput(
        snapshots=tuple(snapshots),
        diagnostics=diagnostics,
    )
