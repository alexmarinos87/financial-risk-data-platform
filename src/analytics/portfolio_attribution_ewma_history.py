from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import date
from typing import Any

from ..common.exceptions import ValidationError
from .portfolio_attribution import PortfolioReturnInput, _current_records
from .portfolio_attribution_ewma import (
    COVARIANCE_METHOD,
    MODEL_VERSION,
    build_portfolio_ewma_attribution,
)
from .portfolio_attribution_history import (
    MAX_HISTORY_SNAPSHOTS,
    PortfolioAttributionHistoryOutput,
    _covariance_window,
    _normalised_input_record,
    _snapshot_limit,
)
from .portfolio_risk import PortfolioDefinition


def build_portfolio_ewma_attribution_history(
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

    current, matched_records = _current_records(records, definition, end_date)
    if len(current) < covariance_window:
        raise ValidationError(
            "EWMA portfolio attribution history requires at least "
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
            "no EWMA portfolio attribution snapshots matched the requested date range"
        )
    if len(selected_end_indexes) > max_snapshots:
        raise ValidationError(
            "EWMA portfolio attribution history exceeds max_snapshots; provide a "
            "later start_date or split the bounded request"
        )

    snapshots: list[dict[str, Any]] = []
    for end_index in selected_end_indexes:
        window = current[
            end_index - covariance_window + 1 : end_index + 1
        ]
        output = build_portfolio_ewma_attribution(
            (_normalised_input_record(record) for record in window),
            definition=definition,
            covariance_window=covariance_window,
            end_date=window[-1]["ts_event"].date(),
        )
        snapshots.append(output.snapshot)

    first_snapshot = snapshots[0]
    last_snapshot = snapshots[-1]
    diagnostics: Mapping[str, Any] = {
        "model_version": MODEL_VERSION,
        "covariance_method": COVARIANCE_METHOD,
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
