from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.daily_risk import MODEL_VERSION, build_daily_risk_outputs
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent


def _event(
    day: int,
    price: float,
    *,
    event_id: str | None = None,
    ingested_at: datetime | None = None,
) -> MarketEvent:
    ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
    return MarketEvent(
        event_id=event_id or f"av-daily-IBM-202601{day:02d}",
        symbol="IBM",
        price=price,
        volume=1_000 + day,
        ts_event=ts_event,
        ts_ingest=ingested_at or ts_event + timedelta(hours=1),
        source="alpha_vantage",
    )


def test_build_daily_risk_outputs_calculates_versioned_metrics() -> None:
    events = [
        _event(1, 100.0),
        _event(2, 110.0),
        _event(3, 99.0),
        _event(4, 108.9),
    ]

    outputs = build_daily_risk_outputs(
        events,
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
    )

    assert len(outputs.returns) == 3
    assert outputs.returns[0]["return_1d"] == pytest.approx(0.1)
    assert outputs.returns[1]["return_1d"] == pytest.approx(-0.1)
    assert outputs.returns[0]["model_version"] == MODEL_VERSION
    assert outputs.returns[0]["calculation_id"].startswith(f"{MODEL_VERSION}-return-")

    assert len(outputs.volatility) == 2
    assert outputs.volatility[0]["window_observations"] == 2
    assert outputs.volatility[0]["volatility_annualized"] > 0

    latest = outputs.risk_summary[-1]
    assert latest["historical_var_loss"] == pytest.approx(0.09)
    assert latest["maximum_drawdown"] == pytest.approx(-0.1)
    assert latest["history_status"] == "ready"
    assert latest["price_observations"] == 4
    assert latest["return_observations"] == 3


def test_start_date_filters_outputs_but_keeps_prior_close_context() -> None:
    events = [_event(1, 100.0), _event(2, 110.0), _event(3, 121.0)]

    outputs = build_daily_risk_outputs(
        events,
        volatility_window=2,
        var_window=2,
        start_date=date(2026, 1, 3),
    )

    assert len(outputs.returns) == 1
    assert outputs.returns[0]["ts_event"].date() == date(2026, 1, 3)
    assert outputs.returns[0]["previous_source_event_id"] == events[1].event_id
    assert outputs.returns[0]["return_1d"] == pytest.approx(0.1)


def test_late_historical_backfill_creates_a_new_traceable_version() -> None:
    late_ingest = datetime(2026, 2, 1, tzinfo=timezone.utc)
    original = [_event(1, 100.0), _event(3, 121.0)]
    corrected_history = [
        _event(1, 100.0),
        _event(2, 110.0, ingested_at=late_ingest),
        _event(3, 121.0),
    ]

    original_output = build_daily_risk_outputs(
        original,
        volatility_window=2,
        var_window=2,
    ).returns[-1]
    corrected_output = build_daily_risk_outputs(
        corrected_history,
        volatility_window=2,
        var_window=2,
    ).returns[-1]

    assert original_output["calculation_id"] != corrected_output["calculation_id"]
    assert original_output["return_1d"] == pytest.approx(0.21)
    assert corrected_output["return_1d"] == pytest.approx(0.1)
    assert corrected_output["ts_ingest"] == late_ingest


def test_duplicate_source_date_is_rejected() -> None:
    with pytest.raises(ValidationError, match="more than one close"):
        build_daily_risk_outputs(
            [_event(1, 100.0), _event(1, 101.0, event_id="different-id")],
            volatility_window=2,
            var_window=2,
        )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"volatility_window": 1}, "volatility_window"),
        ({"var_window": True}, "var_window"),
        ({"var_confidence": 1.0}, "var_confidence"),
        (
            {"start_date": date(2026, 1, 3), "end_date": date(2026, 1, 2)},
            "start_date",
        ),
    ],
)
def test_invalid_options_fail_closed(kwargs: dict[str, object], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        build_daily_risk_outputs([_event(1, 100.0), _event(2, 101.0)], **kwargs)
