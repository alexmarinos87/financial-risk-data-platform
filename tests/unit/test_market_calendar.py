from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.analytics.market_calendar import (
    MODEL_VERSION,
    build_market_freshness,
    parse_market_calendar,
)
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent


def _payload() -> dict[str, object]:
    return {
        "calendars": {
            "XNYS": {
                "timezone": "America/New_York",
                "valid_from": "2026-01-01",
                "valid_to": "2027-01-01",
                "session_weekdays": [0, 1, 2, 3, 4],
                "regular_close_time": "16:00",
                "holidays": [
                    "2026-01-01",
                    "2026-01-19",
                    "2026-02-16",
                    "2026-04-03",
                    "2026-05-25",
                    "2026-06-19",
                    "2026-07-03",
                    "2026-09-07",
                    "2026-11-26",
                    "2026-12-25",
                ],
                "early_closes": {
                    "2026-11-27": "13:00",
                    "2026-12-24": "13:00",
                },
            }
        }
    }


def _calendar():
    return parse_market_calendar(_payload(), "XNYS")


def _event(day: int, *, month: int = 1, price: float = 100.0) -> MarketEvent:
    event_date = date(2026, month, day)
    timestamp = datetime.combine(event_date, datetime.min.time(), timezone.utc)
    return MarketEvent(
        event_id=f"event-{event_date.isoformat()}",
        symbol="AAPL",
        price=price,
        volume=100,
        ts_event=timestamp,
        ts_ingest=timestamp.replace(hour=12),
        source="alpha_vantage",
    )


def test_calendar_distinguishes_sessions_weekends_holidays_and_early_closes() -> None:
    calendar = _calendar()

    assert calendar.day_type(date(2026, 1, 2)) == "session"
    assert calendar.day_type(date(2026, 1, 3)) == "weekend"
    assert calendar.day_type(date(2026, 1, 19)) == "holiday"
    assert calendar.session_close_time(date(2026, 11, 27)).isoformat(
        timespec="minutes"
    ) == "13:00"
    assert calendar.session_close_time(date(2026, 11, 30)).isoformat(
        timespec="minutes"
    ) == "16:00"


def test_weekend_as_of_date_uses_prior_session_without_marking_stale() -> None:
    output = build_market_freshness(
        [_event(8), _event(9)],
        calendar=_calendar(),
        source="alpha_vantage",
        symbol="AAPL",
        as_of_date=date(2026, 1, 10),
    )

    assert output.record["model_version"] == MODEL_VERSION
    assert output.record["as_of_day_type"] == "weekend"
    assert output.record["expected_latest_session_date"] == date(2026, 1, 9)
    assert output.record["freshness_status"] == "current"
    assert output.record["missing_session_count"] == 0


def test_missing_latest_session_is_stale_and_internal_gap_is_separate() -> None:
    stale = build_market_freshness(
        [_event(8), _event(9)],
        calendar=_calendar(),
        source="alpha_vantage",
        symbol="AAPL",
        as_of_date=date(2026, 1, 12),
    )
    assert stale.record["freshness_status"] == "stale"
    assert stale.record["trailing_missing_session_count"] == 1

    gap = build_market_freshness(
        [_event(5), _event(7)],
        calendar=_calendar(),
        source="alpha_vantage",
        symbol="AAPL",
        as_of_date=date(2026, 1, 7),
    )
    assert gap.record["freshness_status"] == "gap_detected"
    assert gap.record["missing_session_count"] == 1
    assert gap.record["trailing_missing_session_count"] == 0


def test_observation_on_non_session_and_conflicting_identity_fail_closed() -> None:
    with pytest.raises(ValidationError, match="non-session"):
        build_market_freshness(
            [_event(3)],
            calendar=_calendar(),
            source="alpha_vantage",
            symbol="AAPL",
            as_of_date=date(2026, 1, 3),
        )

    first = _event(5)
    conflict = _event(5, price=101.0)
    with pytest.raises(ValidationError, match="conflicting"):
        build_market_freshness(
            [first, conflict],
            calendar=_calendar(),
            source="alpha_vantage",
            symbol="AAPL",
            as_of_date=date(2026, 1, 5),
        )


def test_calendar_fingerprint_and_calculation_identity_are_deterministic() -> None:
    first_calendar = _calendar()
    payload = _payload()
    payload["calendars"]["XNYS"]["holidays"] = list(
        reversed(payload["calendars"]["XNYS"]["holidays"])
    )
    second_calendar = parse_market_calendar(payload, "XNYS")
    assert first_calendar.fingerprint == second_calendar.fingerprint

    forward = build_market_freshness(
        [_event(8), _event(9)],
        calendar=first_calendar,
        source="alpha_vantage",
        symbol="AAPL",
        as_of_date=date(2026, 1, 10),
    )
    reverse = build_market_freshness(
        [_event(9), _event(8)],
        calendar=second_calendar,
        source="alpha_vantage",
        symbol="AAPL",
        as_of_date=date(2026, 1, 10),
    )
    assert forward.record == reverse.record


def test_invalid_calendar_coverage_and_early_close_fail_closed() -> None:
    payload = _payload()
    payload["calendars"]["XNYS"]["early_closes"] = {
        "2026-11-26": "13:00"
    }
    with pytest.raises(ValidationError, match="session date"):
        parse_market_calendar(payload, "XNYS")

    with pytest.raises(ValidationError, match="outside calendar coverage"):
        _calendar().day_type(date(2027, 1, 1))
