from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.analytics.market_calendar_freshness import (
    MarketCalendar,
    evaluate_market_freshness,
    expected_latest_session,
    parse_market_calendar,
)
from src.common.exceptions import ValidationError


def _calendar() -> MarketCalendar:
    return parse_market_calendar(
        {
            "calendars": {
                "TEST": {
                    "timezone": "America/New_York",
                    "close_time": "16:00",
                    "grace_minutes": 30,
                    "weekend_days": [5, 6],
                    "holidays": ["2026-01-01"],
                }
            }
        },
        "TEST",
    )


def test_classifies_sessions_weekends_and_holidays() -> None:
    calendar = _calendar()

    assert calendar.classify(date(2026, 1, 1)) == "holiday"
    assert calendar.classify(date(2026, 1, 3)) == "weekend"
    assert calendar.classify(date(2026, 1, 2)) == "trading_session"


def test_expected_session_waits_for_close_and_grace() -> None:
    calendar = _calendar()

    before_close, local_before = expected_latest_session(
        calendar,
        as_of=datetime(2026, 1, 5, 20, 0, tzinfo=timezone.utc),
    )
    after_grace, local_after = expected_latest_session(
        calendar,
        as_of=datetime(2026, 1, 5, 21, 31, tzinfo=timezone.utc),
    )

    assert before_close == date(2026, 1, 2)
    assert local_before.hour == 15
    assert after_grace == date(2026, 1, 5)
    assert local_after.hour == 16


def test_weekend_and_holiday_resolve_to_previous_session() -> None:
    calendar = _calendar()

    weekend, _ = expected_latest_session(
        calendar,
        as_of=datetime(2026, 1, 4, 18, tzinfo=timezone.utc),
    )
    holiday, _ = expected_latest_session(
        calendar,
        as_of=datetime(2026, 1, 1, 18, tzinfo=timezone.utc),
    )

    assert weekend == date(2026, 1, 2)
    assert holiday == date(2025, 12, 31)


def test_evaluation_distinguishes_stale_and_missing_sessions() -> None:
    output = evaluate_market_freshness(
        [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 7)],
        source="alpha_vantage",
        symbol="aapl",
        calendar=_calendar(),
        as_of=datetime(2026, 1, 8, 22, tzinfo=timezone.utc),
    )

    assert output.status == "stale"
    assert output.expected_latest_session == date(2026, 1, 8)
    assert output.latest_observation_date == date(2026, 1, 7)
    assert output.stale_session_count == 1
    assert output.missing_session_dates == (
        date(2026, 1, 6),
        date(2026, 1, 8),
    )
    assert output.symbol == "AAPL"


def test_current_evaluation_does_not_treat_weekend_as_missing() -> None:
    output = evaluate_market_freshness(
        [date(2026, 1, 2), date(2026, 1, 5)],
        source="alpha_vantage",
        symbol="IBM",
        calendar=_calendar(),
        as_of=datetime(2026, 1, 5, 22, tzinfo=timezone.utc),
    )

    assert output.status == "current"
    assert output.stale_session_count == 0
    assert output.missing_session_dates == ()


def test_non_session_observations_remain_explicit_evidence() -> None:
    output = evaluate_market_freshness(
        [date(2026, 1, 2), date(2026, 1, 3), date(2026, 1, 5)],
        source="alpha_vantage",
        symbol="IBM",
        calendar=_calendar(),
        as_of=datetime(2026, 1, 5, 22, tzinfo=timezone.utc),
    )

    assert output.observed_non_session_dates == (date(2026, 1, 3),)


def test_invalid_calendars_and_future_observations_fail_closed() -> None:
    with pytest.raises(ValidationError, match="unknown timezone"):
        parse_market_calendar(
            {
                "calendars": {
                    "BAD": {
                        "timezone": "Not/AZone",
                        "close_time": "16:00",
                    }
                }
            },
            "BAD",
        )

    with pytest.raises(ValidationError, match="after the expected"):
        evaluate_market_freshness(
            [date(2026, 1, 6)],
            source="alpha_vantage",
            symbol="IBM",
            calendar=_calendar(),
            as_of=datetime(2026, 1, 5, 22, tzinfo=timezone.utc),
        )


def test_missing_evidence_limit_fails_before_returning_partial_output() -> None:
    with pytest.raises(ValidationError, match="exceeds the request bound"):
        evaluate_market_freshness(
            [date(2026, 1, 2)],
            source="alpha_vantage",
            symbol="IBM",
            calendar=_calendar(),
            as_of=datetime(2026, 1, 8, 22, tzinfo=timezone.utc),
            max_missing_sessions=1,
        )
