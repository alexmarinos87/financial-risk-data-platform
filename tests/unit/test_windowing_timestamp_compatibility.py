"""Window assignment must not depend on pandas timestamp storage resolution."""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.common.exceptions import ValidationError
from src.processing.windowing import floor_time

UTC = timezone.utc


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("aware", [False, True])
@pytest.mark.parametrize("year", [1969, 2026])
def test_representable_large_window_is_resolution_independent(
    unit: str, aware: bool, year: int
) -> None:
    # This duration fits datetime.timedelta, but not a nanosecond int64.
    instant = datetime(year, 1, 1, tzinfo=UTC if aware else None)
    ts = pd.Timestamp(instant).as_unit(unit)
    original = (ts.isoformat(), ts.unit, ts.tzinfo, ts.fold)
    interval = timedelta(minutes=200_000_000)
    epoch = datetime(1970, 1, 1, tzinfo=instant.tzinfo)
    expected = epoch if year > 1970 else epoch - interval
    result = floor_time(ts, 200_000_000)
    assert result == expected
    assert result == floor_time(instant, 200_000_000)
    assert type(result) is datetime
    assert (ts.isoformat(), ts.unit, ts.tzinfo, ts.fold) == original


@pytest.mark.parametrize("minutes", [1, 5, 7])
@pytest.mark.parametrize("window_index", [-17, 0, 5_000_000])
@pytest.mark.parametrize("nanoseconds", [-1, 0, 1])
def test_nanosecond_boundary_uses_exact_window(
    minutes: int, window_index: int, nanoseconds: int
) -> None:
    interval = timedelta(minutes=minutes)
    boundary = datetime(1970, 1, 1, tzinfo=UTC) + window_index * interval
    ts = pd.Timestamp(boundary).as_unit("ns") + pd.Timedelta(nanoseconds, unit="ns")
    expected = boundary - interval if nanoseconds < 0 else boundary
    assert floor_time(ts, minutes) == expected
    assert floor_time(floor_time(ts, minutes), minutes) == expected


@pytest.mark.parametrize(
    "text,expected,fold",
    [
        ("2026-10-25T00:30:00.000000001Z", datetime(2026, 10, 25, 0, tzinfo=UTC), 0),
        ("2026-10-25T01:30:00.000000001Z", datetime(2026, 10, 25, 1, tzinfo=UTC), 1),
        ("2026-03-29T01:30:00.000000001Z", datetime(2026, 3, 29, 1, tzinfo=UTC), 0),
    ],
)
def test_nanosecond_timestamp_preserves_display_zone_and_fold(
    text: str, expected: datetime, fold: int
) -> None:
    zone = ZoneInfo("Europe/London")
    ts = pd.Timestamp(text).tz_convert(zone)
    result = floor_time(ts, 60)
    assert result.astimezone(UTC) == expected
    assert result.tzinfo is zone
    assert result.fold == fold


@pytest.mark.parametrize("aware", [False, True])
@pytest.mark.parametrize("year", [1, 9999])
def test_timestamp_uses_the_representable_datetime_range(aware: bool, year: int) -> None:
    instant = datetime(year, 1, 1, 0, 3, tzinfo=UTC if aware else None)
    ts = pd.Timestamp(instant).as_unit("us")
    assert floor_time(ts, 5) == instant.replace(minute=0)


def test_missing_timestamp_raises_the_public_validation_error() -> None:
    with pytest.raises(ValidationError, match="^Window timestamp must be a valid datetime$"):
        floor_time(pd.NaT, 5)


@pytest.mark.parametrize("minutes", [7, 200_000_000])
def test_actual_boundary_underflow_remains_rejected(minutes: int) -> None:
    ts = pd.Timestamp(datetime.min.replace(tzinfo=UTC)).as_unit("us")
    with pytest.raises(ValidationError, match="outside datetime range"):
        floor_time(ts, minutes)
