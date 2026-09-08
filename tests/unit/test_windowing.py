from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from src.common.exceptions import ValidationError
from src.processing.windowing import floor_time

UTC = timezone.utc


def test_floor_time() -> None:
    ts = datetime(2025, 1, 1, 12, 7, tzinfo=UTC)
    assert floor_time(ts, 5) == datetime(2025, 1, 1, 12, 5, tzinfo=UTC)


@pytest.mark.parametrize("microseconds", [1, 500_000, 999_999])
def test_fraction_before_epoch_never_rounds_forward(microseconds: int) -> None:
    ts = datetime(1970, 1, 1, tzinfo=UTC) - timedelta(microseconds=microseconds)
    assert floor_time(ts, 5) == datetime(1969, 12, 31, 23, 55, tzinfo=UTC)


def test_fraction_before_negative_window_boundary() -> None:
    ts = datetime(1969, 12, 31, 23, 55, tzinfo=UTC) - timedelta(microseconds=1)
    assert floor_time(ts, 5) == datetime(1969, 12, 31, 23, 50, tzinfo=UTC)


@pytest.mark.parametrize("offset_minutes", [-210, 60, 345, 570])
@pytest.mark.parametrize("minutes", [7, 60, 90, 1440])
def test_same_instant_shares_utc_window_for_every_offset(
    offset_minutes: int, minutes: int
) -> None:
    utc_ts = datetime(2026, 1, 3, 4, 17, 59, 123456, tzinfo=UTC)
    zone = timezone(timedelta(minutes=offset_minutes))
    local_ts = utc_ts.astimezone(zone)
    result = floor_time(local_ts, minutes)
    assert result.astimezone(UTC) == floor_time(utc_ts, minutes)
    assert result.tzinfo is zone


@pytest.mark.parametrize(
    "utc_ts,expected,fold",
    [
        (datetime(2026, 10, 25, 0, 30, tzinfo=UTC), datetime(2026, 10, 25, 0, tzinfo=UTC), 0),
        (datetime(2026, 10, 25, 1, 30, tzinfo=UTC), datetime(2026, 10, 25, 1, tzinfo=UTC), 1),
        (datetime(2026, 3, 29, 1, 30, tzinfo=UTC), datetime(2026, 3, 29, 1, tzinfo=UTC), 0),
    ],
)
def test_london_clock_changes_preserve_instant_and_fold(
    utc_ts: datetime, expected: datetime, fold: int
) -> None:
    zone = ZoneInfo("Europe/London")
    result = floor_time(utc_ts.astimezone(zone), 60)
    assert result.astimezone(UTC) == expected
    assert result.tzinfo is zone
    assert result.fold == fold


@pytest.mark.parametrize("minutes", [1, 5, 7, 90, 1440, 4320])
@pytest.mark.parametrize(
    "ts",
    [
        datetime(1900, 2, 28, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime(2026, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        datetime(2500, 1, 1, 0, 0, 0, 1, tzinfo=UTC),
    ],
)
def test_floor_window_contains_timestamp_and_is_idempotent(ts: datetime, minutes: int) -> None:
    result = floor_time(ts, minutes)
    assert result <= ts < result + timedelta(minutes=minutes)
    assert floor_time(result, minutes) == result
    assert (result - datetime(1970, 1, 1, tzinfo=UTC)) % timedelta(minutes=minutes) == timedelta(0)


def test_naive_timestamp_remains_naive() -> None:
    result = floor_time(datetime(2026, 1, 1, 12, 7), 5)
    assert result == datetime(2026, 1, 1, 12, 5)
    assert result.tzinfo is None
    assert floor_time(datetime(1969, 12, 31, 23, 59, 59, 999999), 5) == datetime(
        1969, 12, 31, 23, 55
    )


@pytest.mark.parametrize("minutes", [0, -1, True, False, 1.5, "5", None])
def test_invalid_window_rejected(minutes: Any) -> None:
    with pytest.raises(ValidationError, match="positive integer"):
        floor_time(datetime(2026, 1, 1, tzinfo=UTC), minutes)


def test_unrepresentable_window_rejected() -> None:
    with pytest.raises(ValidationError, match="outside datetime range"):
        floor_time(datetime(2026, 1, 1, tzinfo=UTC), 10**20)


def test_non_datetime_rejected() -> None:
    with pytest.raises(ValidationError, match="must be a datetime"):
        floor_time("2026-01-01", 5)  # type: ignore[arg-type]


def test_timezone_conversion_overflow_rejected() -> None:
    ts = datetime.min.replace(tzinfo=timezone(timedelta(hours=1)))
    with pytest.raises(ValidationError, match="outside datetime range"):
        floor_time(ts, 5)


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        ("1969-12-31T23:59:59.999999999Z", datetime(1969, 12, 31, 23, 55, tzinfo=UTC)),
        ("2026-09-08T20:11:00Z", datetime(2026, 9, 8, 20, 10, tzinfo=UTC)),
    ],
)
def test_pipeline_pandas_timestamps(timestamp: str, expected: datetime) -> None:
    assert floor_time(pd.Timestamp(timestamp), 5) == expected
