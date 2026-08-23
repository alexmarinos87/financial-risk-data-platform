from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..common.config import load_yaml
from ..common.exceptions import ValidationError

MAX_CALENDAR_SPAN_DAYS = 20 * 366
MAX_MISSING_SESSIONS = 2_500


@dataclass(frozen=True, slots=True)
class MarketCalendar:
    calendar_id: str
    timezone_name: str
    close_time: time
    grace_minutes: int
    weekend_days: frozenset[int]
    holidays: frozenset[date]

    @property
    def timezone(self) -> ZoneInfo:
        try:
            return ZoneInfo(self.timezone_name)
        except ZoneInfoNotFoundError:
            raise ValidationError(
                f"calendar '{self.calendar_id}' has an unknown timezone"
            ) from None

    def classify(self, value: date) -> str:
        if value.weekday() in self.weekend_days:
            return "weekend"
        if value in self.holidays:
            return "holiday"
        return "trading_session"

    def is_session(self, value: date) -> bool:
        return self.classify(value) == "trading_session"


@dataclass(frozen=True, slots=True)
class MarketFreshnessEvaluation:
    calendar_id: str
    source: str
    symbol: str
    as_of_utc: datetime
    as_of_local: datetime
    expected_latest_session: date
    latest_observation_date: date
    status: str
    stale_session_count: int
    missing_session_dates: tuple[date, ...]
    observed_session_count: int
    observed_non_session_dates: tuple[date, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "as_of_local": self.as_of_local.isoformat(),
            "as_of_utc": self.as_of_utc.isoformat(),
            "calendar_id": self.calendar_id,
            "expected_latest_session": self.expected_latest_session.isoformat(),
            "latest_observation_date": self.latest_observation_date.isoformat(),
            "missing_session_count": len(self.missing_session_dates),
            "missing_session_dates": [
                value.isoformat() for value in self.missing_session_dates
            ],
            "observed_non_session_dates": [
                value.isoformat() for value in self.observed_non_session_dates
            ],
            "observed_session_count": self.observed_session_count,
            "source": self.source,
            "stale_session_count": self.stale_session_count,
            "status": self.status,
            "symbol": self.symbol,
        }


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    return value.strip()


def _clock(value: Any, label: str) -> time:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use HH:MM")
    try:
        parsed = time.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(f"{label} must use HH:MM") from None
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise ValidationError(f"{label} must use local HH:MM without seconds")
    return parsed


def _calendar_date(value: Any, label: str) -> date:
    if type(value) is date:
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if value.strip() != parsed.isoformat():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def parse_market_calendar(
    payload: Mapping[str, Any],
    calendar_id: str,
) -> MarketCalendar:
    calendars = payload.get("calendars")
    if not isinstance(calendars, Mapping):
        raise ValidationError("market-calendar configuration must define calendars")
    candidate = calendars.get(calendar_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(f"calendar '{calendar_id}' is not configured")

    timezone_name = _required_text(
        candidate.get("timezone"),
        f"calendar {calendar_id}.timezone",
    )
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise ValidationError(
            f"calendar '{calendar_id}' has an unknown timezone"
        ) from None

    grace_minutes = candidate.get("grace_minutes", 0)
    if type(grace_minutes) is not int or not 0 <= grace_minutes <= 24 * 60:
        raise ValidationError("grace_minutes must be an integer from 0 to 1440")

    weekend_raw = candidate.get("weekend_days", [5, 6])
    if (
        not isinstance(weekend_raw, Sequence)
        or isinstance(weekend_raw, (str, bytes))
        or not weekend_raw
        or any(type(value) is not int or not 0 <= value <= 6 for value in weekend_raw)
    ):
        raise ValidationError("weekend_days must contain weekday integers 0 to 6")
    weekend_days = frozenset(weekend_raw)

    holidays_raw = candidate.get("holidays", [])
    if not isinstance(holidays_raw, Sequence) or isinstance(
        holidays_raw, (str, bytes)
    ):
        raise ValidationError("holidays must be a sequence of calendar dates")
    holidays = frozenset(
        _calendar_date(value, f"calendar {calendar_id}.holiday")
        for value in holidays_raw
    )
    if any(value.weekday() in weekend_days for value in holidays):
        raise ValidationError("configured holidays must not duplicate weekend dates")

    return MarketCalendar(
        calendar_id=_required_text(calendar_id, "calendar_id"),
        timezone_name=timezone_name,
        close_time=_clock(
            candidate.get("close_time"),
            f"calendar {calendar_id}.close_time",
        ),
        grace_minutes=grace_minutes,
        weekend_days=weekend_days,
        holidays=holidays,
    )


def load_market_calendar(path: Path, calendar_id: str) -> MarketCalendar:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "market-calendar configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("market-calendar configuration must be a mapping")
    return parse_market_calendar(payload, calendar_id)


def resolve_instrument_calendar(
    payload: Mapping[str, Any],
    *,
    source: str,
    symbol: str,
) -> str:
    instruments = payload.get("instruments")
    if not isinstance(instruments, Mapping):
        raise ValidationError("market-calendar configuration must define instruments")
    key = f"{_required_text(source, 'source')}:{_required_text(symbol, 'symbol').upper()}"
    calendar_id = instruments.get(key)
    return _required_text(calendar_id, f"calendar mapping for {key}")


def load_instrument_calendar(
    path: Path,
    *,
    source: str,
    symbol: str,
) -> MarketCalendar:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "market-calendar configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("market-calendar configuration must be a mapping")
    calendar_id = resolve_instrument_calendar(
        payload,
        source=source,
        symbol=symbol,
    )
    return parse_market_calendar(payload, calendar_id)


def _previous_session(calendar: MarketCalendar, candidate: date) -> date:
    for offset in range(1, MAX_CALENDAR_SPAN_DAYS + 1):
        value = candidate - timedelta(days=offset)
        if calendar.is_session(value):
            return value
    raise ValidationError("market calendar has no previous session within the bound")


def expected_latest_session(
    calendar: MarketCalendar,
    *,
    as_of: datetime,
) -> tuple[date, datetime]:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValidationError("as_of must be timezone-aware")
    as_of_utc = as_of.astimezone(timezone.utc)
    local = as_of_utc.astimezone(calendar.timezone)
    local_date = local.date()

    if not calendar.is_session(local_date):
        return _previous_session(calendar, local_date), local

    close = datetime.combine(
        local_date,
        calendar.close_time,
        tzinfo=calendar.timezone,
    ) + timedelta(minutes=calendar.grace_minutes)
    if local >= close:
        return local_date, local
    return _previous_session(calendar, local_date), local


def _session_dates(
    calendar: MarketCalendar,
    start: date,
    end: date,
) -> tuple[date, ...]:
    if start > end:
        return ()
    span = (end - start).days + 1
    if span > MAX_CALENDAR_SPAN_DAYS:
        raise ValidationError("market freshness date span exceeds the safety bound")
    return tuple(
        start + timedelta(days=offset)
        for offset in range(span)
        if calendar.is_session(start + timedelta(days=offset))
    )


def evaluate_market_freshness(
    observed_dates: Iterable[date],
    *,
    source: str,
    symbol: str,
    calendar: MarketCalendar,
    as_of: datetime,
    max_missing_sessions: int = MAX_MISSING_SESSIONS,
) -> MarketFreshnessEvaluation:
    if type(max_missing_sessions) is not int or not 0 <= max_missing_sessions <= MAX_MISSING_SESSIONS:
        raise ValidationError(
            f"max_missing_sessions must be between 0 and {MAX_MISSING_SESSIONS}"
        )
    source = _required_text(source, "source")
    symbol = _required_text(symbol, "symbol").upper()
    materialized = tuple(observed_dates)
    if not materialized or any(type(value) is not date for value in materialized):
        raise ValidationError("observed_dates must contain calendar dates")
    unique = tuple(sorted(set(materialized)))

    expected, local = expected_latest_session(calendar, as_of=as_of)
    future = [value for value in unique if value > expected]
    if future:
        raise ValidationError(
            "observed dates contain a session after the expected latest session"
        )

    latest = unique[-1]
    non_sessions = tuple(
        value for value in unique if not calendar.is_session(value)
    )
    observed_sessions = {value for value in unique if calendar.is_session(value)}
    sessions = _session_dates(calendar, unique[0], expected)
    missing = tuple(value for value in sessions if value not in observed_sessions)
    if len(missing) > max_missing_sessions:
        raise ValidationError(
            "market freshness missing-session evidence exceeds the request bound"
        )
    stale_sessions = sum(1 for value in sessions if value > latest)
    status = "current" if latest == expected else "stale"

    return MarketFreshnessEvaluation(
        calendar_id=calendar.calendar_id,
        source=source,
        symbol=symbol,
        as_of_utc=as_of.astimezone(timezone.utc),
        as_of_local=local,
        expected_latest_session=expected,
        latest_observation_date=latest,
        status=status,
        stale_session_count=stale_sessions,
        missing_session_dates=missing,
        observed_session_count=len(observed_sessions),
        observed_non_session_dates=non_sessions,
    )
