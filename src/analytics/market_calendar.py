from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..common.config import load_yaml
from ..common.exceptions import ValidationError
from ..ingestion.schemas import MarketEvent

MODEL_VERSION = "market-freshness-v1"
MAX_CALENDAR_DAYS = 3_700
MAX_HOLIDAYS = 1_000
MAX_EARLY_CLOSES = 1_000
MAX_MISSING_SESSIONS = 2_500


@dataclass(frozen=True, slots=True)
class MarketCalendar:
    calendar_id: str
    timezone_name: str
    valid_from: date
    valid_to: date
    session_weekdays: tuple[int, ...]
    holidays: frozenset[date]
    regular_close_time: time
    early_closes: Mapping[date, time]

    @property
    def fingerprint(self) -> str:
        payload = {
            "calendar_id": self.calendar_id,
            "early_closes": {
                day.isoformat(): close.isoformat(timespec="minutes")
                for day, close in sorted(self.early_closes.items())
            },
            "holidays": sorted(day.isoformat() for day in self.holidays),
            "regular_close_time": self.regular_close_time.isoformat(
                timespec="minutes"
            ),
            "session_weekdays": list(self.session_weekdays),
            "timezone": self.timezone_name,
            "valid_from": self.valid_from.isoformat(),
            "valid_to": self.valid_to.isoformat(),
        }
        digest = hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()[:24]
        return f"market-calendar-{digest}"

    def require_covered(self, value: date) -> None:
        if not self.valid_from <= value < self.valid_to:
            raise ValidationError(
                f"date {value.isoformat()} is outside calendar coverage"
            )

    def day_type(self, value: date) -> str:
        self.require_covered(value)
        if value in self.holidays:
            return "holiday"
        if value.weekday() not in self.session_weekdays:
            return "weekend"
        return "session"

    def is_session(self, value: date) -> bool:
        return self.day_type(value) == "session"

    def session_close_time(self, value: date) -> time:
        if not self.is_session(value):
            raise ValidationError(
                f"date {value.isoformat()} is not a trading session"
            )
        return self.early_closes.get(value, self.regular_close_time)

    def sessions_between(self, start_date: date, end_date: date) -> tuple[date, ...]:
        if start_date > end_date:
            raise ValidationError("start_date must be on or before end_date")
        self.require_covered(start_date)
        self.require_covered(end_date)
        span = (end_date - start_date).days + 1
        if span > MAX_CALENDAR_DAYS:
            raise ValidationError(
                "market-calendar request exceeds the calendar-day limit"
            )
        sessions: list[date] = []
        current = start_date
        while current <= end_date:
            if self.is_session(current):
                sessions.append(current)
            current += timedelta(days=1)
        return tuple(sessions)

    def latest_expected_session(self, as_of_date: date) -> date:
        self.require_covered(as_of_date)
        current = as_of_date
        while current >= self.valid_from:
            if self.is_session(current):
                return current
            current -= timedelta(days=1)
        raise ValidationError(
            "calendar coverage contains no expected session on or before as_of_date"
        )


@dataclass(frozen=True, slots=True)
class MarketFreshnessOutput:
    record: dict[str, Any]
    diagnostics: Mapping[str, Any]


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def _calendar_date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
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


def _clock_time(value: Any, label: str) -> time:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use HH:MM")
    try:
        parsed = time.fromisoformat(value.strip())
    except ValueError:
        raise ValidationError(f"{label} must use HH:MM") from None
    if parsed.second or parsed.microsecond or parsed.tzinfo is not None:
        raise ValidationError(f"{label} must use local HH:MM without seconds")
    if value.strip() != parsed.isoformat(timespec="minutes"):
        raise ValidationError(f"{label} must use HH:MM")
    return parsed


def parse_market_calendar(
    payload: Mapping[str, Any],
    calendar_id: str,
) -> MarketCalendar:
    if not isinstance(payload, Mapping):
        raise ValidationError("market-calendar configuration must be a mapping")
    calendar_id = _required_text(calendar_id, "calendar_id")
    calendars = payload.get("calendars")
    if not isinstance(calendars, Mapping):
        raise ValidationError(
            "market-calendar configuration must define a calendars mapping"
        )
    candidate = calendars.get(calendar_id)
    if not isinstance(candidate, Mapping):
        raise ValidationError(
            f"market calendar '{calendar_id}' is not configured"
        )

    timezone_name = _required_text(candidate.get("timezone"), "timezone")
    try:
        ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise ValidationError("market-calendar timezone is unknown") from None

    valid_from = _calendar_date(candidate.get("valid_from"), "valid_from")
    valid_to = _calendar_date(candidate.get("valid_to"), "valid_to")
    if valid_to <= valid_from:
        raise ValidationError("valid_to must be after valid_from")
    if (valid_to - valid_from).days > MAX_CALENDAR_DAYS:
        raise ValidationError(
            "market-calendar coverage exceeds the calendar-day limit"
        )

    raw_weekdays = candidate.get("session_weekdays")
    if (
        not isinstance(raw_weekdays, Sequence)
        or isinstance(raw_weekdays, (str, bytes))
        or not raw_weekdays
    ):
        raise ValidationError("session_weekdays must be a non-empty sequence")
    if any(type(item) is not int or not 0 <= item <= 6 for item in raw_weekdays):
        raise ValidationError(
            "session_weekdays must contain integers between 0 and 6"
        )
    parsed_weekdays = cast(Sequence[int], raw_weekdays)
    session_weekdays = tuple(sorted(set(parsed_weekdays)))
    if len(session_weekdays) != len(parsed_weekdays):
        raise ValidationError("session_weekdays must not contain duplicates")

    raw_holidays = candidate.get("holidays", [])
    if (
        not isinstance(raw_holidays, Sequence)
        or isinstance(raw_holidays, (str, bytes))
        or len(raw_holidays) > MAX_HOLIDAYS
    ):
        raise ValidationError("holidays must be a bounded sequence")
    holidays = tuple(
        _calendar_date(value, f"holiday[{index}]")
        for index, value in enumerate(raw_holidays)
    )
    if len(set(holidays)) != len(holidays):
        raise ValidationError("holidays must not contain duplicates")
    if any(not valid_from <= value < valid_to for value in holidays):
        raise ValidationError("holiday is outside calendar coverage")

    regular_close_time = _clock_time(
        candidate.get("regular_close_time", "16:00"),
        "regular_close_time",
    )
    raw_early_closes = candidate.get("early_closes", {})
    if (
        not isinstance(raw_early_closes, Mapping)
        or len(raw_early_closes) > MAX_EARLY_CLOSES
    ):
        raise ValidationError("early_closes must be a bounded mapping")
    early_closes: dict[date, time] = {}
    for raw_date, raw_close in raw_early_closes.items():
        close_date = _calendar_date(raw_date, "early_close date")
        close_time = _clock_time(
            raw_close,
            f"early close {close_date.isoformat()}",
        )
        if close_date in early_closes:
            raise ValidationError("early_closes must not contain duplicate dates")
        if not valid_from <= close_date < valid_to:
            raise ValidationError("early close is outside calendar coverage")
        if close_date in holidays or close_date.weekday() not in session_weekdays:
            raise ValidationError("early close must be a configured session date")
        if close_time >= regular_close_time:
            raise ValidationError(
                "early close time must be before the regular close"
            )
        early_closes[close_date] = close_time

    return MarketCalendar(
        calendar_id=calendar_id,
        timezone_name=timezone_name,
        valid_from=valid_from,
        valid_to=valid_to,
        session_weekdays=session_weekdays,
        holidays=frozenset(holidays),
        regular_close_time=regular_close_time,
        early_closes=MappingProxyType(dict(early_closes)),
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


def _event_signature(event: MarketEvent) -> tuple[Any, ...]:
    return (
        event.event_id,
        event.symbol,
        event.price,
        event.volume,
        event.ts_event,
        event.ts_ingest,
        event.source,
    )


def _input_fingerprint(events: Iterable[MarketEvent]) -> str:
    digest = hashlib.sha256()
    for event in events:
        payload = {
            "event_id": event.event_id,
            "price": event.price,
            "source": event.source,
            "symbol": event.symbol,
            "ts_event": event.ts_event.isoformat(),
            "ts_ingest": event.ts_ingest.isoformat(),
            "volume": event.volume,
        }
        digest.update(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def build_market_freshness(
    events: Iterable[MarketEvent],
    *,
    calendar: MarketCalendar,
    source: str,
    symbol: str,
    as_of_date: date,
) -> MarketFreshnessOutput:
    source = _required_text(source, "source")
    symbol = _required_text(symbol, "symbol")
    as_of_date = _calendar_date(as_of_date, "as_of_date")
    calendar.require_covered(as_of_date)

    by_date: dict[date, MarketEvent] = {}
    seen_event_ids: dict[str, tuple[Any, ...]] = {}
    for event in events:
        if not isinstance(event, MarketEvent):
            raise ValidationError("market-freshness input must contain MarketEvent rows")
        if event.source != source or event.symbol != symbol:
            continue
        event_date = event.ts_event.astimezone(timezone.utc).date()
        if event_date > as_of_date:
            continue
        calendar.require_covered(event_date)
        if event.ts_event.astimezone(timezone.utc).time() != time.min:
            raise ValidationError(
                "daily market observations must use UTC midnight"
            )
        if not calendar.is_session(event_date):
            raise ValidationError(
                "daily market observation falls on a non-session date"
            )
        signature = _event_signature(event)
        previous = seen_event_ids.get(event.event_id)
        if previous is not None:
            if previous != signature:
                raise ValidationError(
                    "market event IDs must not contain conflicting records"
                )
            continue
        seen_event_ids[event.event_id] = signature
        existing = by_date.get(event_date)
        if existing is not None and _event_signature(existing) != signature:
            raise ValidationError(
                "daily market observations must be unique per session date"
            )
        by_date[event_date] = event

    if not by_date:
        raise ValidationError(
            "no daily market observations matched the freshness request"
        )

    ordered_dates = sorted(by_date)
    ordered_events = tuple(by_date[value] for value in ordered_dates)
    first_observation = ordered_dates[0]
    latest_observation = ordered_dates[-1]
    expected_sessions = calendar.sessions_between(
        first_observation,
        as_of_date,
    )
    if not expected_sessions:
        raise ValidationError(
            "calendar contains no expected sessions for the observation range"
        )
    expected_latest = expected_sessions[-1]
    observed_dates = set(ordered_dates)
    missing_sessions = tuple(
        value for value in expected_sessions if value not in observed_dates
    )
    if len(missing_sessions) > MAX_MISSING_SESSIONS:
        raise ValidationError(
            "market-freshness evidence exceeds the missing-session limit"
        )
    trailing_missing = tuple(
        value for value in missing_sessions if value > latest_observation
    )
    if trailing_missing:
        freshness_status = "stale"
    elif missing_sessions:
        freshness_status = "gap_detected"
    else:
        freshness_status = "current"

    input_fingerprint = _input_fingerprint(ordered_events)
    calculation_payload = {
        "as_of_date": as_of_date.isoformat(),
        "calendar_fingerprint": calendar.fingerprint,
        "input_fingerprint": input_fingerprint,
        "model_version": MODEL_VERSION,
        "source": source,
        "symbol": symbol,
    }
    calculation_digest = hashlib.sha256(
        json.dumps(
            calculation_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    close_time = calendar.session_close_time(expected_latest)
    record = {
        "model_version": MODEL_VERSION,
        "calculation_id": f"{MODEL_VERSION}-calculation-{calculation_digest}",
        "calendar_id": calendar.calendar_id,
        "calendar_fingerprint": calendar.fingerprint,
        "calendar_timezone": calendar.timezone_name,
        "calendar_valid_from": calendar.valid_from,
        "calendar_valid_to": calendar.valid_to,
        "source": source,
        "symbol": symbol,
        "as_of_date": as_of_date,
        "as_of_day_type": calendar.day_type(as_of_date),
        "ts_event": datetime.combine(as_of_date, time.min, timezone.utc),
        "ts_ingest": max(event.ts_ingest for event in ordered_events),
        "first_observation_date": first_observation,
        "latest_observation_date": latest_observation,
        "expected_latest_session_date": expected_latest,
        "expected_session_close_time": close_time.isoformat(timespec="minutes"),
        "expected_session_is_early_close": expected_latest in calendar.early_closes,
        "observation_count": len(ordered_events),
        "expected_session_count": len(expected_sessions),
        "missing_session_count": len(missing_sessions),
        "trailing_missing_session_count": len(trailing_missing),
        "missing_sessions_json": json.dumps(
            [value.isoformat() for value in missing_sessions],
            separators=(",", ":"),
        ),
        "freshness_status": freshness_status,
        "input_fingerprint": input_fingerprint,
    }
    diagnostics = {
        "as_of_day_type": record["as_of_day_type"],
        "expected_latest_session_date": expected_latest.isoformat(),
        "freshness_status": freshness_status,
        "missing_session_count": len(missing_sessions),
        "trailing_missing_session_count": len(trailing_missing),
    }
    return MarketFreshnessOutput(record=record, diagnostics=diagnostics)
