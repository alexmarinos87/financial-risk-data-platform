"""Hardened Alpha Vantage daily adapter for the provider-neutral event contract.

The standard-library timeout limits socket operations. A monotonic guard is
checked between streamed reads, but Python's synchronous DNS resolution cannot
be forcibly cancelled; callers should therefore treat it as a bounded local
adapter rather than a hard real-time deadline.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, time as datetime_time, timezone
from decimal import Decimal, InvalidOperation
from http.client import HTTPException
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from ..common.exceptions import IngestionError
from ..common.time import utc_now
from ..processing.normaliser import normalize_symbol
from .schemas import MarketEvent

API_HOST = "www.alphavantage.co"
API_URL = f"https://{API_HOST}/query"
DAILY_SERIES_KEY = "Time Series (Daily)"
DEFAULT_MAX_RESPONSE_BYTES = 2_000_000
DEFAULT_TIMEOUT_SECONDS = 10.0
DEFAULT_MAX_RETRIES = 2
MAX_COMPACT_RECORDS = 100
MAX_SIGNED_64_BIT = 9_223_372_036_854_775_807
RETRYABLE_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
INPUT_SYMBOL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")
CALENDAR_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
ALPHA_VANTAGE_DAILY_EVENT_PREFIX = "av-daily-"

HttpTransport = Callable[[str, float, int], bytes]
Sleep = Callable[[float], None]


def _canonical_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise IngestionError("Alpha Vantage symbol must be text")
    stripped = symbol.strip()
    if INPUT_SYMBOL_PATTERN.fullmatch(stripped) is None:
        raise IngestionError("Alpha Vantage symbol contains unsupported characters")
    canonical = normalize_symbol(stripped)
    if SYMBOL_PATTERN.fullmatch(canonical) is None:
        raise IngestionError("Alpha Vantage symbol contains unsupported characters")
    return canonical


def _aware_utc(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise IngestionError(f"{field_name} must include a timezone")
    return value.astimezone(timezone.utc)


def _validate_selection(
    *,
    start_date: date | None,
    end_date: date | None,
    max_records: int,
) -> None:
    if start_date is not None and end_date is not None and start_date > end_date:
        raise IngestionError("start_date must be on or before end_date")
    if isinstance(max_records, bool) or not 1 <= max_records <= MAX_COMPACT_RECORDS:
        raise IngestionError(f"max_records must be between 1 and {MAX_COMPACT_RECORDS}")


def _parse_price(value: Any, *, field_name: str) -> float:
    parsed: float | None = None
    try:
        parsed = float(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError, OverflowError):
        pass
    if parsed is None:
        raise IngestionError(f"Alpha Vantage {field_name} must be numeric")
    if not math.isfinite(parsed) or parsed <= 0:
        raise IngestionError(
            f"Alpha Vantage {field_name} must be finite and greater than zero"
        )
    return parsed


def _parse_volume(value: Any) -> int:
    parsed: Decimal | None = None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        pass
    if parsed is None:
        raise IngestionError("Alpha Vantage volume must be numeric")
    if (
        not parsed.is_finite()
        or parsed < 0
        or parsed > MAX_SIGNED_64_BIT
        or parsed != parsed.to_integral_value()
    ):
        raise IngestionError("Alpha Vantage volume must be a non-negative whole number")
    return int(parsed)


def alpha_vantage_daily_event_id(symbol: str, event_date: date) -> str:
    if type(event_date) is not date:
        raise IngestionError("Alpha Vantage event identity requires a calendar date")
    canonical_symbol = _canonical_symbol(symbol)
    identity = (
        f"alpha_vantage|TIME_SERIES_DAILY|{canonical_symbol}|{event_date.isoformat()}"
    )
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    return f"{ALPHA_VANTAGE_DAILY_EVENT_PREFIX}{digest}"


def _provider_error(payload: Mapping[str, Any]) -> str | None:
    if "Error Message" in payload:
        return "Alpha Vantage rejected the request"
    if "Note" in payload:
        return "Alpha Vantage rate limit was reached"
    if "Information" in payload:
        return "Alpha Vantage returned a service information response"
    return None


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def parse_alpha_vantage_daily_response(
    payload_bytes: bytes,
    *,
    symbol: str,
    ingested_at: datetime,
    start_date: date | None = None,
    end_date: date | None = None,
    max_records: int = MAX_COMPACT_RECORDS,
) -> list[MarketEvent]:
    canonical_symbol = _canonical_symbol(symbol)
    ingest_timestamp = _aware_utc(ingested_at, field_name="ingested_at")
    _validate_selection(
        start_date=start_date,
        end_date=end_date,
        max_records=max_records,
    )

    payload: Any = None
    payload_is_invalid = False
    try:
        payload = json.loads(
            payload_bytes.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        payload_is_invalid = True
    if payload_is_invalid:
        raise IngestionError("Alpha Vantage response must be valid UTF-8 JSON")
    if not isinstance(payload, dict):
        raise IngestionError("Alpha Vantage response must be a JSON object")

    provider_error = _provider_error(payload)
    if provider_error is not None:
        raise IngestionError(provider_error)

    metadata = payload.get("Meta Data")
    if not isinstance(metadata, dict):
        raise IngestionError("Alpha Vantage response is missing metadata")
    returned_symbol = metadata.get("2. Symbol")
    if not isinstance(returned_symbol, str) or _canonical_symbol(returned_symbol) != canonical_symbol:
        raise IngestionError("Alpha Vantage response symbol does not match the request")

    series = payload.get(DAILY_SERIES_KEY)
    if not isinstance(series, dict) or not series:
        raise IngestionError("Alpha Vantage response contains no daily time series")
    if len(series) > MAX_COMPACT_RECORDS:
        raise IngestionError("Alpha Vantage compact response contains more than 100 rows")

    candidates: list[tuple[date, Mapping[str, Any]]] = []
    for raw_date, raw_bar in series.items():
        if not isinstance(raw_date, str) or not isinstance(raw_bar, dict):
            raise IngestionError("Alpha Vantage daily time series has an invalid row")
        if CALENDAR_DATE_PATTERN.fullmatch(raw_date) is None:
            raise IngestionError("Alpha Vantage daily time series has an invalid date")
        event_date: date | None = None
        try:
            event_date = date.fromisoformat(raw_date)
        except ValueError:
            pass
        if event_date is None:
            raise IngestionError("Alpha Vantage daily time series has an invalid date")
        if start_date is not None and event_date < start_date:
            continue
        if end_date is not None and event_date > end_date:
            continue
        candidates.append((event_date, raw_bar))

    selected = sorted(candidates, key=lambda item: item[0], reverse=True)[:max_records]
    events: list[MarketEvent] = []
    for event_date, raw_bar in sorted(selected, key=lambda item: item[0]):
        required_bar_fields = {"1. open", "2. high", "3. low", "4. close", "5. volume"}
        if not required_bar_fields.issubset(raw_bar):
            raise IngestionError("Alpha Vantage daily bar is missing an OHLCV field")
        open_price = _parse_price(raw_bar["1. open"], field_name="open price")
        high_price = _parse_price(raw_bar["2. high"], field_name="high price")
        low_price = _parse_price(raw_bar["3. low"], field_name="low price")
        close_price = _parse_price(raw_bar["4. close"], field_name="close price")
        if low_price > high_price or not (
            low_price <= open_price <= high_price and low_price <= close_price <= high_price
        ):
            raise IngestionError("Alpha Vantage daily bar has inconsistent OHLC values")
        events.append(
            MarketEvent(
                event_id=alpha_vantage_daily_event_id(canonical_symbol, event_date),
                symbol=canonical_symbol,
                price=close_price,
                volume=_parse_volume(raw_bar["5. volume"]),
                ts_event=datetime.combine(event_date, datetime_time.min, tzinfo=timezone.utc),
                ts_ingest=ingest_timestamp,
                source="alpha_vantage",
            )
        )

    return events


class _RejectRedirects(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        file_pointer: Any,
        code: int,
        message: str,
        headers: Any,
        new_url: str,
    ) -> None:
        return None


_HTTP_OPENER = build_opener(ProxyHandler({}), _RejectRedirects())


def _download(url: str, timeout_seconds: float, max_response_bytes: int) -> bytes:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "identity",
            "User-Agent": "financial-risk-data-platform/0.1",
        },
    )
    deadline = time.monotonic() + timeout_seconds
    with _HTTP_OPENER.open(request, timeout=timeout_seconds) as response:
        final_url = urlsplit(response.geturl())
        if final_url.scheme != "https" or final_url.hostname != API_HOST:
            raise IngestionError("Alpha Vantage redirected to an unexpected host")
        content_encoding = response.headers.get("Content-Encoding", "identity").lower()
        if content_encoding not in {"", "identity"}:
            raise IngestionError("Alpha Vantage returned unsupported content encoding")
        content_type = response.headers.get("Content-Type", "").split(";", maxsplit=1)[0].lower()
        if content_type != "application/json":
            raise IngestionError("Alpha Vantage response content type must be application/json")
        content_length = response.headers.get("Content-Length")
        declared_length: int | None = None
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                pass
            if declared_length is None:
                raise IngestionError("Alpha Vantage returned an invalid content length")
            if declared_length < 0 or declared_length > max_response_bytes:
                raise IngestionError("Alpha Vantage response exceeded the configured size limit")
        chunks: list[bytes] = []
        received = 0
        reader = getattr(response, "read1", response.read)
        while True:
            if time.monotonic() >= deadline:
                raise IngestionError("Alpha Vantage response exceeded the request time limit")
            chunk = reader(min(65_536, max_response_bytes - received + 1))
            if not chunk:
                break
            chunks.append(chunk)
            received += len(chunk)
            if received > max_response_bytes:
                raise IngestionError("Alpha Vantage response exceeded the configured size limit")
        payload = b"".join(chunks)
        if declared_length is not None and received != declared_length:
            raise IngestionError("Alpha Vantage response did not match its content length")
    if len(payload) > max_response_bytes:
        raise IngestionError("Alpha Vantage response exceeded the configured size limit")
    return payload


def fetch_alpha_vantage_daily_events(
    *,
    symbol: str,
    api_key: str,
    ingested_at: datetime | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    max_records: int = MAX_COMPACT_RECORDS,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    max_retries: int = DEFAULT_MAX_RETRIES,
    max_response_bytes: int = DEFAULT_MAX_RESPONSE_BYTES,
    transport: HttpTransport = _download,
    sleep: Sleep = time.sleep,
) -> list[MarketEvent]:
    canonical_symbol = _canonical_symbol(symbol)
    _validate_selection(
        start_date=start_date,
        end_date=end_date,
        max_records=max_records,
    )
    if ingested_at is not None:
        ingested_at = _aware_utc(ingested_at, field_name="ingested_at")
    secret = api_key.strip()
    if not secret:
        raise IngestionError("Alpha Vantage API key is required")
    if len(secret) > 256 or not secret.isascii() or any(
        not 33 <= ord(character) <= 126 for character in secret
    ):
        raise IngestionError("Alpha Vantage API key has an invalid format")
    if not 0 < timeout_seconds <= 60:
        raise IngestionError("timeout_seconds must be greater than zero and at most 60")
    if not 0 <= max_retries <= 3:
        raise IngestionError("max_retries must be between 0 and 3")
    if not 1 <= max_response_bytes <= 10_000_000:
        raise IngestionError("max_response_bytes must be between 1 and 10000000")

    query = urlencode(
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": canonical_symbol,
            "outputsize": "compact",
            "datatype": "json",
            "apikey": secret,
        }
    )
    url = f"{API_URL}?{query}"

    payload: bytes | None = None
    for attempt in range(max_retries + 1):
        try:
            payload = transport(url, timeout_seconds, max_response_bytes)
            break
        except HTTPError as exc:
            retryable = exc.code in RETRYABLE_HTTP_STATUSES
            message = f"Alpha Vantage request failed with HTTP {exc.code}"
        except (HTTPException, URLError, TimeoutError, OSError):
            retryable = True
            message = "Alpha Vantage request failed due to a network error"

        if not retryable or attempt == max_retries:
            raise IngestionError(message) from None
        sleep(0.25 * (2**attempt))

    if payload is None:
        raise IngestionError("Alpha Vantage request produced no response")
    if len(payload) > max_response_bytes:
        raise IngestionError("Alpha Vantage response exceeded the configured size limit")

    return parse_alpha_vantage_daily_response(
        payload,
        symbol=canonical_symbol,
        ingested_at=ingested_at or utc_now(),
        start_date=start_date,
        end_date=end_date,
        max_records=max_records,
    )


__all__ = [
    "ALPHA_VANTAGE_DAILY_EVENT_PREFIX",
    "alpha_vantage_daily_event_id",
    "fetch_alpha_vantage_daily_events",
    "parse_alpha_vantage_daily_response",
]
