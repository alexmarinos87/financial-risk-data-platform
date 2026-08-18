from __future__ import annotations

import json
import traceback
from datetime import date, datetime, timezone
from http.client import BadStatusLine
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit

import pytest

import src.ingestion.alpha_vantage_client as alpha_vantage_client
from src.common.exceptions import IngestionError
from src.ingestion.alpha_vantage_client import (
    alpha_vantage_daily_event_id,
    fetch_alpha_vantage_daily_events,
    parse_alpha_vantage_daily_response,
)


class _FakeResponse:
    def __init__(
        self,
        body: bytes,
        *,
        headers: dict[str, str],
        final_url: str = "https://www.alphavantage.co/query",
    ) -> None:
        self._body = body
        self._offset = 0
        self._final_url = final_url
        self.headers = headers
        self.closed = False

    def geturl(self) -> str:
        return self._final_url

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self._body) - self._offset
        chunk = self._body[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk

    def read1(self, size: int = -1) -> bytes:
        return self.read(size)

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True


class _FakeOpener:
    def __init__(self, response: _FakeResponse) -> None:
        self.response = response
        self.request: object | None = None
        self.timeout: float | None = None

    def open(self, request: object, *, timeout: float) -> _FakeResponse:
        self.request = request
        self.timeout = timeout
        return self.response


def _response(
    *,
    symbol: str = "IBM",
    first_close: str = "101.50",
    first_volume: str = "1200",
) -> bytes:
    return json.dumps(
        {
            "Meta Data": {"2. Symbol": symbol},
            "Time Series (Daily)": {
                "2025-01-03": {
                    "1. open": "100.75",
                    "2. high": "102.00",
                    "3. low": "100.50",
                    "4. close": first_close,
                    "5. volume": first_volume,
                },
                "2025-01-02": {
                    "1. open": "99.75",
                    "2. high": "101.00",
                    "3. low": "99.50",
                    "4. close": "100.25",
                    "5. volume": "900",
                },
                "2024-12-31": {
                    "1. open": "99.00",
                    "2. high": "100.00",
                    "3. low": "98.50",
                    "4. close": "99.75",
                    "5. volume": "800",
                },
            },
        }
    ).encode("utf-8")


def test_parse_daily_response_builds_ordered_market_events() -> None:
    ingested_at = datetime(2025, 1, 4, 12, 30, tzinfo=timezone.utc)

    events = parse_alpha_vantage_daily_response(
        _response(symbol="ibm"),
        symbol="IBM",
        ingested_at=ingested_at,
        start_date=date(2025, 1, 1),
    )

    assert [event.ts_event for event in events] == [
        datetime(2025, 1, 2, tzinfo=timezone.utc),
        datetime(2025, 1, 3, tzinfo=timezone.utc),
    ]
    assert [event.price for event in events] == [100.25, 101.5]
    assert [event.volume for event in events] == [900, 1200]
    assert {event.symbol for event in events} == {"IBM"}
    assert {event.source for event in events} == {"alpha_vantage"}
    assert {event.ts_ingest for event in events} == {ingested_at}


def test_event_id_tracks_logical_bar_not_mutable_values() -> None:
    ingested_at = datetime(2025, 1, 4, tzinfo=timezone.utc)

    original = parse_alpha_vantage_daily_response(
        _response(first_close="101.50", first_volume="1200"),
        symbol="IBM",
        ingested_at=ingested_at,
        start_date=date(2025, 1, 3),
    )
    corrected = parse_alpha_vantage_daily_response(
        _response(first_close="102.00", first_volume="1250"),
        symbol="IBM",
        ingested_at=ingested_at,
        start_date=date(2025, 1, 3),
    )

    assert original[0].event_id == corrected[0].event_id
    assert original[0].event_id == "av-daily-696a6d4963466a26937a"
    assert original[0].price != corrected[0].price


def test_daily_event_identity_is_pinned_to_canonical_symbol_and_calendar_date() -> None:
    expected = "av-daily-696a6d4963466a26937a"

    assert alpha_vantage_daily_event_id("IBM", date(2025, 1, 3)) == expected
    assert alpha_vantage_daily_event_id("ibm", date(2025, 1, 3)) == expected
    with pytest.raises(IngestionError, match="requires a calendar date"):
        alpha_vantage_daily_event_id(
            "IBM",
            datetime(2025, 1, 3, tzinfo=timezone.utc),  # type: ignore[arg-type]
        )


def test_parse_daily_response_limits_most_recent_rows_then_orders_them() -> None:
    events = parse_alpha_vantage_daily_response(
        _response(),
        symbol="IBM",
        ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        max_records=2,
    )

    assert [event.ts_event.date() for event in events] == [
        date(2025, 1, 2),
        date(2025, 1, 3),
    ]


def test_parse_daily_response_allows_an_empty_filtered_range() -> None:
    events = parse_alpha_vantage_daily_response(
        _response(),
        symbol="IBM",
        ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        start_date=date(2026, 1, 1),
    )

    assert events == []


@pytest.mark.parametrize(
    ("field", "message"),
    [
        ("Error Message", "rejected"),
        ("Note", "rate limit"),
        ("Information", "service information"),
    ],
)
def test_parse_daily_response_rejects_provider_errors(field: str, message: str) -> None:
    payload = json.dumps({field: "opaque provider detail"}).encode("utf-8")

    with pytest.raises(IngestionError, match=message):
        parse_alpha_vantage_daily_response(
            payload,
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize(
    ("close", "volume", "message"),
    [
        ("nan", "1200", "close price"),
        ("0", "1200", "greater than zero"),
        ("101.50", "-1", "non-negative whole number"),
        ("101.50", "1.5", "non-negative whole number"),
    ],
)
def test_parse_daily_response_rejects_invalid_values(
    close: str,
    volume: str,
    message: str,
) -> None:
    with pytest.raises(IngestionError, match=message):
        parse_alpha_vantage_daily_response(
            _response(first_close=close, first_volume=volume),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
            start_date=date(2025, 1, 3),
        )


def test_parse_daily_response_rejects_inconsistent_ohlc() -> None:
    payload = json.loads(_response())
    payload["Time Series (Daily)"]["2025-01-03"]["2. high"] = "100.00"

    with pytest.raises(IngestionError, match="inconsistent OHLC"):
        parse_alpha_vantage_daily_response(
            json.dumps(payload).encode("utf-8"),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
            start_date=date(2025, 1, 3),
        )


def test_parse_daily_response_rejects_duplicate_json_keys() -> None:
    payload = b'{"Meta Data": {}, "Meta Data": {}, "Time Series (Daily)": {}}'

    with pytest.raises(IngestionError, match="valid UTF-8 JSON"):
        parse_alpha_vantage_daily_response(
            payload,
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )


def test_parse_daily_response_rejects_iso_week_date_key() -> None:
    payload = json.loads(_response())
    payload["Time Series (Daily)"]["2025-W01-1"] = payload["Time Series (Daily)"].pop(
        "2025-01-03"
    )

    with pytest.raises(IngestionError, match="invalid date"):
        parse_alpha_vantage_daily_response(
            json.dumps(payload).encode("utf-8"),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )


@pytest.mark.parametrize("symbol", ["ß", "ſ", "ı", "ﬀ"])
def test_symbol_validation_rejects_unicode_casefold_aliases(symbol: str) -> None:
    with pytest.raises(IngestionError, match="unsupported characters"):
        fetch_alpha_vantage_daily_events(
            symbol=symbol,
            api_key="test-secret",
            transport=lambda url, timeout, maximum: _response(),
        )


def test_fetch_builds_bounded_request_without_logging_key() -> None:
    calls: list[tuple[str, float, int]] = []
    ingested_at = datetime(2025, 1, 4, tzinfo=timezone.utc)

    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        calls.append((url, timeout_seconds, max_bytes))
        return _response()

    events = fetch_alpha_vantage_daily_events(
        symbol="ibm",
        api_key="test-secret",
        ingested_at=ingested_at,
        max_records=1,
        timeout_seconds=4.0,
        max_response_bytes=4096,
        transport=transport,
    )

    query = parse_qs(urlsplit(calls[0][0]).query)
    assert urlsplit(calls[0][0]).scheme == "https"
    assert urlsplit(calls[0][0]).hostname == "www.alphavantage.co"
    assert query == {
        "function": ["TIME_SERIES_DAILY"],
        "symbol": ["IBM"],
        "outputsize": ["compact"],
        "datatype": ["json"],
        "apikey": ["test-secret"],
    }
    assert calls[0][1:] == (4.0, 4096)
    assert len(events) == 1


def test_fetch_retries_retryable_http_errors() -> None:
    attempts = 0
    delays: list[float] = []

    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise HTTPError(url, 503, "unavailable", hdrs=None, fp=None)
        return _response()

    events = fetch_alpha_vantage_daily_events(
        symbol="IBM",
        api_key="test-secret",
        ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        transport=transport,
        sleep=delays.append,
    )

    assert len(events) == 3
    assert attempts == 3
    assert delays == [0.25, 0.5]


def test_fetch_redacts_key_when_non_retryable_request_fails() -> None:
    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        raise HTTPError(url, 403, "forbidden", hdrs=None, fp=None)

    with pytest.raises(IngestionError) as error:
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="do-not-expose",
            transport=transport,
        )

    assert "HTTP 403" in str(error.value)
    assert "do-not-expose" not in str(error.value)
    assert "do-not-expose" not in repr(error.value)
    assert "https://" not in str(error.value)
    rendered = "".join(
        traceback.format_exception(type(error.value), error.value, error.value.__traceback__)
    )
    assert "do-not-expose" not in rendered


def test_fetch_sanitizes_low_level_http_protocol_errors() -> None:
    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        raise BadStatusLine(f"bad response for {url}")

    with pytest.raises(IngestionError) as error:
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="sentinel-secret",
            max_retries=0,
            transport=transport,
        )

    rendered = "".join(
        traceback.format_exception(type(error.value), error.value, error.value.__traceback__)
    )
    assert "sentinel-secret" not in str(error.value)
    assert "sentinel-secret" not in repr(error.value)
    assert "sentinel-secret" not in rendered
    assert error.value.__context__ is None


def test_fetch_rejects_oversized_injected_response() -> None:
    with pytest.raises(IngestionError, match="size limit"):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="test-secret",
            max_response_bytes=8,
            transport=lambda url, timeout, maximum: b"x" * 9,
        )


def test_default_transport_streams_json_with_fixed_network_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _response()
    response = _FakeResponse(
        payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Content-Encoding": "identity",
            "Content-Length": str(len(payload)),
        },
    )
    opener = _FakeOpener(response)
    monkeypatch.setattr(alpha_vantage_client, "_HTTP_OPENER", opener)

    events = fetch_alpha_vantage_daily_events(
        symbol="IBM",
        api_key="sentinel-secret",
        ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        timeout_seconds=3.0,
    )

    assert len(events) == 3
    assert opener.timeout == 3.0
    assert response.closed is True
    request = opener.request
    assert request is not None
    assert request.get_header("Accept-encoding") == "identity"  # type: ignore[attr-defined]


@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ({"Content-Type": "text/html"}, "content type"),
        (
            {
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
            "content encoding",
        ),
        (
            {
                "Content-Type": "application/json",
                "Content-Length": "9000000",
            },
            "size limit",
        ),
        (
            {
                "Content-Type": "application/json",
                "Content-Length": "999",
            },
            "content length",
        ),
    ],
)
def test_default_transport_rejects_unsafe_response_metadata(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str],
    message: str,
) -> None:
    response = _FakeResponse(_response(), headers=headers)
    monkeypatch.setattr(alpha_vantage_client, "_HTTP_OPENER", _FakeOpener(response))

    with pytest.raises(IngestionError, match=message):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="sentinel-secret",
            max_response_bytes=4096,
        )

    assert response.closed is True


def test_invalid_content_length_does_not_leak_reflected_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    response = _FakeResponse(
        _response(),
        headers={
            "Content-Type": "application/json",
            "Content-Length": "sentinel-secret",
        },
    )
    monkeypatch.setattr(alpha_vantage_client, "_HTTP_OPENER", _FakeOpener(response))

    with pytest.raises(IngestionError) as error:
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="test-secret",
            max_retries=0,
        )

    rendered = "".join(
        traceback.format_exception(type(error.value), error.value, error.value.__traceback__)
    )
    assert "sentinel-secret" not in str(error.value)
    assert "sentinel-secret" not in repr(error.value)
    assert "sentinel-secret" not in rendered
    assert error.value.__context__ is None


def test_default_transport_enforces_streamed_monotonic_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _response()
    response = _FakeResponse(
        payload,
        headers={
            "Content-Type": "application/json",
            "Content-Length": str(len(payload)),
        },
    )
    monkeypatch.setattr(alpha_vantage_client, "_HTTP_OPENER", _FakeOpener(response))
    clock = iter([0.0, 4.0])
    monkeypatch.setattr(alpha_vantage_client.time, "monotonic", lambda: next(clock))

    with pytest.raises(IngestionError, match="time limit"):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="test-secret",
            timeout_seconds=3.0,
            max_retries=0,
        )

    assert response.closed is True


def test_default_opener_disables_proxy_environment_and_redirects() -> None:
    proxy_handlers = [
        handler
        for handler in alpha_vantage_client._HTTP_OPENER.handlers
        if isinstance(handler, alpha_vantage_client.ProxyHandler)
    ]
    redirect_handlers = [
        handler
        for handler in alpha_vantage_client._HTTP_OPENER.handlers
        if isinstance(handler, alpha_vantage_client._RejectRedirects)
    ]

    # ProxyHandler({}) removes the environment-backed default and registers no
    # proxy protocol handlers, so none remains in the built opener.
    assert proxy_handlers == []
    assert len(redirect_handlers) == 1
    assert (
        redirect_handlers[0].redirect_request(
            alpha_vantage_client.Request("https://www.alphavantage.co/query"),
            None,
            302,
            "redirect",
            {},
            "https://example.com/steal",
        )
        is None
    )


def test_fetch_rejects_invalid_key_before_transport() -> None:
    called = False

    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        nonlocal called
        called = True
        return _response()

    with pytest.raises(IngestionError, match="invalid format"):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="secret with spaces",
            transport=transport,
        )

    assert called is False


@pytest.mark.parametrize(
    "options",
    [
        {"max_records": 0},
        {
            "start_date": date(2025, 1, 2),
            "end_date": date(2025, 1, 1),
        },
        {"ingested_at": datetime(2025, 1, 4)},
    ],
)
def test_fetch_rejects_invalid_local_options_before_transport(
    options: dict[str, object],
) -> None:
    called = False

    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        nonlocal called
        called = True
        return _response()

    with pytest.raises(IngestionError):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key="test-secret",
            transport=transport,
            **options,  # type: ignore[arg-type]
        )

    assert called is False


@pytest.mark.parametrize("api_key", ["", "   ", "secret\x00value"])
def test_fetch_rejects_missing_or_control_character_key_before_transport(api_key: str) -> None:
    called = False

    def transport(url: str, timeout_seconds: float, max_bytes: int) -> bytes:
        nonlocal called
        called = True
        return _response()

    with pytest.raises(IngestionError):
        fetch_alpha_vantage_daily_events(
            symbol="IBM",
            api_key=api_key,
            transport=transport,
        )

    assert called is False


def test_parse_rejects_boolean_record_limit_and_volume_overflow() -> None:
    with pytest.raises(IngestionError, match="max_records"):
        parse_alpha_vantage_daily_response(
            _response(),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
            max_records=True,
        )

    with pytest.raises(IngestionError, match="non-negative whole number"):
        parse_alpha_vantage_daily_response(
            _response(first_volume="9223372036854775808"),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
            start_date=date(2025, 1, 3),
        )


def test_parse_rejects_naive_ingest_timestamp_and_symbol_mismatch() -> None:
    with pytest.raises(IngestionError, match="timezone"):
        parse_alpha_vantage_daily_response(
            _response(),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4),
        )

    with pytest.raises(IngestionError, match="does not match"):
        parse_alpha_vantage_daily_response(
            _response(symbol="MSFT"),
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )


def test_parse_sanitizes_pathologically_nested_json() -> None:
    nested = b"[" * 1_200 + b"0" + b"]" * 1_200
    payload = b'{"ignored":' + nested + b"}"

    with pytest.raises(IngestionError, match="valid UTF-8 JSON"):
        parse_alpha_vantage_daily_response(
            payload,
            symbol="IBM",
            ingested_at=datetime(2025, 1, 4, tzinfo=timezone.utc),
        )
