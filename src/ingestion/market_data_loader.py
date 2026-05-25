from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pydantic import ValidationError as PydanticValidationError

from ..common.exceptions import IngestionError
from ..common.time import utc_now
from ..processing.normaliser import normalize_symbol
from .schemas import MarketEvent

FIELD_ALIASES = {
    "event_id": ("event_id", "id"),
    "symbol": ("symbol", "ticker", "instrument"),
    "price": ("price", "close", "last", "last_price"),
    "volume": ("volume", "trade_volume"),
    "ts_event": ("ts_event", "event_time", "timestamp", "datetime", "date"),
    "ts_ingest": ("ts_ingest", "ingest_time", "ingested_at"),
    "source": ("source", "provider"),
}


def _normalise_key(key: str) -> str:
    return key.strip().lower().replace(" ", "_").replace("-", "_")


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    return {_normalise_key(str(key)): value for key, value in row.items() if key is not None}


def _first_present(row: dict[str, Any], field_name: str) -> Any:
    for alias in FIELD_ALIASES[field_name]:
        value = row.get(alias)
        if value not in (None, ""):
            return value
    return None


def _parse_datetime(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise IngestionError(f"{field_name} must be an ISO-8601 timestamp") from exc
    else:
        raise IngestionError(f"{field_name} must be a datetime or ISO-8601 string")

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _coerce_float(value: Any, *, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise IngestionError(f"{field_name} must be numeric") from exc


def _coerce_int(value: Any, *, field_name: str) -> int:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise IngestionError(f"{field_name} must be numeric") from exc

    if not parsed.is_integer():
        raise IngestionError(f"{field_name} must be a whole number")
    return int(parsed)


def _stable_event_id(
    *,
    symbol: str,
    price: float,
    volume: int,
    ts_event: datetime,
    source: str,
) -> str:
    payload = {
        "price": price,
        "source": source,
        "symbol": symbol,
        "ts_event": ts_event.isoformat(),
        "volume": volume,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"market-{digest}"


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        raise IngestionError("JSON market data must be a list of objects")
    return payload


def _read_json_lines(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            row = json.loads(line)
            if not isinstance(row, dict):
                raise IngestionError(f"Line {line_number} is not an object")
            rows.append(row)
    return rows


def _read_landed_rows(path: Path) -> list[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _read_csv(path)
    if suffix == ".json":
        return _read_json(path)
    if suffix in {".jsonl", ".ndjson"}:
        return _read_json_lines(path)
    raise IngestionError(f"Unsupported market data file type: {suffix}")


def build_market_event_from_row(
    row: dict[str, Any],
    *,
    default_source: str = "stooq",
    ingested_at: datetime | None = None,
) -> MarketEvent:
    normalised = _normalise_row(row)
    source = str(_first_present(normalised, "source") or default_source)
    symbol_value = _first_present(normalised, "symbol")
    price_value = _first_present(normalised, "price")
    volume_value = _first_present(normalised, "volume")
    ts_event_value = _first_present(normalised, "ts_event")
    ts_ingest_value = _first_present(normalised, "ts_ingest")

    missing = [
        name
        for name, value in {
            "symbol": symbol_value,
            "price": price_value,
            "volume": volume_value,
            "ts_event": ts_event_value,
        }.items()
        if value in (None, "")
    ]
    if missing:
        raise IngestionError(f"Missing required market data fields: {missing}")

    symbol = normalize_symbol(str(symbol_value))
    price = _coerce_float(price_value, field_name="price")
    volume = _coerce_int(volume_value, field_name="volume")
    ts_event = _parse_datetime(ts_event_value, field_name="ts_event")
    if ts_ingest_value not in (None, ""):
        ts_ingest = _parse_datetime(ts_ingest_value, field_name="ts_ingest")
    elif ingested_at is not None:
        ts_ingest = _parse_datetime(ingested_at, field_name="ingested_at")
    else:
        ts_ingest = utc_now()

    event_id = _first_present(normalised, "event_id") or _stable_event_id(
        symbol=symbol,
        price=price,
        volume=volume,
        ts_event=ts_event,
        source=source,
    )

    try:
        return MarketEvent(
            event_id=str(event_id),
            symbol=symbol,
            price=price,
            volume=volume,
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            source=source,
        )
    except PydanticValidationError as exc:
        raise IngestionError(f"Invalid market data row: {exc}") from exc


def load_market_events(
    path: Path,
    *,
    default_source: str = "stooq",
    ingested_at: datetime | None = None,
) -> list[MarketEvent]:
    rows = _read_landed_rows(path)
    return [
        build_market_event_from_row(row, default_source=default_source, ingested_at=ingested_at)
        for row in rows
    ]


def market_events_to_records(events: Iterable[MarketEvent]) -> list[dict[str, Any]]:
    return [event.model_dump() for event in events]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert landed market data into market events.")
    parser.add_argument("input", type=Path, help="CSV, JSON, JSONL, or NDJSON market data file.")
    parser.add_argument("--source", default="stooq", help="Default source when rows omit source.")
    parser.add_argument("--output", type=Path, help="Optional JSON output path.")
    parser.add_argument("--ingested-at", help="Optional ISO-8601 ingest timestamp.")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    ingested_at = (
        _parse_datetime(args.ingested_at, field_name="ingested_at")
        if args.ingested_at is not None
        else None
    )
    events = load_market_events(args.input, default_source=args.source, ingested_at=ingested_at)
    payload = [event.model_dump(mode="json") for event in events]

    if args.output is None:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
