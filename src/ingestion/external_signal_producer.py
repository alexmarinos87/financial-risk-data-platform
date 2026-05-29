from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pydantic import BaseModel
from pydantic import ValidationError as PydanticValidationError

from ..common.exceptions import IngestionError
from ..common.time import utc_now

FIELD_ALIASES = {
    "signal_id": ("signal_id", "event_id", "id"),
    "name": ("name", "signal", "indicator", "metric"),
    "value": ("value", "reading", "level", "score"),
    "ts_event": ("ts_event", "event_time", "timestamp", "datetime", "date"),
    "ts_ingest": ("ts_ingest", "ingest_time", "ingested_at"),
    "source": ("source", "provider"),
}


class ExternalSignal(BaseModel):
    signal_id: str
    name: str
    value: float
    ts_event: datetime
    ts_ingest: datetime
    source: str


def _normalise_key(key: str) -> str:
    return key.strip().lower().replace(" ", "_").replace("-", "_")


def _normalise_signal_name(name: str) -> str:
    return name.strip().replace(" ", "_").replace("-", "_").upper()


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


def _stable_signal_id(
    *,
    name: str,
    value: float,
    ts_event: datetime,
    source: str,
) -> str:
    payload = {
        "name": name,
        "source": source,
        "ts_event": ts_event.isoformat(),
        "value": value,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:16]
    return f"signal-{digest}"


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def _read_json(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        raise IngestionError("JSON external signals must be a list of objects")
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
    raise IngestionError(f"Unsupported external signal file type: {suffix}")


def build_external_signal_from_row(
    row: dict[str, Any],
    *,
    default_source: str = "fred",
    ingested_at: datetime | None = None,
) -> ExternalSignal:
    normalised = _normalise_row(row)
    source = str(_first_present(normalised, "source") or default_source)
    name_value = _first_present(normalised, "name")
    signal_value = _first_present(normalised, "value")
    ts_event_value = _first_present(normalised, "ts_event")
    ts_ingest_value = _first_present(normalised, "ts_ingest")

    missing = [
        name
        for name, value in {
            "name": name_value,
            "value": signal_value,
            "ts_event": ts_event_value,
        }.items()
        if value in (None, "")
    ]
    if missing:
        raise IngestionError(f"Missing required external signal fields: {missing}")

    name = _normalise_signal_name(str(name_value))
    value = _coerce_float(signal_value, field_name="value")
    ts_event = _parse_datetime(ts_event_value, field_name="ts_event")
    if ts_ingest_value not in (None, ""):
        ts_ingest = _parse_datetime(ts_ingest_value, field_name="ts_ingest")
    elif ingested_at is not None:
        ts_ingest = _parse_datetime(ingested_at, field_name="ingested_at")
    else:
        ts_ingest = utc_now()

    signal_id = _first_present(normalised, "signal_id") or _stable_signal_id(
        name=name,
        value=value,
        ts_event=ts_event,
        source=source,
    )

    try:
        return ExternalSignal(
            signal_id=str(signal_id),
            name=name,
            value=value,
            ts_event=ts_event,
            ts_ingest=ts_ingest,
            source=source,
        )
    except PydanticValidationError as exc:
        raise IngestionError(f"Invalid external signal row: {exc}") from exc


def load_external_signals(
    path: Path,
    *,
    default_source: str = "fred",
    ingested_at: datetime | None = None,
) -> list[ExternalSignal]:
    rows = _read_landed_rows(path)
    return [
        build_external_signal_from_row(
            row,
            default_source=default_source,
            ingested_at=ingested_at,
        )
        for row in rows
    ]


def external_signals_to_records(signals: Iterable[ExternalSignal]) -> list[dict[str, Any]]:
    return [signal.model_dump() for signal in signals]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert landed external signals into records.")
    parser.add_argument("input", type=Path, help="CSV, JSON, JSONL, or NDJSON signal file.")
    parser.add_argument("--source", default="fred", help="Default source when rows omit source.")
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
    signals = load_external_signals(args.input, default_source=args.source, ingested_at=ingested_at)
    payload = [signal.model_dump(mode="json") for signal in signals]

    if args.output is None:
        print(json.dumps(payload, indent=2, sort_keys=True))
        return

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


if __name__ == "__main__":
    main()
