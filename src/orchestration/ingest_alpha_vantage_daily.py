from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import ValidationError as PydanticValidationError

from ..common.exceptions import IngestionError, RawEventConflictError, StorageError
from ..common.time import utc_now
from ..ingestion.alpha_vantage_client import (
    alpha_vantage_daily_event_id,
    fetch_alpha_vantage_daily_events,
)
from ..ingestion.schemas import MarketEvent
from ..storage.partitioning import partition_path
from ..storage.s3_writer import validate_raw_storage_destination, write_records
from ..storage.storage_config import load_storage_config

API_KEY_ENV = "ALPHA_VANTAGE_API_KEY"
SOURCE_NAME = "alpha_vantage"
SOURCE_OPERATION = "TIME_SERIES_DAILY"
SOURCE_SEMANTICS = "normalized_daily_close"
CALENDAR_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
INPUT_SYMBOL_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$")

Fetcher = Callable[..., list[MarketEvent]]
Writer = Callable[..., int]
ConfigLoader = Callable[[Path], dict[str, Any]]
Clock = Callable[[], datetime]


class _EventContractError(IngestionError):
    pass


class _RawPublicationAmbiguousError(StorageError):
    pass


def _calendar_date(value: str) -> date:
    parsed: date | None = None
    if CALENDAR_DATE_PATTERN.fullmatch(value) is not None:
        try:
            parsed = date.fromisoformat(value)
        except ValueError:
            pass
    if parsed is None:
        raise argparse.ArgumentTypeError("must be a calendar date in YYYY-MM-DD format")
    return parsed


def _record_limit(value: str) -> int:
    parsed: int | None = None
    try:
        parsed = int(value)
    except ValueError:
        pass
    if parsed is None or not 1 <= parsed <= 100:
        raise argparse.ArgumentTypeError("must be an integer between 1 and 100")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch one Alpha Vantage daily series into normalized local raw parquet."
    )
    parser.add_argument("--source", required=True, choices=[SOURCE_NAME])
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument("--max-records", type=_record_limit, default=100)
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    return parser


def _captured_utc(clock: Clock) -> datetime:
    captured: datetime | None = None
    try:
        candidate = clock()
        if isinstance(candidate, datetime):
            captured = candidate
            if captured.tzinfo is None or captured.utcoffset() is None:
                captured = None
            elif captured is not None:
                captured = captured.astimezone(timezone.utc)
    except Exception:
        captured = None
    if captured is None:
        raise IngestionError("Raw ingest clock must include a valid timezone")
    return captured


def _validate_request_dates(
    *,
    start_date: date | None,
    end_date: date,
    run_ingested_at: datetime,
) -> None:
    if start_date is not None and start_date > end_date:
        raise IngestionError("start_date must be on or before end_date")
    if end_date >= run_ingested_at.date():
        raise IngestionError("end_date must be earlier than the captured UTC run date")


def _validate_local_options(symbol: str, max_records: int) -> None:
    if not isinstance(symbol, str) or INPUT_SYMBOL_PATTERN.fullmatch(symbol.strip()) is None:
        raise IngestionError("Alpha Vantage symbol contains unsupported characters")
    if (
        isinstance(max_records, bool)
        or not isinstance(max_records, int)
        or not 1 <= max_records <= 100
    ):
        raise IngestionError("max_records must be between 1 and 100")


def _api_key(environment: Mapping[str, str]) -> str:
    configured = environment.get(API_KEY_ENV, "")
    if not isinstance(configured, str):
        raise IngestionError("Alpha Vantage API key has an invalid format")
    value = configured.strip()
    if not value:
        raise IngestionError("Alpha Vantage API key is required")
    if len(value) > 256 or not value.isascii() or any(
        not 33 <= ord(character) <= 126 for character in value
    ):
        raise IngestionError("Alpha Vantage API key has an invalid format")
    return value


def _load_valid_storage_config(path: Path, loader: ConfigLoader) -> dict[str, Any]:
    configured: dict[str, Any] | None = None
    try:
        candidate = loader(path)
        if isinstance(candidate, dict):
            configured = candidate
    except Exception:
        configured = None
    if configured is None:
        raise StorageError("Storage configuration is invalid")
    return configured


def _validated_events(
    events: list[MarketEvent],
    *,
    requested_symbol: str,
    start_date: date | None,
    end_date: date,
    run_ingested_at: datetime,
    max_records: int,
) -> tuple[list[dict[str, Any]], str, str, date, date]:
    if not events:
        raise _EventContractError("Alpha Vantage selection returned no daily records")
    if len(events) > max_records:
        raise _EventContractError("Alpha Vantage adapter exceeded the requested record limit")

    validated: list[MarketEvent] = []
    try:
        validated = [MarketEvent.model_validate(event) for event in events]
    except PydanticValidationError:
        raise _EventContractError(
            "Alpha Vantage adapter returned an invalid event contract"
        ) from None

    requested_canonical_symbol = requested_symbol.strip().upper()
    event_ids = [event.event_id for event in validated]
    if len(set(event_ids)) != len(event_ids):
        raise _EventContractError("Alpha Vantage adapter returned duplicate event IDs")

    symbols = {event.symbol for event in validated}
    if len(symbols) != 1:
        raise _EventContractError("Alpha Vantage adapter returned more than one symbol")
    canonical_symbol = next(iter(symbols))
    if canonical_symbol != requested_canonical_symbol:
        raise _EventContractError("Alpha Vantage adapter returned an unexpected symbol")
    if {event.source for event in validated} != {SOURCE_NAME}:
        raise _EventContractError("Alpha Vantage adapter returned an unexpected source")

    event_dates: list[date] = []
    ingest_partitions: set[str] = set()
    for event in validated:
        if event.ts_event.tzinfo is None or event.ts_event.utcoffset() is None:
            raise _EventContractError("Alpha Vantage event timestamp must include a timezone")
        event_timestamp = event.ts_event.astimezone(timezone.utc)
        if event_timestamp.time().replace(tzinfo=None) != datetime.min.time():
            raise _EventContractError(
                "Alpha Vantage daily event timestamp must be UTC midnight"
            )
        event_date = event_timestamp.date()
        if (start_date is not None and event_date < start_date) or event_date > end_date:
            raise _EventContractError(
                "Alpha Vantage adapter returned a date outside the request"
            )
        if event.event_id != alpha_vantage_daily_event_id(canonical_symbol, event_date):
            raise _EventContractError(
                "Alpha Vantage adapter returned an unexpected event identity"
            )
        event_dates.append(event_date)

        if event.ts_ingest.tzinfo is None or event.ts_ingest.utcoffset() is None:
            raise _EventContractError("Alpha Vantage ingest timestamp must include a timezone")
        ingest_timestamp = event.ts_ingest.astimezone(timezone.utc)
        if ingest_timestamp != run_ingested_at:
            raise _EventContractError(
                "Alpha Vantage events must share the captured ingest timestamp"
            )
        ingest_partitions.add(partition_path(ingest_timestamp))

    if len(ingest_partitions) != 1:
        raise _EventContractError("Alpha Vantage events must share one raw ingest partition")
    if len(set(event_dates)) != len(event_dates):
        raise _EventContractError(
            "Alpha Vantage adapter returned duplicate daily observations"
        )

    records = [event.model_dump() for event in validated]
    return (
        records,
        canonical_symbol,
        next(iter(ingest_partitions)),
        min(event_dates),
        max(event_dates),
    )


def ingest_alpha_vantage_daily(
    *,
    symbol: str,
    start_date: date | None,
    end_date: date,
    max_records: int,
    storage_config_path: Path,
    environment: Mapping[str, str] | None = None,
    fetcher: Fetcher | None = None,
    writer: Writer | None = None,
    config_loader: ConfigLoader | None = None,
    clock: Clock | None = None,
) -> dict[str, Any]:
    selected_clock = clock or utc_now
    run_ingested_at = _captured_utc(selected_clock)
    run_id = str(uuid4())
    _validate_request_dates(
        start_date=start_date,
        end_date=end_date,
        run_ingested_at=run_ingested_at,
    )
    _validate_local_options(symbol, max_records)
    secret = _api_key(environment if environment is not None else os.environ)

    selected_loader = config_loader or load_storage_config
    storage_config = _load_valid_storage_config(storage_config_path, selected_loader)
    raw_dataset: str | None = None
    try:
        raw_dataset = validate_raw_storage_destination(storage_config)
    except Exception:
        raw_dataset = None
    if raw_dataset is None:
        raise StorageError("Storage configuration is invalid")

    selected_fetcher = fetcher or fetch_alpha_vantage_daily_events
    events: list[MarketEvent] | None = None
    try:
        events = selected_fetcher(
            symbol=symbol,
            api_key=secret,
            ingested_at=run_ingested_at,
            start_date=start_date,
            end_date=end_date,
            max_records=max_records,
        )
    except Exception:
        events = None
    if events is None:
        raise IngestionError("Alpha Vantage source request failed")

    validated_result: tuple[list[dict[str, Any]], str, str, date, date] | None = None
    validation_message: str | None = None
    try:
        validated_result = _validated_events(
            events,
            requested_symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            run_ingested_at=run_ingested_at,
            max_records=max_records,
        )
    except _EventContractError as exc:
        validation_message = str(exc)
    except Exception:
        validated_result = None
    if validated_result is None:
        raise IngestionError(
            validation_message or "Alpha Vantage source returned invalid daily data"
        )
    records, canonical_symbol, ingest_partition, first_date, last_date = validated_result

    selected_writer = writer or write_records
    records_written: int | None = None
    raw_conflict = False
    publication_ambiguous = False
    try:
        candidate_written = selected_writer(
            records,
            kind="raw",
            dataset=raw_dataset,
            storage_config=storage_config,
        )
        if type(candidate_written) is int:
            records_written = candidate_written
        else:
            publication_ambiguous = True
    except RawEventConflictError:
        raw_conflict = True
    except Exception:
        publication_ambiguous = True
    if raw_conflict:
        raise RawEventConflictError(
            "Alpha Vantage daily data conflicts with the immutable raw event version"
        )
    if (
        publication_ambiguous
        or records_written is None
        or not 0 <= records_written <= len(records)
    ):
        raise _RawPublicationAmbiguousError(
            "Raw publication outcome is ambiguous; raw data may have been committed; "
            "rerun is safe"
        )

    summary = {
        "run_id": run_id,
        "run_ingested_at": run_ingested_at.isoformat(),
        "source": {
            "name": SOURCE_NAME,
            "operation": SOURCE_OPERATION,
            "symbol": canonical_symbol,
            "start_date": start_date.isoformat() if start_date is not None else None,
            "end_date": end_date.isoformat(),
            "max_records": max_records,
            "semantics": SOURCE_SEMANTICS,
        },
        "selection": {
            "first_event_date": first_date.isoformat(),
            "last_event_date": last_date.isoformat(),
        },
        "raw_output": {
            "dataset": raw_dataset,
            "location": f"local_parquet:{raw_dataset}",
            "records_selected": len(records),
            "records_written": records_written,
            "records_already_present": len(records) - records_written,
            "partitions_written": [ingest_partition] if records_written else [],
        },
    }
    return summary


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        summary = ingest_alpha_vantage_daily(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            max_records=args.max_records,
            storage_config_path=args.storage_config,
        )
    except RawEventConflictError:
        print("Alpha Vantage raw ingest failed: raw event conflict", file=sys.stderr)
        return 1
    except IngestionError:
        print("Alpha Vantage raw ingest failed: source data was unavailable or invalid", file=sys.stderr)
        return 1
    except _RawPublicationAmbiguousError:
        print(
            "Alpha Vantage raw ingest failed: raw data may have been committed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print("Alpha Vantage raw ingest failed: local storage operation failed", file=sys.stderr)
        return 1
    except Exception:
        print("Alpha Vantage raw ingest failed: unexpected local failure", file=sys.stderr)
        return 1
    try:
        print(json.dumps(summary, sort_keys=True))
    except Exception:
        print(
            "Alpha Vantage raw ingest failed: raw data may have been committed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
