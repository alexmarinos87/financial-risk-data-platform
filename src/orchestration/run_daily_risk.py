from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.daily_risk import DailyRiskOutputs, build_daily_risk_outputs
from ..common.exceptions import StorageError, ValidationError
from ..ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from ..ingestion.schemas import MarketEvent
from ..processing.normaliser import normalize_symbol
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config

SOURCE_NAME = "alpha_vantage"
DAILY_DATASETS = {
    "returns": "daily_returns",
    "volatility": "daily_volatility",
    "risk_summary": "daily_risk_summary",
}
CALENDAR_DATE_PATTERN = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,31}$")
MAX_RAW_FILES = 2_048
MAX_RAW_BYTES = 1_000_000_000
MAX_RAW_ROWS = 100_000
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

Reader = Callable[..., list[MarketEvent]]
Writer = Callable[..., int]
ConfigLoader = Callable[[Path], dict[str, Any]]


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


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer greater than zero") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a number between 0 and 1") from exc
    if not math.isfinite(parsed) or not 0 < parsed < 1:
        raise argparse.ArgumentTypeError("must be a number between 0 and 1")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build versioned daily risk analytics from local Alpha Vantage raw parquet."
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument("--vol-window", type=_positive_integer, default=20)
    parser.add_argument("--var-window", type=_positive_integer, default=60)
    parser.add_argument("--var-confidence", type=_confidence, default=0.95)
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _canonical_symbol(symbol: str) -> str:
    if not isinstance(symbol, str):
        raise ValidationError("symbol must be text")
    canonical = normalize_symbol(symbol)
    if SYMBOL_PATTERN.fullmatch(canonical) is None:
        raise ValidationError("symbol contains unsupported characters")
    return canonical


def _timestamp_from_epoch_microseconds(value: Any) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StorageError("Raw daily timestamp has an invalid physical value")
    try:
        return UTC_EPOCH + timedelta(microseconds=value)
    except (OverflowError, TypeError, ValueError):
        raise StorageError("Raw daily timestamp is outside the supported range") from None


def _raw_parquet_files(storage_config: dict[str, Any]) -> list[Path]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    raw_base = Path(storage["raw"]["base_path"])
    dataset_path = raw_base / storage["raw"]["dataset"]

    if raw_base.is_symlink() or dataset_path.is_symlink():
        raise StorageError("Raw daily storage path must not be a symbolic link")
    if not dataset_path.exists():
        raise ValidationError("No local raw market data is available")
    if not dataset_path.is_dir():
        raise StorageError("Raw daily storage path must be a directory")

    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError("Raw daily storage could not be inventoried") from None
    if not files:
        raise ValidationError("No local raw market data is available")
    if len(files) > MAX_RAW_FILES:
        raise StorageError("Raw daily storage exceeds the file scan limit")

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError("Raw daily storage contains an unsafe file type")
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError("Raw daily storage could not be inventoried") from None
        if total_bytes > MAX_RAW_BYTES:
            raise StorageError("Raw daily storage exceeds the byte scan limit")
    return files


def load_alpha_vantage_daily_events(
    *,
    storage_config: dict[str, Any],
    symbol: str,
    end_date: date,
) -> list[MarketEvent]:
    canonical_symbol = _canonical_symbol(symbol)
    files = _raw_parquet_files(storage_config)
    try:
        import duckdb
    except ImportError:
        raise StorageError("DuckDB is required to read local raw parquet") from None

    escaped_paths = [str(path).replace("'", "''") for path in files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    try:
        end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min, timezone.utc)
    except OverflowError:
        raise ValidationError("end_date is outside the supported range") from None
    end_exclusive_us = int((end_exclusive - UTC_EPOCH).total_seconds() * 1_000_000)
    relation = f"read_parquet([{quoted_paths}], union_by_name=true, hive_partitioning=false)"
    where_clause = "source = ? AND symbol = ? AND epoch_us(ts_event) < ?"

    try:
        with duckdb.connect() as connection:
            count_row = connection.execute(
                f"SELECT COUNT(*) FROM {relation} WHERE {where_clause}",
                [SOURCE_NAME, canonical_symbol, end_exclusive_us],
            ).fetchone()
            if count_row is None:
                raise StorageError("Raw daily query did not return a row count")
            row_count = int(count_row[0])
            if row_count > MAX_RAW_ROWS:
                raise StorageError("Raw daily storage exceeds the row scan limit")
            cursor = connection.execute(
                "SELECT event_id, symbol, price, volume, "
                "epoch_us(ts_event) AS ts_event, epoch_us(ts_ingest) AS ts_ingest, source "
                f"FROM {relation} WHERE {where_clause} ORDER BY ts_event, event_id",
                [SOURCE_NAME, canonical_symbol, end_exclusive_us],
            )
            rows = cursor.fetchall()
    except StorageError:
        raise
    except Exception:
        raise StorageError("Unable to read local raw daily parquet") from None

    events: list[MarketEvent] = []
    try:
        for row in rows:
            event = MarketEvent(
                event_id=str(row[0]),
                symbol=str(row[1]),
                price=float(row[2]),
                volume=int(row[3]),
                ts_event=_timestamp_from_epoch_microseconds(row[4]),
                ts_ingest=_timestamp_from_epoch_microseconds(row[5]),
                source=str(row[6]),
            )
            event_date = event.ts_event.astimezone(timezone.utc).date()
            if event.event_id != alpha_vantage_daily_event_id(canonical_symbol, event_date):
                raise ValidationError("Raw Alpha Vantage daily event identity is invalid")
            if event.ts_event.astimezone(timezone.utc).time() != time.min:
                raise ValidationError("Raw Alpha Vantage daily timestamps must use UTC midnight")
            events.append(event)
    except ValidationError:
        raise
    except Exception:
        raise StorageError("Raw Alpha Vantage daily records are incompatible") from None

    if not events:
        raise ValidationError("No Alpha Vantage daily raw events matched the request")
    return events


def _require_daily_datasets(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    missing = sorted(dataset for dataset in DAILY_DATASETS.values() if dataset not in datasets)
    if missing:
        raise StorageError("Storage configuration is missing daily curated datasets")


def _publish_records(
    records: tuple[dict[str, Any], ...],
    *,
    dataset: str,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for record in records:
        try:
            result = writer(
                [record],
                kind="curated",
                dataset=dataset,
                storage_config=storage_config,
            )
        except Exception:
            raise StorageError("Daily curated publication failed; rerun is safe") from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError("Daily curated publication returned an invalid result")
        written += result
    return written


def run_daily_risk(
    *,
    symbol: str,
    start_date: date | None,
    end_date: date,
    volatility_window: int,
    var_window: int,
    var_confidence: float,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    config_loader: ConfigLoader | None = None,
) -> dict[str, Any]:
    canonical_symbol = _canonical_symbol(symbol)
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    selected_loader = config_loader or load_storage_config
    try:
        storage_config = selected_loader(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("Storage configuration is invalid")
    _require_daily_datasets(storage_config)

    selected_reader = reader or load_alpha_vantage_daily_events
    try:
        events = selected_reader(
            storage_config=storage_config,
            symbol=canonical_symbol,
            end_date=end_date,
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local raw daily data") from None

    outputs: DailyRiskOutputs = build_daily_risk_outputs(
        events,
        volatility_window=volatility_window,
        var_window=var_window,
        var_confidence=var_confidence,
        start_date=start_date,
        end_date=end_date,
    )
    selected_writer = writer or write_records

    records_by_dataset = {
        DAILY_DATASETS["returns"]: outputs.returns,
        DAILY_DATASETS["volatility"]: outputs.volatility,
        DAILY_DATASETS["risk_summary"]: outputs.risk_summary,
    }
    written_by_dataset = {
        dataset: _publish_records(
            records,
            dataset=dataset,
            storage_config=storage_config,
            writer=selected_writer,
        )
        for dataset, records in records_by_dataset.items()
    }

    selected_count_by_dataset = {
        dataset: len(records) for dataset, records in records_by_dataset.items()
    }
    latest = outputs.risk_summary[-1]
    return {
        "run_id": str(uuid4()),
        "source": SOURCE_NAME,
        "symbol": canonical_symbol,
        "selection": {
            "start_date": start_date.isoformat() if start_date is not None else None,
            "end_date": end_date.isoformat(),
            "raw_observations": len(events),
            "first_raw_event_date": min(event.ts_event for event in events).date().isoformat(),
            "last_raw_event_date": max(event.ts_event for event in events).date().isoformat(),
        },
        "parameters": {
            "volatility_window": volatility_window,
            "var_window": var_window,
            "var_confidence": float(var_confidence),
        },
        "curated_output": {
            dataset: {
                "records_selected": selected_count_by_dataset[dataset],
                "records_written": written_by_dataset[dataset],
                "records_already_present": (
                    selected_count_by_dataset[dataset] - written_by_dataset[dataset]
                ),
            }
            for dataset in records_by_dataset
        },
        "latest_metrics": {
            "ts_event": latest["ts_event"].isoformat(),
            "price_close": latest["price_close"],
            "return_1d": latest["return_1d"],
            "volatility_annualized": latest["volatility_annualized"],
            "historical_var_loss": latest["historical_var_loss"],
            "maximum_drawdown": latest["maximum_drawdown"],
            "history_status": latest["history_status"],
            "calculation_id": latest["calculation_id"],
        },
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise StorageError("Unable to write the daily risk summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_daily_risk(
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            volatility_window=args.vol_window,
            var_window=args.var_window,
            var_confidence=args.var_confidence,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print("Daily risk pipeline failed: raw daily data or options were invalid", file=sys.stderr)
        return 1
    except StorageError:
        print(
            "Daily risk pipeline failed: local storage operation failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Daily risk pipeline failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
