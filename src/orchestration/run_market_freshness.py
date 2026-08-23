from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.market_calendar import (
    MarketCalendar,
    MarketFreshnessOutput,
    build_market_freshness,
    load_market_calendar,
)
from ..common.exceptions import StorageError, ValidationError
from ..ingestion.schemas import MarketEvent
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config
from .run_daily_risk import (
    SOURCE_NAME,
    _calendar_date,
    _canonical_symbol,
    load_alpha_vantage_daily_events,
)

OUTPUT_DATASET = "daily_market_freshness"

Reader = Callable[..., list[MarketEvent]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
CalendarLoader = Callable[[Path, str], MarketCalendar]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate exchange-calendar-aware daily market freshness from "
            "local Alpha Vantage raw Parquet."
        )
    )
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--as-of-date", required=True, type=_calendar_date)
    parser.add_argument("--calendar-id", default="XNYS")
    parser.add_argument(
        "--calendar-config",
        type=Path,
        default=Path("config/market_calendars.yaml"),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _require_datasets(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    if OUTPUT_DATASET not in datasets:
        raise StorageError(
            f"Storage configuration is missing '{OUTPUT_DATASET}'"
        )


def _publish(
    record: dict[str, Any],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    try:
        result = writer(
            [record],
            kind="curated",
            dataset=OUTPUT_DATASET,
            storage_config=storage_config,
        )
    except Exception:
        raise StorageError(
            "Market-freshness publication failed; rerun is safe"
        ) from None
    if type(result) is not int or result not in {0, 1}:
        raise StorageError(
            "Market-freshness publication returned an invalid result"
        )
    return result


def run_market_freshness(
    *,
    symbol: str,
    as_of_date: date,
    calendar_id: str,
    calendar_config_path: Path,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
) -> dict[str, Any]:
    canonical_symbol = _canonical_symbol(symbol)
    selected_storage_loader = storage_config_loader or load_storage_config
    try:
        storage_config = selected_storage_loader(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("Storage configuration is invalid")
    _require_datasets(storage_config)

    selected_calendar_loader = calendar_loader or load_market_calendar
    try:
        calendar = selected_calendar_loader(
            calendar_config_path,
            calendar_id,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "Market-calendar configuration is invalid"
        ) from None
    if not isinstance(calendar, MarketCalendar):
        raise ValidationError(
            "Market-calendar loader returned an invalid calendar"
        )

    selected_reader = reader or load_alpha_vantage_daily_events
    try:
        events = selected_reader(
            storage_config=storage_config,
            symbol=canonical_symbol,
            end_date=as_of_date,
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "Unable to read local daily market data"
        ) from None

    output: MarketFreshnessOutput = build_market_freshness(
        events,
        calendar=calendar,
        source=SOURCE_NAME,
        symbol=canonical_symbol,
        as_of_date=as_of_date,
    )
    selected_writer = writer or write_records
    written = _publish(
        output.record,
        storage_config=storage_config,
        writer=selected_writer,
    )
    return {
        "run_id": str(uuid4()),
        "source": SOURCE_NAME,
        "symbol": canonical_symbol,
        "calendar": {
            "calendar_id": calendar.calendar_id,
            "calendar_fingerprint": calendar.fingerprint,
            "timezone": calendar.timezone_name,
            "valid_from": calendar.valid_from.isoformat(),
            "valid_to": calendar.valid_to.isoformat(),
        },
        "selection": {
            "as_of_date": as_of_date.isoformat(),
            **dict(output.diagnostics),
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": 1,
                "records_written": written,
                "records_already_present": 1 - written,
            }
        },
        "latest_status": {
            "calculation_id": output.record["calculation_id"],
            "freshness_status": output.record["freshness_status"],
            "latest_observation_date": output.record[
                "latest_observation_date"
            ].isoformat(),
            "expected_latest_session_date": output.record[
                "expected_latest_session_date"
            ].isoformat(),
            "missing_session_count": output.record[
                "missing_session_count"
            ],
            "trailing_missing_session_count": output.record[
                "trailing_missing_session_count"
            ],
        },
        "provider_request_performed": False,
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
        raise StorageError(
            "Unable to write the market-freshness summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_market_freshness(
            symbol=args.symbol,
            as_of_date=args.as_of_date,
            calendar_id=args.calendar_id,
            calendar_config_path=args.calendar_config,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Market-freshness evaluation failed: calendar, data or options "
            "were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Market-freshness evaluation failed: local storage operation "
            "failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Market-freshness evaluation failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
