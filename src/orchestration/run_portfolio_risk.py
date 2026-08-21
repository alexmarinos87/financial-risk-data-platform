from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_risk import (
    PortfolioDefinition,
    PortfolioRiskOutputs,
    build_portfolio_risk_outputs,
    load_portfolio_definition,
)
from ..common.exceptions import StorageError, ValidationError
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config

INPUT_DATASET = "daily_returns"
OUTPUT_DATASETS = {
    "returns": "portfolio_daily_returns",
    "risk_summary": "portfolio_daily_risk_summary",
}
MAX_INPUT_FILES = 4_096
MAX_INPUT_BYTES = 1_000_000_000
MAX_INPUT_ROWS = 250_000
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

Reader = Callable[..., list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
DefinitionLoader = Callable[[Path, str], PortfolioDefinition]


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a calendar date in YYYY-MM-DD format") from exc
    if value != parsed.isoformat():
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
        description="Build versioned portfolio risk from local daily-return parquet."
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
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


def _timestamp_from_epoch_microseconds(value: Any) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StorageError("Portfolio input timestamp has an invalid physical value")
    try:
        return UTC_EPOCH + timedelta(microseconds=value)
    except (OverflowError, TypeError, ValueError):
        raise StorageError("Portfolio input timestamp is outside the supported range") from None


def _dataset_files(
    storage_config: dict[str, Any],
    dataset_key: str,
) -> list[Path]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    datasets = storage["curated"]["datasets"]
    if dataset_key not in datasets:
        raise StorageError(f"Storage configuration is missing '{dataset_key}'")
    curated_base = Path(storage["curated"]["base_path"])
    dataset_path = curated_base / datasets[dataset_key]

    if curated_base.is_symlink() or dataset_path.is_symlink():
        raise StorageError("Portfolio input path must not be a symbolic link")
    if not dataset_path.exists():
        raise ValidationError("No local daily return data is available")
    if not dataset_path.is_dir():
        raise StorageError("Portfolio input path must be a directory")

    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError("Portfolio input could not be inventoried") from None
    if not files:
        raise ValidationError("No local daily return data is available")
    if len(files) > MAX_INPUT_FILES:
        raise StorageError("Portfolio input exceeds the file scan limit")

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError("Portfolio input contains an unsafe file type")
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError("Portfolio input could not be inventoried") from None
        if total_bytes > MAX_INPUT_BYTES:
            raise StorageError("Portfolio input exceeds the byte scan limit")
    return files


def load_daily_return_records(
    *,
    storage_config: dict[str, Any],
    end_date: date,
) -> list[dict[str, Any]]:
    files = _dataset_files(storage_config, INPUT_DATASET)
    try:
        import duckdb
    except ImportError:
        raise StorageError("DuckDB is required to read local daily returns") from None

    escaped_paths = [str(path).replace("'", "''") for path in files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    relation = f"read_parquet([{quoted_paths}], union_by_name=true, hive_partitioning=false)"
    try:
        end_exclusive = datetime.combine(end_date + timedelta(days=1), time.min, timezone.utc)
    except OverflowError:
        raise ValidationError("end_date is outside the supported range") from None
    end_exclusive_us = int((end_exclusive - UTC_EPOCH).total_seconds() * 1_000_000)

    try:
        with duckdb.connect() as connection:
            count_row = connection.execute(
                f"SELECT COUNT(*) FROM {relation} WHERE epoch_us(ts_event) < ?",
                [end_exclusive_us],
            ).fetchone()
            if count_row is None:
                raise StorageError("Portfolio input query did not return a row count")
            row_count = int(count_row[0])
            if row_count > MAX_INPUT_ROWS:
                raise StorageError("Portfolio input exceeds the row scan limit")
            cursor = connection.execute(
                "SELECT model_version, calculation_id, source, symbol, source_event_id, "
                "epoch_us(ts_event) AS ts_event, epoch_us(ts_ingest) AS ts_ingest, return_1d "
                f"FROM {relation} WHERE epoch_us(ts_event) < ? "
                "ORDER BY ts_event, source, symbol, ts_ingest, calculation_id",
                [end_exclusive_us],
            )
            rows = cursor.fetchall()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local daily return parquet") from None

    records: list[dict[str, Any]] = []
    try:
        for row in rows:
            records.append(
                {
                    "model_version": str(row[0]),
                    "calculation_id": str(row[1]),
                    "source": str(row[2]),
                    "symbol": str(row[3]),
                    "source_event_id": str(row[4]),
                    "ts_event": _timestamp_from_epoch_microseconds(row[5]),
                    "ts_ingest": _timestamp_from_epoch_microseconds(row[6]),
                    "return_1d": float(row[7]),
                }
            )
    except StorageError:
        raise
    except Exception:
        raise StorageError("Daily return records are incompatible") from None
    if not records:
        raise ValidationError("No daily returns matched the requested end date")
    return records


def _require_datasets(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    required = {INPUT_DATASET, *OUTPUT_DATASETS.values()}
    missing = sorted(dataset for dataset in required if dataset not in datasets)
    if missing:
        raise StorageError(
            "Storage configuration is missing portfolio datasets: " + ", ".join(missing)
        )


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
            raise StorageError("Portfolio curated publication failed; rerun is safe") from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError("Portfolio curated publication returned an invalid result")
        written += result
    return written


def run_portfolio_risk(
    *,
    portfolio_id: str,
    portfolio_config_path: Path,
    start_date: date | None,
    end_date: date,
    volatility_window: int,
    var_window: int,
    var_confidence: float,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    definition_loader: DefinitionLoader | None = None,
) -> dict[str, Any]:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    selected_storage_loader = storage_config_loader or load_storage_config
    try:
        storage_config = selected_storage_loader(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("Storage configuration is invalid")
    _require_datasets(storage_config)

    selected_definition_loader = definition_loader or load_portfolio_definition
    try:
        definition = selected_definition_loader(portfolio_config_path, portfolio_id)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Portfolio configuration is invalid") from None

    selected_reader = reader or load_daily_return_records
    try:
        records = selected_reader(storage_config=storage_config, end_date=end_date)
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local daily returns") from None

    outputs: PortfolioRiskOutputs = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=volatility_window,
        var_window=var_window,
        var_confidence=var_confidence,
        start_date=start_date,
        end_date=end_date,
    )
    selected_writer = writer or write_records
    records_by_dataset = {
        OUTPUT_DATASETS["returns"]: outputs.returns,
        OUTPUT_DATASETS["risk_summary"]: outputs.risk_summary,
    }
    written_by_dataset = {
        dataset: _publish_records(
            output_records,
            dataset=dataset,
            storage_config=storage_config,
            writer=selected_writer,
        )
        for dataset, output_records in records_by_dataset.items()
    }

    latest = outputs.risk_summary[-1]
    return {
        "run_id": str(uuid4()),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "selection": {
            "start_date": start_date.isoformat() if start_date is not None else None,
            "end_date": end_date.isoformat(),
            **dict(outputs.diagnostics),
        },
        "parameters": {
            "volatility_window": volatility_window,
            "var_window": var_window,
            "var_confidence": float(var_confidence),
        },
        "curated_output": {
            dataset: {
                "records_selected": len(output_records),
                "records_written": written_by_dataset[dataset],
                "records_already_present": len(output_records)
                - written_by_dataset[dataset],
            }
            for dataset, output_records in records_by_dataset.items()
        },
        "latest_metrics": {
            "ts_event": latest["ts_event"].isoformat(),
            "portfolio_return_1d": latest["portfolio_return_1d"],
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
        raise StorageError("Unable to write the portfolio risk summary") from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_risk(
            portfolio_id=args.portfolio_id,
            portfolio_config_path=args.portfolio_config,
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
        print(
            "Portfolio risk pipeline failed: configuration, input data or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio risk pipeline failed: local storage operation failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print("Portfolio risk pipeline failed: unexpected local failure", file=sys.stderr)
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
