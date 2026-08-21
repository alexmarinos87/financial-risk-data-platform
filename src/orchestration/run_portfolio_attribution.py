from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_attribution import (
    PortfolioAttributionOutput,
    build_portfolio_attribution,
)
from ..analytics.portfolio_risk import (
    PortfolioDefinition,
    load_portfolio_definition,
)
from ..common.exceptions import StorageError, ValidationError
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config

INPUT_DATASET = "portfolio_daily_returns"
OUTPUT_DATASET = "portfolio_risk_attribution"
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
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        ) from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be an integer greater than one"
        ) from exc
    if parsed < 2:
        raise argparse.ArgumentTypeError("must be greater than one")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a covariance, correlation and component-risk snapshot from "
            "local portfolio-return parquet."
        )
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument("--covariance-window", type=_positive_integer, default=20)
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _timestamp_from_epoch_microseconds(value: Any) -> datetime:
    if isinstance(value, bool) or not isinstance(value, int):
        raise StorageError(
            "Portfolio attribution input timestamp has an invalid physical value"
        )
    try:
        return UTC_EPOCH + timedelta(microseconds=value)
    except (OverflowError, TypeError, ValueError):
        raise StorageError(
            "Portfolio attribution input timestamp is outside the supported range"
        ) from None


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
        raise StorageError(
            "Portfolio attribution input path must not be a symbolic link"
        )
    if not dataset_path.exists():
        raise ValidationError("No local portfolio return data is available")
    if not dataset_path.is_dir():
        raise StorageError("Portfolio attribution input path must be a directory")

    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError(
            "Portfolio attribution input could not be inventoried"
        ) from None
    if not files:
        raise ValidationError("No local portfolio return data is available")
    if len(files) > MAX_INPUT_FILES:
        raise StorageError("Portfolio attribution input exceeds the file scan limit")

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError(
                "Portfolio attribution input contains an unsafe file type"
            )
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError(
                "Portfolio attribution input could not be inventoried"
            ) from None
        if total_bytes > MAX_INPUT_BYTES:
            raise StorageError(
                "Portfolio attribution input exceeds the byte scan limit"
            )
    return files


def load_portfolio_return_records(
    *,
    storage_config: dict[str, Any],
    portfolio_id: str,
    definition_fingerprint: str,
    end_date: date,
) -> list[dict[str, Any]]:
    files = _dataset_files(storage_config, INPUT_DATASET)
    try:
        import duckdb
    except ImportError:
        raise StorageError(
            "DuckDB is required to read local portfolio returns"
        ) from None

    escaped_paths = [str(path).replace("'", "''") for path in files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    relation = (
        f"read_parquet([{quoted_paths}], union_by_name=true, "
        "hive_partitioning=false)"
    )
    try:
        end_exclusive = datetime.combine(
            end_date + timedelta(days=1),
            time.min,
            timezone.utc,
        )
    except OverflowError:
        raise ValidationError("end_date is outside the supported range") from None
    end_exclusive_us = int(
        (end_exclusive - UTC_EPOCH).total_seconds() * 1_000_000
    )
    where_clause = (
        "portfolio_id = ? AND definition_fingerprint = ? "
        "AND epoch_us(ts_event) < ?"
    )
    parameters = [portfolio_id, definition_fingerprint, end_exclusive_us]

    try:
        with duckdb.connect() as connection:
            count_row = connection.execute(
                f"SELECT COUNT(*) FROM {relation} WHERE {where_clause}",
                parameters,
            ).fetchone()
            if count_row is None:
                raise StorageError(
                    "Portfolio attribution query did not return a row count"
                )
            row_count = int(count_row[0])
            if row_count > MAX_INPUT_ROWS:
                raise StorageError(
                    "Portfolio attribution input exceeds the row scan limit"
                )
            cursor = connection.execute(
                "SELECT model_version, calculation_id, portfolio_id, base_currency, "
                "definition_fingerprint, weighting_method, "
                "epoch_us(ts_event) AS ts_event, "
                "epoch_us(ts_ingest) AS ts_ingest, constituent_count, weights_json, "
                "component_calculation_ids_json, component_returns_json, "
                "portfolio_return_1d "
                f"FROM {relation} WHERE {where_clause} "
                "ORDER BY ts_event, ts_ingest, calculation_id",
                parameters,
            )
            rows = cursor.fetchall()
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError(
            "Unable to read local portfolio return parquet"
        ) from None

    records: list[dict[str, Any]] = []
    try:
        for row in rows:
            records.append(
                {
                    "model_version": row[0],
                    "calculation_id": row[1],
                    "portfolio_id": row[2],
                    "base_currency": row[3],
                    "definition_fingerprint": row[4],
                    "weighting_method": row[5],
                    "ts_event": _timestamp_from_epoch_microseconds(row[6]),
                    "ts_ingest": _timestamp_from_epoch_microseconds(row[7]),
                    "constituent_count": int(row[8]),
                    "weights_json": row[9],
                    "component_calculation_ids_json": row[10],
                    "component_returns_json": row[11],
                    "portfolio_return_1d": float(row[12]),
                }
            )
    except StorageError:
        raise
    except Exception:
        raise StorageError(
            "Portfolio return records are incompatible"
        ) from None
    if not records:
        raise ValidationError(
            "No portfolio returns matched the requested portfolio and end date"
        )
    return records


def _require_datasets(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    missing = sorted(
        dataset
        for dataset in (INPUT_DATASET, OUTPUT_DATASET)
        if dataset not in datasets
    )
    if missing:
        raise StorageError(
            "Storage configuration is missing portfolio attribution datasets: "
            + ", ".join(missing)
        )


def _publish_snapshot(
    snapshot: dict[str, Any],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    try:
        result = writer(
            [snapshot],
            kind="curated",
            dataset=OUTPUT_DATASET,
            storage_config=storage_config,
        )
    except Exception:
        raise StorageError(
            "Portfolio attribution publication failed; rerun is safe"
        ) from None
    if type(result) is not int or result not in {0, 1}:
        raise StorageError(
            "Portfolio attribution publication returned an invalid result"
        )
    return result


def run_portfolio_attribution(
    *,
    portfolio_id: str,
    portfolio_config_path: Path,
    end_date: date,
    covariance_window: int,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    definition_loader: DefinitionLoader | None = None,
) -> dict[str, Any]:
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

    selected_reader = reader or load_portfolio_return_records
    try:
        records = selected_reader(
            storage_config=storage_config,
            portfolio_id=definition.portfolio_id,
            definition_fingerprint=definition.fingerprint,
            end_date=end_date,
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local portfolio returns") from None

    output: PortfolioAttributionOutput = build_portfolio_attribution(
        records,
        definition=definition,
        covariance_window=covariance_window,
        end_date=end_date,
    )
    selected_writer = writer or write_records
    written = _publish_snapshot(
        output.snapshot,
        storage_config=storage_config,
        writer=selected_writer,
    )

    components = json.loads(
        output.snapshot["component_volatility_contribution_json"]
    )
    largest_component = max(
        sorted(components),
        key=lambda key: abs(float(components[key])),
    )
    return {
        "run_id": str(uuid4()),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "selection": {
            "end_date": end_date.isoformat(),
            **dict(output.diagnostics),
        },
        "parameters": {
            "covariance_window": covariance_window,
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": 1,
                "records_written": written,
                "records_already_present": 1 - written,
            }
        },
        "latest_metrics": {
            "ts_event": output.snapshot["ts_event"].isoformat(),
            "portfolio_variance_annualized": output.snapshot[
                "portfolio_variance_annualized"
            ],
            "portfolio_volatility_annualized": output.snapshot[
                "portfolio_volatility_annualized"
            ],
            "volatility_status": output.snapshot["volatility_status"],
            "correlation_status": output.snapshot["correlation_status"],
            "largest_absolute_component": largest_component,
            "largest_absolute_component_contribution": components[
                largest_component
            ],
            "calculation_id": output.snapshot["calculation_id"],
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
        raise StorageError(
            "Unable to write the portfolio attribution summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_attribution(
            portfolio_id=args.portfolio_id,
            portfolio_config_path=args.portfolio_config,
            end_date=args.end_date,
            covariance_window=args.covariance_window,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Portfolio attribution failed: configuration, input data or options "
            "were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio attribution failed: local storage operation failed; "
            "rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio attribution failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
