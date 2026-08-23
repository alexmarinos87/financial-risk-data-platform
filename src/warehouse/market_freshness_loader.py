from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.common.exceptions import StorageError
from src.storage.storage_config import load_storage_config, validate_storage_config
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

DATASET_KEY = "daily_market_freshness"
TABLE_NAME = "daily_market_freshness"
MAX_INPUT_FILES = 4_096
MAX_INPUT_BYTES = 1_000_000_000
MAX_INPUT_ROWS = 250_000

DATE_COLUMNS = frozenset(
    {
        "calendar_valid_from",
        "calendar_valid_to",
        "as_of_date",
        "first_observation_date",
        "latest_observation_date",
        "expected_latest_session_date",
    }
)

COLUMNS = (
    "calculation_id",
    "model_version",
    "calendar_id",
    "calendar_fingerprint",
    "calendar_timezone",
    "calendar_valid_from",
    "calendar_valid_to",
    "source",
    "symbol",
    "as_of_date",
    "as_of_day_type",
    "ts_event",
    "ts_ingest",
    "first_observation_date",
    "latest_observation_date",
    "expected_latest_session_date",
    "expected_session_close_time",
    "expected_session_is_early_close",
    "observation_count",
    "expected_session_count",
    "missing_session_count",
    "trailing_missing_session_count",
    "missing_sessions_json",
    "freshness_status",
    "input_fingerprint",
)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _dataset_files(storage_config: dict[str, Any]) -> list[Path]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    datasets = storage["curated"]["datasets"]
    if DATASET_KEY not in datasets:
        raise StorageError(
            f"Storage configuration is missing '{DATASET_KEY}'"
        )
    curated_base = Path(storage["curated"]["base_path"])
    dataset_path = curated_base / datasets[DATASET_KEY]
    if curated_base.is_symlink() or dataset_path.is_symlink():
        raise StorageError(
            "Market-freshness warehouse path must not be a symbolic link"
        )
    if not dataset_path.exists():
        return []
    if not dataset_path.is_dir():
        raise StorageError(
            "Market-freshness warehouse path must be a directory"
        )
    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError(
            "Market-freshness warehouse input could not be inventoried"
        ) from None
    if len(files) > MAX_INPUT_FILES:
        raise StorageError(
            "Market-freshness warehouse input exceeds the file scan limit"
        )

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError(
                "Market-freshness warehouse input contains an unsafe file type"
            )
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError(
                "Market-freshness warehouse input could not be inventoried"
            ) from None
        if total_bytes > MAX_INPUT_BYTES:
            raise StorageError(
                "Market-freshness warehouse input exceeds the byte scan limit"
            )
    return files


def _normalise_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    if pd.isna(value) is True:
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "date") and value.__class__.__module__.startswith("pandas"):
        return value.date()
    return value


def collect_market_freshness_records(
    storage_config_path: Path = Path("config/storage.yaml"),
) -> list[dict[str, Any]]:
    storage_config = load_storage_config(storage_config_path)
    files = _dataset_files(storage_config)
    if not files:
        return []

    paths = ", ".join(
        f"'{str(path).replace(chr(39), chr(39) * 2)}'" for path in files
    )
    relation = (
        f"read_parquet([{paths}], union_by_name=true, "
        "hive_partitioning=false)"
    )
    selected = ", ".join(_quote_identifier(column) for column in COLUMNS)
    try:
        with duckdb.connect() as connection:
            count_row = connection.execute(
                f"SELECT COUNT(*) FROM {relation}"
            ).fetchone()
            if count_row is None:
                raise StorageError(
                    "Market-freshness query returned no row count"
                )
            if int(count_row[0]) > MAX_INPUT_ROWS:
                raise StorageError(
                    "Market-freshness input exceeds the row scan limit"
                )
            frame = connection.execute(
                f"SELECT {selected} FROM {relation} "
                "ORDER BY as_of_date, ts_ingest, calculation_id"
            ).df()
    except StorageError:
        raise
    except Exception:
        raise StorageError(
            "Unable to read market-freshness Parquet"
        ) from None

    records: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        record = {
            key: _normalise_value(value)
            for key, value in row.items()
        }
        for column in DATE_COLUMNS:
            value = record[column]
            if isinstance(value, datetime):
                record[column] = value.date()
        records.append(record)
    for record in records:
        payload = record["missing_sessions_json"]
        if not isinstance(payload, str):
            raise StorageError(
                "Market-freshness missing sessions must be JSON text"
            )
        try:
            parsed = json.loads(payload)
        except (TypeError, ValueError):
            raise StorageError(
                "Market-freshness missing sessions are invalid JSON"
            ) from None
        if not isinstance(parsed, list) or not all(
            isinstance(value, str) for value in parsed
        ):
            raise StorageError(
                "Market-freshness missing sessions must be a JSON string array"
            )
    return records


def build_upsert_sql(schema_name: str = "risk_platform") -> str:
    columns = ", ".join(_quote_identifier(column) for column in COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    updates = ", ".join(
        f"{_quote_identifier(column)} = "
        f"EXCLUDED.{_quote_identifier(column)}"
        for column in COLUMNS
        if column != "calculation_id"
    )
    return (
        f"INSERT INTO {_quote_identifier(schema_name)}."
        f"{_quote_identifier(TABLE_NAME)} ({columns}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({_quote_identifier('calculation_id')}) "
        f"DO UPDATE SET {updates}, loaded_at = now()"
    )


def load_market_freshness_to_postgres(
    *,
    dsn: str,
    storage_config_path: Path = Path("config/storage.yaml"),
    schema_name: str = "risk_platform",
    dry_run: bool = False,
) -> int:
    records = collect_market_freshness_records(storage_config_path)
    if dry_run or not records:
        return len(records)
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL loading requires psycopg. Run `make setup` first."
        ) from exc

    rows: list[tuple[Any, ...]] = []
    for record in records:
        values: list[Any] = []
        for column in COLUMNS:
            value = record[column]
            if column == "missing_sessions_json":
                value = Jsonb(json.loads(value))
            values.append(value)
        rows.append(tuple(values))

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(build_upsert_sql(schema_name), rows)
        connection.commit()
    return len(records)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load local market-freshness Parquet into PostgreSQL without "
            "requesting a provider."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get(
            "WAREHOUSE_POSTGRES_DSN",
            DEFAULT_POSTGRES_DSN,
        ),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--schema", default="risk_platform")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    count = load_market_freshness_to_postgres(
        dsn=args.dsn,
        storage_config_path=args.storage_config,
        schema_name=args.schema,
        dry_run=args.dry_run,
    )
    print(
        "Market-freshness warehouse load summary at "
        f"{datetime.utcnow().isoformat()}Z"
    )
    print(f"{TABLE_NAME}: {count}")
    print("provider_request_performed: false")


if __name__ == "__main__":
    main()
