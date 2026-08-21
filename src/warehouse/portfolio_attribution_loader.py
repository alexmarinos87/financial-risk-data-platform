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

DEFAULT_POSTGRES_DSN = (
    "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
)
DATASET_KEY = "portfolio_risk_attribution"
TABLE_NAME = "portfolio_risk_attribution"
MAX_INPUT_FILES = 4_096
MAX_INPUT_BYTES = 1_000_000_000
MAX_INPUT_ROWS = 250_000

COLUMNS = (
    "calculation_id",
    "model_version",
    "portfolio_id",
    "base_currency",
    "definition_fingerprint",
    "weighting_method",
    "covariance_method",
    "correlation_method",
    "covariance_window",
    "window_start",
    "window_end",
    "window_observations",
    "annualization_days",
    "ts_event",
    "ts_ingest",
    "constituent_count",
    "weights_json",
    "input_calculation_ids_json",
    "input_first_calculation_id",
    "input_last_calculation_id",
    "covariance_annualized_json",
    "correlation_json",
    "constituent_volatility_annualized_json",
    "marginal_volatility_contribution_json",
    "component_volatility_contribution_json",
    "component_contribution_share_json",
    "portfolio_variance_annualized",
    "portfolio_volatility_annualized",
    "volatility_status",
    "correlation_status",
    "undefined_correlation_cells",
    "euler_residual",
)

JSONB_COLUMNS = frozenset(
    {
        "weights_json",
        "input_calculation_ids_json",
        "covariance_annualized_json",
        "correlation_json",
        "constituent_volatility_annualized_json",
        "marginal_volatility_contribution_json",
        "component_volatility_contribution_json",
        "component_contribution_share_json",
    }
)


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
            "Portfolio attribution warehouse path must not be a symbolic link"
        )
    if not dataset_path.exists():
        return []
    if not dataset_path.is_dir():
        raise StorageError(
            "Portfolio attribution warehouse path must be a directory"
        )

    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError(
            "Portfolio attribution warehouse input could not be inventoried"
        ) from None
    if len(files) > MAX_INPUT_FILES:
        raise StorageError(
            "Portfolio attribution warehouse input exceeds the file scan limit"
        )

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError(
                "Portfolio attribution warehouse input contains an unsafe file type"
            )
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError(
                "Portfolio attribution warehouse input could not be inventoried"
            ) from None
        if total_bytes > MAX_INPUT_BYTES:
            raise StorageError(
                "Portfolio attribution warehouse input exceeds the byte scan limit"
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
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return value


def collect_attribution_records(
    storage_config_path: Path = Path("config/storage.yaml"),
) -> list[dict[str, Any]]:
    storage_config = load_storage_config(storage_config_path)
    files = _dataset_files(storage_config)
    if not files:
        return []

    escaped_paths = [str(path).replace("'", "''") for path in files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    relation = (
        f"read_parquet([{quoted_paths}], union_by_name=true, "
        "hive_partitioning=false)"
    )
    selected_columns = ", ".join(_quote_identifier(column) for column in COLUMNS)

    try:
        with duckdb.connect() as connection:
            count_row = connection.execute(
                f"SELECT COUNT(*) FROM {relation}"
            ).fetchone()
            if count_row is None:
                raise StorageError(
                    "Portfolio attribution warehouse query returned no row count"
                )
            row_count = int(count_row[0])
            if row_count > MAX_INPUT_ROWS:
                raise StorageError(
                    "Portfolio attribution warehouse input exceeds the row scan limit"
                )
            frame = connection.execute(
                f"SELECT {selected_columns} FROM {relation} "
                "ORDER BY ts_event, ts_ingest, calculation_id"
            ).df()
    except StorageError:
        raise
    except Exception:
        raise StorageError(
            "Unable to read portfolio attribution parquet for warehouse loading"
        ) from None

    return [
        {
            key: _normalise_value(value)
            for key, value in record.items()
        }
        for record in frame.to_dict("records")
    ]


def build_upsert_sql(schema_name: str = "risk_platform") -> str:
    quoted_columns = ", ".join(_quote_identifier(column) for column in COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    updates = ", ".join(
        f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
        for column in COLUMNS
        if column != "calculation_id"
    )
    return (
        f"INSERT INTO {_quote_identifier(schema_name)}."
        f"{_quote_identifier(TABLE_NAME)} ({quoted_columns}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({_quote_identifier('calculation_id')}) "
        f"DO UPDATE SET {updates}, loaded_at = now()"
    )


def load_attribution_to_postgres(
    *,
    dsn: str,
    storage_config_path: Path = Path("config/storage.yaml"),
    schema_name: str = "risk_platform",
    dry_run: bool = False,
) -> int:
    records = collect_attribution_records(storage_config_path)
    if dry_run or not records:
        return len(records)

    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL loading requires psycopg. Run `make setup` first."
        ) from exc

    statement = build_upsert_sql(schema_name)
    rows = [
        tuple(
            _postgres_value(
                record[column],
                jsonb=column in JSONB_COLUMNS,
                jsonb_wrapper=Jsonb,
            )
            for column in COLUMNS
        )
        for record in records
    ]

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(statement, rows)
        connection.commit()
    return len(records)


def _postgres_value(value: Any, *, jsonb: bool, jsonb_wrapper: Any) -> Any:
    if not jsonb or value is None:
        return value
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            raise StorageError(
                "Portfolio attribution warehouse JSON evidence is invalid"
            ) from None
    return jsonb_wrapper(value)


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load local portfolio-attribution parquet into PostgreSQL "
            "without recalculating analytics."
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
    count = load_attribution_to_postgres(
        dsn=args.dsn,
        storage_config_path=args.storage_config,
        schema_name=args.schema,
        dry_run=args.dry_run,
    )
    print(
        "Portfolio attribution warehouse load summary at "
        f"{datetime.utcnow().isoformat()}Z"
    )
    print(f"{TABLE_NAME}: {count}")


if __name__ == "__main__":
    main()
