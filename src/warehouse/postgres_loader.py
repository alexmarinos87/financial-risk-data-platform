from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from src.storage.storage_config import load_storage_config

DEFAULT_POSTGRES_DSN = "postgresql://risk_user:risk_password@localhost:5433/risk_platform"


@dataclass(frozen=True)
class TableLoadSpec:
    dataset_key: str
    table_name: str
    columns: tuple[str, ...]
    conflict_columns: tuple[str, ...]
    storage_kind: str = "curated"
    jsonb_columns: frozenset[str] = field(default_factory=frozenset)

    @property
    def update_columns(self) -> tuple[str, ...]:
        return tuple(column for column in self.columns if column not in self.conflict_columns)


LOAD_SPECS = (
    TableLoadSpec(
        dataset_key="market_events",
        table_name="market_events_raw",
        storage_kind="raw",
        columns=("event_id", "symbol", "price", "volume", "ts_event", "ts_ingest", "source"),
        conflict_columns=("event_id",),
    ),
    TableLoadSpec(
        dataset_key="returns_1m",
        table_name="returns_1m",
        columns=("symbol", "ts_event", "window_start", "return_1m", "ts_ingest"),
        conflict_columns=("symbol", "ts_event"),
    ),
    TableLoadSpec(
        dataset_key="volatility_5m",
        table_name="volatility_5m",
        columns=("symbol", "ts_event", "window_start", "volatility_5m", "ts_ingest"),
        conflict_columns=("symbol", "ts_event"),
    ),
    TableLoadSpec(
        dataset_key="data_quality_metrics",
        table_name="data_quality_metrics",
        columns=(
            "total_events",
            "deduped_events",
            "duplicate_events",
            "late_events",
            "late_rate",
            "duplicate_rate",
            "required_fields_checked",
            "missing_required_field_count",
            "missing_required_record_count",
            "missing_required_fields_by_name",
            "required_fields_status",
            "null_fields_checked",
            "null_field_count",
            "null_record_count",
            "max_null_rate",
            "null_fields_by_name",
            "null_rates_by_name",
            "null_rate_status",
            "value_fields_checked",
            "invalid_value_count",
            "invalid_value_record_count",
            "invalid_values_by_name",
            "value_validity_status",
            "late_status",
            "duplicate_status",
            "ts_ingest",
        ),
        conflict_columns=("ts_ingest",),
        jsonb_columns=frozenset(
            {
                "missing_required_fields_by_name",
                "null_fields_by_name",
                "null_rates_by_name",
                "invalid_values_by_name",
            }
        ),
    ),
    TableLoadSpec(
        dataset_key="risk_summary",
        table_name="risk_summary",
        columns=(
            "symbol",
            "ts_ingest",
            "volatility_5m",
            "value_at_risk_95",
            "volatility_status",
            "late_rate",
            "duplicate_rate",
            "late_status",
            "duplicate_status",
            "external_signal_count",
            "latest_external_signal_name",
            "latest_external_signal_value",
            "latest_external_signal_source",
            "latest_external_signal_ts_event",
        ),
        conflict_columns=("symbol", "ts_ingest"),
    ),
    TableLoadSpec(
        dataset_key="external_signal_summary",
        table_name="external_signal_summary",
        columns=(
            "name",
            "source",
            "latest_value",
            "latest_signal_id",
            "latest_ts_event",
            "ts_ingest",
        ),
        conflict_columns=("name", "source", "latest_signal_id"),
    ),
)


def _dataset_dir(storage_config: dict[str, Any], spec: TableLoadSpec) -> Path:
    storage = storage_config["storage"]
    if spec.storage_kind == "raw":
        return Path(storage["raw"]["base_path"]) / storage["raw"]["dataset"]
    dataset_name = storage["curated"]["datasets"][spec.dataset_key]
    return Path(storage["curated"]["base_path"]) / dataset_name


def _read_dataset_records(dataset_dir: Path, columns: tuple[str, ...]) -> list[dict[str, Any]]:
    parquet_files = sorted(dataset_dir.rglob("*.parquet"))
    if not parquet_files:
        return []

    escaped_paths = [str(path).replace("'", "''") for path in parquet_files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    with duckdb.connect() as conn:
        df = conn.execute(
            f"SELECT * FROM read_parquet([{quoted_paths}], union_by_name=true)"
        ).df()

    for column in columns:
        if column not in df.columns:
            df[column] = None
    df = df.loc[:, list(columns)]
    return [
        {key: _normalise_value(value) for key, value in record.items()}
        for record in df.to_dict("records")
    ]


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


def collect_load_batches(
    storage_config_path: Path = Path("config/storage.yaml"),
) -> dict[str, list[dict[str, Any]]]:
    storage_config = load_storage_config(storage_config_path)
    batches: dict[str, list[dict[str, Any]]] = {}
    for spec in LOAD_SPECS:
        batches[spec.table_name] = _read_dataset_records(
            _dataset_dir(storage_config, spec),
            spec.columns,
        )
    return batches


def build_upsert_sql(spec: TableLoadSpec, schema_name: str = "risk_platform") -> str:
    quoted_columns = ", ".join(_quote_identifier(column) for column in spec.columns)
    placeholders = ", ".join(["%s"] * len(spec.columns))
    conflict_columns = ", ".join(_quote_identifier(column) for column in spec.conflict_columns)
    updates = ", ".join(
        f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
        for column in spec.update_columns
    )
    return (
        f"INSERT INTO {_quote_identifier(schema_name)}.{_quote_identifier(spec.table_name)} "
        f"({quoted_columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {updates}, loaded_at = now()"
    )


def load_batches_to_postgres(
    *,
    dsn: str,
    storage_config_path: Path = Path("config/storage.yaml"),
    schema_name: str = "risk_platform",
    dry_run: bool = False,
) -> dict[str, int]:
    batches = collect_load_batches(storage_config_path)
    counts = {table_name: len(records) for table_name, records in batches.items()}
    if dry_run:
        return counts

    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL loading requires psycopg. Run `make setup` to install dependencies."
        ) from exc

    specs_by_table = {spec.table_name: spec for spec in LOAD_SPECS}
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cursor:
            for table_name, records in batches.items():
                if not records:
                    continue
                spec = specs_by_table[table_name]
                statement = build_upsert_sql(spec, schema_name=schema_name)
                rows = [
                    tuple(
                        _postgres_value(
                            record[column],
                            jsonb=column in spec.jsonb_columns,
                            jsonb_wrapper=Jsonb,
                        )
                        for column in spec.columns
                    )
                    for record in records
                ]
                cursor.executemany(statement, rows)
        conn.commit()
    return counts


def _postgres_value(value: Any, *, jsonb: bool, jsonb_wrapper: Any) -> Any:
    if not jsonb or value is None:
        return value
    if isinstance(value, str):
        value = json.loads(value)
    return jsonb_wrapper(value)


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load local parquet pipeline outputs into PostgreSQL warehouse tables."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
        help="PostgreSQL connection string. Defaults to WAREHOUSE_POSTGRES_DSN or the local Docker DSN.",
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
        help="Path to storage configuration YAML.",
    )
    parser.add_argument(
        "--schema",
        default="risk_platform",
        help="Target PostgreSQL schema.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned row counts without connecting to PostgreSQL.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    counts = load_batches_to_postgres(
        dsn=args.dsn,
        storage_config_path=args.storage_config,
        schema_name=args.schema,
        dry_run=args.dry_run,
    )
    print(f"Warehouse load summary at {datetime.utcnow().isoformat()}Z")
    for table_name, count in sorted(counts.items()):
        print(f"{table_name}: {count}")


if __name__ == "__main__":
    main()
