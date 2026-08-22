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

DATASET_KEY = "portfolio_risk_notification_outbox"
TABLE_NAME = "portfolio_risk_notification_outbox"
MAX_INPUT_FILES = 4_096
MAX_INPUT_BYTES = 1_000_000_000
MAX_INPUT_ROWS = 250_000

COLUMNS = (
    "event_id",
    "model_version",
    "event_type",
    "transition_type",
    "delivery_disposition",
    "suppression_reason",
    "source_evaluation_calculation_id",
    "source_previous_evaluation_calculation_id",
    "risk_limit_model_version",
    "policy_id",
    "policy_fingerprint",
    "portfolio_id",
    "base_currency",
    "definition_fingerprint",
    "attribution_model_version",
    "weighting_method",
    "covariance_method",
    "correlation_method",
    "covariance_window",
    "annualization_days",
    "ts_event",
    "ts_ingest",
    "metric_name",
    "subject_type",
    "subject_key",
    "previous_subject_key",
    "subject_changed",
    "unit",
    "previous_status",
    "current_status",
    "severity_rank",
    "observed_value",
    "observed_signed_value",
    "warning_threshold",
    "critical_threshold",
    "breach_excess",
    "payload_json",
)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _dataset_files(storage_config: dict[str, Any]) -> list[Path]:
    validate_storage_config(storage_config)
    storage = storage_config["storage"]
    datasets = storage["curated"]["datasets"]
    if DATASET_KEY not in datasets:
        raise StorageError(f"Storage configuration is missing '{DATASET_KEY}'")
    curated_base = Path(storage["curated"]["base_path"])
    dataset_path = curated_base / datasets[DATASET_KEY]
    if curated_base.is_symlink() or dataset_path.is_symlink():
        raise StorageError(
            "Portfolio notification-outbox warehouse path must not be a symbolic link"
        )
    if not dataset_path.exists():
        return []
    if not dataset_path.is_dir():
        raise StorageError(
            "Portfolio notification-outbox warehouse path must be a directory"
        )
    try:
        files = sorted(dataset_path.rglob("*.parquet"))
    except OSError:
        raise StorageError(
            "Portfolio notification-outbox input could not be inventoried"
        ) from None
    if len(files) > MAX_INPUT_FILES:
        raise StorageError(
            "Portfolio notification-outbox input exceeds the file scan limit"
        )

    total_bytes = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise StorageError(
                "Portfolio notification-outbox input contains an unsafe file type"
            )
        try:
            total_bytes += path.stat().st_size
        except OSError:
            raise StorageError(
                "Portfolio notification-outbox input could not be inventoried"
            ) from None
        if total_bytes > MAX_INPUT_BYTES:
            raise StorageError(
                "Portfolio notification-outbox input exceeds the byte scan limit"
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
    return value


def collect_notification_events(
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
                    "Portfolio notification-outbox query returned no row count"
                )
            if int(count_row[0]) > MAX_INPUT_ROWS:
                raise StorageError(
                    "Portfolio notification-outbox input exceeds the row scan limit"
                )
            frame = connection.execute(
                f"SELECT {selected} FROM {relation} "
                "ORDER BY ts_event, metric_name, transition_type, event_id"
            ).df()
    except StorageError:
        raise
    except Exception:
        raise StorageError(
            "Unable to read portfolio notification-outbox Parquet"
        ) from None

    records = [
        {key: _normalise_value(value) for key, value in row.items()}
        for row in frame.to_dict("records")
    ]
    for record in records:
        payload = record["payload_json"]
        if not isinstance(payload, str):
            raise StorageError(
                "Portfolio notification-outbox payload must be JSON text"
            )
        try:
            parsed = json.loads(payload)
        except (TypeError, ValueError):
            raise StorageError(
                "Portfolio notification-outbox payload is invalid JSON"
            ) from None
        if not isinstance(parsed, dict):
            raise StorageError(
                "Portfolio notification-outbox payload must be a JSON object"
            )
    return records


def build_insert_sql(schema_name: str = "risk_platform") -> str:
    columns = ", ".join(_quote_identifier(column) for column in COLUMNS)
    placeholders = ", ".join(["%s"] * len(COLUMNS))
    return (
        f"INSERT INTO {_quote_identifier(schema_name)}."
        f"{_quote_identifier(TABLE_NAME)} ({columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({_quote_identifier('event_id')}) DO NOTHING"
    )


def load_notification_events_to_postgres(
    *,
    dsn: str,
    storage_config_path: Path = Path("config/storage.yaml"),
    schema_name: str = "risk_platform",
    dry_run: bool = False,
) -> int:
    records = collect_notification_events(storage_config_path)
    if dry_run or not records:
        return len(records)
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL loading requires psycopg. Run `make setup` first."
        ) from exc

    rows = []
    for record in records:
        values = []
        for column in COLUMNS:
            value = record[column]
            if column == "payload_json":
                value = Jsonb(json.loads(value))
            values.append(value)
        rows.append(tuple(values))

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(build_insert_sql(schema_name), rows)
        connection.commit()
    return len(records)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load local portfolio notification-outbox Parquet into PostgreSQL "
            "without delivering messages."
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
    count = load_notification_events_to_postgres(
        dsn=args.dsn,
        storage_config_path=args.storage_config,
        schema_name=args.schema,
        dry_run=args.dry_run,
    )
    print(
        "Portfolio notification-outbox warehouse load summary at "
        f"{datetime.utcnow().isoformat()}Z"
    )
    print(f"{TABLE_NAME}: {count}")
    print("external_delivery_performed: false")


if __name__ == "__main__":
    main()
