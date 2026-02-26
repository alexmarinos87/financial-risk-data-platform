from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any

import duckdb

from ..storage.partitioning import partition_path
from ..storage.storage_config import load_storage_config
from .run_pipeline import run_pipeline

_HOURLY_WINDOW_ALIASES = {"hourly", "hour", "1h"}
_DEFAULT_THRESHOLDS_PATH = Path("config/risk_thresholds.yaml")
_DEFAULT_STORAGE_CONFIG_PATH = Path("config/storage.yaml")
_REPLAY_COLUMNS = ["event_id", "symbol", "price", "volume", "ts_event", "ts_ingest", "source"]


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid datetime '{value}'. Use ISO-8601 format.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _window_delta(window: str) -> timedelta:
    if window.strip().lower() not in _HOURLY_WINDOW_ALIASES:
        raise ValueError("Unsupported window. Use 'hourly'.")
    return timedelta(hours=1)


def _load_partition_records(partition_dir: Path) -> list[dict[str, Any]]:
    parquet_files = sorted(partition_dir.glob("*.parquet"))
    if not parquet_files:
        return []

    escaped_paths = [str(path).replace("'", "''") for path in parquet_files]
    quoted_paths = ", ".join(f"'{path}'" for path in escaped_paths)
    select_list = ", ".join(_REPLAY_COLUMNS)
    with duckdb.connect() as conn:
        df = conn.execute(
            "SELECT "
            f"{select_list} "
            f"FROM read_parquet([{quoted_paths}], union_by_name=true) "
            "ORDER BY ts_ingest, event_id"
        ).df()
    return json.loads(df.to_json(orient="records", date_format="iso"))


def run_backfill(
    start_date: str,
    end_date: str,
    window: str,
    *,
    thresholds_path: Path = _DEFAULT_THRESHOLDS_PATH,
    late_seconds: int = 300,
    window_minutes: int = 5,
    vol_window: int = 5,
    storage_config_path: Path = _DEFAULT_STORAGE_CONFIG_PATH,
) -> list[dict[str, Any]]:
    start_ts = _parse_datetime(start_date)
    end_ts = _parse_datetime(end_date)
    if end_ts < start_ts:
        raise ValueError("end_date must be greater than or equal to start_date")

    step = _window_delta(window)
    current = start_ts.replace(minute=0, second=0, microsecond=0)

    storage = load_storage_config(storage_config_path)["storage"]
    raw_dataset_dir = Path(storage["raw"]["base_path"]) / storage["raw"]["dataset"]

    replay_summaries: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix="backfill_") as tmp_dir:
        while current <= end_ts:
            partition = partition_path(current)
            records = _load_partition_records(raw_dataset_dir / partition)

            if records:
                input_path = Path(tmp_dir) / f"{current.strftime('%Y%m%dT%H%M%SZ')}.json"
                with input_path.open("w", encoding="utf-8") as handle:
                    json.dump(records, handle)

                summary = run_pipeline(
                    input_path=input_path,
                    thresholds_path=thresholds_path,
                    late_seconds=late_seconds,
                    window_minutes=window_minutes,
                    vol_window=vol_window,
                    storage_config_path=storage_config_path,
                )
                replay_summaries.append(
                    {
                        "partition": partition,
                        "window_start": current.isoformat(),
                        "window": window,
                        "records_replayed": len(records),
                        **summary,
                    }
                )

            current += step

    return replay_summaries
