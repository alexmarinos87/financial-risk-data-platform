from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from uuid import uuid4

import duckdb

from ..common.exceptions import OverlapError
from ..storage.partitioning import partition_path
from ..storage.storage_config import load_storage_config
from .run_pipeline import run_pipeline

_HOURLY_WINDOW_ALIASES = {"hourly", "hour", "1h"}
_DEFAULT_THRESHOLDS_PATH = Path("config/risk_thresholds.yaml")
_DEFAULT_STORAGE_CONFIG_PATH = Path("config/storage.yaml")
_REPLAY_COLUMNS = ["event_id", "symbol", "price", "volume", "ts_event", "ts_ingest", "source"]
_RESUME_STATE_PATH = Path(".orchestration_state/backfill_resume.json")


def _normalize_window(window: str) -> str:
    normalized = window.strip().lower()
    if normalized in _HOURLY_WINDOW_ALIASES:
        return "hourly"
    raise ValueError("Unsupported window. Use 'hourly'.")


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid datetime '{value}'. Use ISO-8601 format.") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _window_delta(normalized_window: str) -> timedelta:
    if normalized_window == "hourly":
        return timedelta(hours=1)
    raise ValueError("Unsupported window. Use 'hourly'.")


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


def _load_resume_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Invalid backfill resume state in '{path}'")
    return payload


def _write_resume_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
    tmp_path.replace(path)


def _resume_from_last_success(
    *,
    current: datetime,
    normalized_window: str,
    step: timedelta,
    resume_state: dict[str, Any],
) -> datetime:
    checkpoint = resume_state.get(normalized_window)
    if not isinstance(checkpoint, dict):
        return current
    last_success_start = checkpoint.get("last_successful_window_start")
    if not isinstance(last_success_start, str):
        return current
    resumed = _parse_datetime(last_success_start) + step
    return resumed if resumed > current else current


def _record_last_success(
    *,
    resume_state_path: Path,
    resume_state: dict[str, Any],
    normalized_window: str,
    partition: str,
    window_start: datetime,
    run_id: str,
) -> None:
    resume_state[normalized_window] = {
        "last_successful_partition": partition,
        "last_successful_window_start": window_start.isoformat(),
        "run_id": run_id,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    _write_resume_state(resume_state_path, resume_state)


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
    resume: bool = True,
) -> list[dict[str, Any]]:
    start_ts = _parse_datetime(start_date)
    end_ts = _parse_datetime(end_date)
    if end_ts < start_ts:
        raise ValueError("end_date must be greater than or equal to start_date")

    normalized_window = _normalize_window(window)
    run_id = str(uuid4())
    run_started_at = datetime.now(timezone.utc)
    step = _window_delta(normalized_window)
    current = start_ts.replace(minute=0, second=0, microsecond=0)

    storage = load_storage_config(storage_config_path)["storage"]
    resume_state_path = Path(storage["base_dir"]) / _RESUME_STATE_PATH
    resume_state = _load_resume_state(resume_state_path)
    if resume:
        current = _resume_from_last_success(
            current=current,
            normalized_window=normalized_window,
            step=step,
            resume_state=resume_state,
        )

    raw_dataset_dir = Path(storage["raw"]["base_path"]) / storage["raw"]["dataset"]

    replay_summaries: list[dict[str, Any]] = []
    with TemporaryDirectory(prefix="backfill_") as tmp_dir:
        while current <= end_ts:
            started_at = datetime.now(timezone.utc)
            partition = partition_path(current)
            records = _load_partition_records(raw_dataset_dir / partition)

            if not records:
                replay_summaries.append(
                    {
                        "run_id": run_id,
                        "partition": partition,
                        "window_start": current.isoformat(),
                        "window": normalized_window,
                        "status": "skipped_no_records",
                        "records_replayed": 0,
                        "raw_events": 0,
                        "curated_records": 0,
                        "started_at": started_at.isoformat(),
                        "ended_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                current += step
                continue

            input_path = Path(tmp_dir) / f"{current.strftime('%Y%m%dT%H%M%SZ')}.json"
            with input_path.open("w", encoding="utf-8") as handle:
                json.dump(records, handle)

            try:
                summary = run_pipeline(
                    input_path=input_path,
                    thresholds_path=thresholds_path,
                    late_seconds=late_seconds,
                    window_minutes=window_minutes,
                    vol_window=vol_window,
                    storage_config_path=storage_config_path,
                    lock_owner=f"backfill:{run_id}:{partition}",
                )
                replay_summaries.append(
                    {
                        "run_id": run_id,
                        "partition": partition,
                        "window_start": current.isoformat(),
                        "window": normalized_window,
                        "status": "success",
                        "records_replayed": len(records),
                        "started_at": started_at.isoformat(),
                        "ended_at": datetime.now(timezone.utc).isoformat(),
                        **summary,
                    }
                )
                _record_last_success(
                    resume_state_path=resume_state_path,
                    resume_state=resume_state,
                    normalized_window=normalized_window,
                    partition=partition,
                    window_start=current,
                    run_id=run_id,
                )
            except OverlapError as exc:
                replay_summaries.append(
                    {
                        "run_id": run_id,
                        "partition": partition,
                        "window_start": current.isoformat(),
                        "window": normalized_window,
                        "status": "blocked_overlap",
                        "records_replayed": 0,
                        "raw_events": 0,
                        "curated_records": 0,
                        "overlap_error": str(exc),
                        "started_at": started_at.isoformat(),
                        "ended_at": datetime.now(timezone.utc).isoformat(),
                    }
                )
                break

            current += step

    run_ended_at = datetime.now(timezone.utc).isoformat()
    for summary in replay_summaries:
        summary["run_started_at"] = run_started_at.isoformat()
        summary["run_ended_at"] = run_ended_at

    return replay_summaries
