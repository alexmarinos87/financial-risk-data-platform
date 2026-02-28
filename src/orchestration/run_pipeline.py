from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ..analytics.data_quality import late_rate
from ..analytics.risk_metrics import value_at_risk
from ..analytics.returns import compute_returns
from ..analytics.volatility import rolling_volatility
from ..common.config import load_yaml
from ..ingestion.schemas import MarketEvent
from ..processing.deduplicator import dedupe_events
from ..processing.normaliser import normalize_symbol
from ..processing.validator import require_fields
from ..processing.windowing import floor_time
from ..storage.partitioning import partition_path
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config
from .locks import acquire_partition_locks, release_partition_locks

REQUIRED_FIELDS = ["event_id", "symbol", "price", "volume", "ts_event", "ts_ingest", "source"]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the local end-to-end risk pipeline demo.")
    parser.add_argument(
        "--input",
        type=Path,
        help="Optional path to a JSON list of market events.",
    )
    parser.add_argument(
        "--thresholds",
        type=Path,
        default=Path("config/risk_thresholds.yaml"),
        help="Path to risk thresholds YAML.",
    )
    parser.add_argument(
        "--late-seconds",
        type=int,
        default=300,
        help="Seconds after which an event is considered late.",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=5,
        help="Window size in minutes for aggregation.",
    )
    parser.add_argument(
        "--vol-window",
        type=int,
        default=5,
        help="Rolling window length for volatility.",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        help="Optional path to write the pipeline run summary as JSON.",
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
        help="Path to storage configuration YAML.",
    )
    return parser.parse_args()


def _sample_events() -> list[dict[str, Any]]:
    base = datetime(2025, 1, 20, 10, 0, 0, tzinfo=timezone.utc)
    return [
        {
            "event_id": "evt-1",
            "symbol": "aapl",
            "price": 100.0,
            "volume": 10,
            "ts_event": base + timedelta(minutes=1),
            "ts_ingest": base + timedelta(minutes=1, seconds=3),
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "aapl",
            "price": 101.2,
            "volume": 12,
            "ts_event": base + timedelta(minutes=2),
            "ts_ingest": base + timedelta(minutes=2, seconds=2),
            "source": "stooq",
        },
        {
            "event_id": "evt-2",
            "symbol": "AAPL",
            "price": 101.2,
            "volume": 12,
            "ts_event": base + timedelta(minutes=2),
            "ts_ingest": base + timedelta(minutes=2, seconds=2),
            "source": "stooq",
        },
        {
            "event_id": "evt-3",
            "symbol": "msft",
            "price": 240.5,
            "volume": 9,
            "ts_event": base + timedelta(minutes=3),
            "ts_ingest": base + timedelta(minutes=10),
            "source": "stooq",
        },
        {
            "event_id": "evt-4",
            "symbol": "msft",
            "price": 241.0,
            "volume": 11,
            "ts_event": base + timedelta(minutes=4),
            "ts_ingest": base + timedelta(minutes=4, seconds=1),
            "source": "stooq",
        },
    ]


def _load_input(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return _sample_events()
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("Input JSON must be a list of event objects.")
    return data


def _validate_and_normalize(payload: dict[str, Any]) -> dict[str, Any]:
    require_fields(payload, REQUIRED_FIELDS)
    payload = dict(payload)
    payload["symbol"] = normalize_symbol(str(payload["symbol"]))
    event = MarketEvent.model_validate(payload)
    return event.model_dump()


def _evaluate_threshold(value: float, warn: float | None, critical: float | None) -> str:
    if critical is not None and value >= critical:
        return "critical"
    if warn is not None and value >= warn:
        return "warn"
    return "ok"


def _evaluate_max(value: float, max_value: float | None) -> str:
    if max_value is not None and value > max_value:
        return "critical"
    return "ok"


def _latest_metric_timestamp(events: list[dict[str, Any]]) -> datetime:
    if not events:
        return datetime.now(timezone.utc)
    return max(event["ts_ingest"] for event in events)


def run_pipeline(
    input_path: Path | None,
    thresholds_path: Path,
    late_seconds: int,
    window_minutes: int,
    vol_window: int,
    storage_config_path: Path,
    lock_owner: str | None = None,
) -> dict[str, Any]:
    raw_payloads = _load_input(input_path)
    storage_config = load_storage_config(storage_config_path)

    validated = [_validate_and_normalize(payload) for payload in raw_payloads]
    deduped = dedupe_events(validated, key="event_id")

    total_events = len(validated)
    duplicate_rate = 0.0 if total_events == 0 else 1 - (len(deduped) / total_events)
    late_count = sum(
        1
        for event in deduped
        if (event["ts_ingest"] - event["ts_event"]) > timedelta(seconds=late_seconds)
    )
    late_rate_value = late_rate(late_count, len(deduped))

    thresholds = load_yaml(thresholds_path)
    vol_thresholds = thresholds["thresholds"]["volatility_5m"]
    dq_thresholds = thresholds["thresholds"]["data_quality"]

    late_status = _evaluate_max(late_rate_value, dq_thresholds.get("max_late_rate"))
    duplicate_status = _evaluate_max(duplicate_rate, dq_thresholds.get("max_duplicate_rate"))

    returns_records: list[dict[str, Any]] = []
    volatility_records: list[dict[str, Any]] = []
    var_latest: dict[str, float] = {}
    volatility_latest: dict[str, float] = {}

    if deduped:
        df = pd.DataFrame(deduped).sort_values(["symbol", "ts_event"])
        df["window_start"] = df["ts_event"].map(lambda ts: floor_time(ts, window_minutes))
        for symbol, group in df.groupby("symbol", sort=True):
            returns_series = compute_returns(group["price"])
            if returns_series.empty:
                continue

            var_latest[symbol] = float(value_at_risk(returns_series))
            for idx, value in returns_series.items():
                row = group.loc[idx]
                returns_records.append(
                    {
                        "symbol": symbol,
                        "ts_event": row["ts_event"],
                        "window_start": row["window_start"],
                        "return_1m": float(value),
                        "ts_ingest": row["ts_ingest"],
                    }
                )

            vol_series = rolling_volatility(returns_series, vol_window)
            if vol_series.empty:
                continue

            volatility_latest[symbol] = float(vol_series.iloc[-1])
            for idx, value in vol_series.items():
                row = group.loc[idx]
                volatility_records.append(
                    {
                        "symbol": symbol,
                        "ts_event": row["ts_event"],
                        "window_start": row["window_start"],
                        "volatility_5m": float(value),
                        "ts_ingest": row["ts_ingest"],
                    }
                )

    volatility_status = {
        symbol: _evaluate_threshold(
            value,
            vol_thresholds.get("warn"),
            vol_thresholds.get("critical"),
        )
        for symbol, value in volatility_latest.items()
    }

    raw_dataset = storage_config["storage"]["raw"]["dataset"]
    metric_ts = _latest_metric_timestamp(deduped)
    data_quality_records = [
        {
            "total_events": total_events,
            "deduped_events": len(deduped),
            "duplicate_events": total_events - len(deduped),
            "late_events": late_count,
            "late_rate": late_rate_value,
            "duplicate_rate": duplicate_rate,
            "late_status": late_status,
            "duplicate_status": duplicate_status,
            "ts_ingest": metric_ts,
        }
    ]
    risk_symbols = sorted(set(var_latest) | set(volatility_latest))
    risk_summary_records = [
        {
            "symbol": symbol,
            "volatility_5m": volatility_latest.get(symbol),
            "value_at_risk_95": var_latest.get(symbol),
            "volatility_status": volatility_status.get(symbol, "no_data"),
            "late_rate": late_rate_value,
            "duplicate_rate": duplicate_rate,
            "late_status": late_status,
            "duplicate_status": duplicate_status,
            "ts_ingest": metric_ts,
        }
        for symbol in risk_symbols
    ]

    partitions = sorted({partition_path(event["ts_ingest"]) for event in deduped})
    lock_paths: list[Path] = []
    try:
        lock_paths = acquire_partition_locks(
            Path(storage_config["storage"]["base_dir"]),
            partitions,
            lock_owner or "live",
        )

        raw_written = write_records(
            deduped,
            kind="raw",
            dataset=raw_dataset,
            storage_config=storage_config,
        )
        curated_writes = {
            "returns_1m": write_records(
                returns_records,
                kind="curated",
                dataset="returns_1m",
                storage_config=storage_config,
            ),
            "volatility_5m": write_records(
                volatility_records,
                kind="curated",
                dataset="volatility_5m",
                storage_config=storage_config,
            ),
            "data_quality_metrics": write_records(
                data_quality_records,
                kind="curated",
                dataset="data_quality_metrics",
                storage_config=storage_config,
            ),
            "risk_summary": write_records(
                risk_summary_records,
                kind="curated",
                dataset="risk_summary",
                storage_config=storage_config,
            ),
        }
        curated_written = sum(curated_writes.values())
    finally:
        release_partition_locks(lock_paths)

    return {
        "raw_events": raw_written,
        "curated_records": curated_written,
        "curated_records_by_dataset": curated_writes,
        "partitions": partitions,
        "late_rate": late_rate_value,
        "duplicate_rate": duplicate_rate,
        "volatility_latest": volatility_latest,
        "value_at_risk": var_latest,
        "volatility_status": volatility_status,
        "late_status": late_status,
        "duplicate_status": duplicate_status,
    }


def main() -> None:
    args = _parse_args()
    summary = run_pipeline(
        input_path=args.input,
        thresholds_path=args.thresholds,
        late_seconds=args.late_seconds,
        window_minutes=args.window_minutes,
        vol_window=args.vol_window,
        storage_config_path=args.storage_config,
    )
    if args.summary_json is not None:
        with args.summary_json.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True, default=str)

    print("Pipeline run summary")
    print(f"Raw events written: {summary['raw_events']}")
    print(f"Curated records written: {summary['curated_records']}")
    print(f"Partitions: {summary['partitions']}")
    print(f"Late rate: {summary['late_rate']:.2%} (status: {summary['late_status']})")
    print(
        f"Duplicate rate: {summary['duplicate_rate']:.2%} "
        f"(status: {summary['duplicate_status']})"
    )
    print(f"Volatility latest: {summary['volatility_latest']}")
    print(f"Volatility status: {summary['volatility_status']}")
    print(f"Value-at-Risk (95%): {summary['value_at_risk']}")


if __name__ == "__main__":
    main()
