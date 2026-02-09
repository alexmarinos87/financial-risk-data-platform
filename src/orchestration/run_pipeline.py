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


def run_pipeline(
    input_path: Path | None,
    thresholds_path: Path,
    late_seconds: int,
    window_minutes: int,
    vol_window: int,
) -> dict[str, Any]:
    raw_payloads = _load_input(input_path)

    validated = [_validate_and_normalize(payload) for payload in raw_payloads]
    deduped = dedupe_events(validated, key="event_id")

    for event in deduped:
        event["window_start"] = floor_time(event["ts_event"], window_minutes)

    total_events = len(validated)
    duplicate_rate = 0.0 if total_events == 0 else 1 - (len(deduped) / total_events)
    late_count = sum(
        1
        for event in deduped
        if (event["ts_ingest"] - event["ts_event"]) > timedelta(seconds=late_seconds)
    )
    late_rate_value = late_rate(late_count, len(deduped))

    df = pd.DataFrame(deduped).sort_values(["symbol", "ts_event"])
    returns = df.groupby("symbol")["price"].apply(compute_returns)
    vol = returns.groupby("symbol").apply(lambda series: rolling_volatility(series, vol_window))

    volatility_latest: dict[str, float] = (
        vol.groupby("symbol").tail(1).droplevel(0).to_dict() if not vol.empty else {}
    )
    var_latest: dict[str, float] = (
        returns.groupby("symbol").apply(value_at_risk).to_dict() if not returns.empty else {}
    )

    thresholds = load_yaml(thresholds_path)
    vol_thresholds = thresholds["thresholds"]["volatility_5m"]
    dq_thresholds = thresholds["thresholds"]["data_quality"]

    volatility_status = {
        symbol: _evaluate_threshold(value, vol_thresholds.get("warn"), vol_thresholds.get("critical"))
        for symbol, value in volatility_latest.items()
    }
    late_status = _evaluate_max(late_rate_value, dq_thresholds.get("max_late_rate"))
    duplicate_status = _evaluate_max(duplicate_rate, dq_thresholds.get("max_duplicate_rate"))

    raw_written = write_records(deduped)
    curated_written = write_records(
        [
            {
                "late_rate": late_rate_value,
                "duplicate_rate": duplicate_rate,
                "volatility_latest": volatility_latest,
                "value_at_risk": var_latest,
            }
        ]
    )

    partitions = sorted({partition_path(event["ts_ingest"]) for event in deduped})

    return {
        "raw_events": raw_written,
        "curated_records": curated_written,
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
    )
    if args.summary_json is not None:
        with args.summary_json.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True, default=str)

    print("Pipeline run summary")
    print(f"Raw events written: {summary['raw_events']}")
    print(f"Curated records written: {summary['curated_records']}")
    print(f"Partitions: {summary['partitions']}")
    print(f"Late rate: {summary['late_rate']:.2%} (status: {summary['late_status']})")
    print(f"Duplicate rate: {summary['duplicate_rate']:.2%} (status: {summary['duplicate_status']})")
    print(f"Volatility latest: {summary['volatility_latest']}")
    print(f"Volatility status: {summary['volatility_status']}")
    print(f"Value-at-Risk (95%): {summary['value_at_risk']}")


if __name__ == "__main__":
    main()
