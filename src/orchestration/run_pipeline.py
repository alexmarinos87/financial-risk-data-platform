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
from ..common.exceptions import DataQualityError
from ..ingestion.schemas import MarketEvent, load_market_event_contract
from ..processing.deduplicator import dedupe_events
from ..processing.normaliser import normalize_symbol
from ..processing.validator import require_fields
from ..processing.windowing import floor_time
from ..storage.partitioning import partition_path
from ..storage.s3_writer import write_records, write_run_metadata


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
        "--contracts",
        type=Path,
        default=Path("config/data_contracts.yaml"),
        help="Path to data contracts YAML.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_lake"),
        help="Root path for local bronze/silver/gold datasets.",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        help="Optional stable run id. Use this for idempotent reruns.",
    )
    parser.add_argument(
        "--allow-dq-breach",
        action="store_true",
        help="Continue and exit successfully even when DQ thresholds are breached.",
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


def _validate_and_normalize(
    payload: dict[str, Any],
    *,
    required_fields: list[str],
    contract_version: str,
) -> dict[str, Any]:
    require_fields(payload, required_fields)
    payload = dict(payload)
    payload["symbol"] = normalize_symbol(str(payload["symbol"]))
    payload["contract_version"] = contract_version
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


def _resolve_run_id(run_id: str | None) -> str:
    if run_id:
        return run_id
    return datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ")


def _build_risk_summary_records(
    *,
    symbols: list[str],
    volatility_latest: dict[str, float],
    var_latest: dict[str, float],
    volatility_status: dict[str, str],
    run_id: str,
    ts_run: datetime,
) -> list[dict[str, Any]]:
    return [
        {
            "symbol": symbol,
            "volatility_5m": volatility_latest.get(symbol),
            "value_at_risk_95": var_latest.get(symbol),
            "volatility_status": volatility_status.get(symbol, "ok"),
            "run_id": run_id,
            "ts_run": ts_run,
        }
        for symbol in symbols
    ]


def run_pipeline(
    input_path: Path | None,
    thresholds_path: Path,
    late_seconds: int,
    window_minutes: int,
    vol_window: int,
    output_root: Path = Path("data_lake"),
    contracts_path: Path = Path("config/data_contracts.yaml"),
    run_id: str | None = None,
    fail_on_dq: bool = True,
) -> dict[str, Any]:
    contract = load_market_event_contract(contracts_path)
    required_fields = [str(field) for field in contract["required_fields"]]
    contract_version = str(contract["version"])

    resolved_run_id = _resolve_run_id(run_id)
    ts_run = datetime.now(timezone.utc)

    raw_payloads = _load_input(input_path)
    validated = [
        _validate_and_normalize(
            payload,
            required_fields=required_fields,
            contract_version=contract_version,
        )
        for payload in raw_payloads
    ]
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

    volatility_latest: dict[str, float] = {}
    var_latest: dict[str, float] = {}

    if deduped:
        df = pd.DataFrame(deduped).sort_values(["symbol", "ts_event"])
        for symbol, symbol_df in df.groupby("symbol"):
            symbol_returns = compute_returns(symbol_df["price"])
            if symbol_returns.empty:
                continue
            var_latest[symbol] = value_at_risk(symbol_returns)
            symbol_vol = rolling_volatility(symbol_returns, vol_window)
            if not symbol_vol.empty:
                volatility_latest[symbol] = float(symbol_vol.iloc[-1])

    thresholds = load_yaml(thresholds_path)
    vol_thresholds = thresholds["thresholds"]["volatility_5m"]
    dq_thresholds = thresholds["thresholds"]["data_quality"]

    volatility_status = {
        symbol: _evaluate_threshold(
            value,
            vol_thresholds.get("warn"),
            vol_thresholds.get("critical"),
        )
        for symbol, value in volatility_latest.items()
    }
    late_status = _evaluate_max(late_rate_value, dq_thresholds.get("max_late_rate"))
    duplicate_status = _evaluate_max(duplicate_rate, dq_thresholds.get("max_duplicate_rate"))

    dq_failures: list[str] = []
    if late_status != "ok":
        dq_failures.append(
            f"late_rate {late_rate_value:.4f} exceeded max_late_rate "
            f"{dq_thresholds.get('max_late_rate')}"
        )
    if duplicate_status != "ok":
        dq_failures.append(
            f"duplicate_rate {duplicate_rate:.4f} exceeded max_duplicate_rate "
            f"{dq_thresholds.get('max_duplicate_rate')}"
        )

    bronze_written = write_records(
        validated,
        layer="bronze",
        dataset="market_events",
        root_path=output_root,
        partition_field="ts_ingest",
        run_id=resolved_run_id,
        contract_version=contract_version,
    )
    silver_written = write_records(
        deduped,
        layer="silver",
        dataset="market_events",
        root_path=output_root,
        partition_field="ts_ingest",
        run_id=resolved_run_id,
        contract_version=contract_version,
    )

    symbols = sorted(set(volatility_latest.keys()) | set(var_latest.keys()))
    risk_summary_records = _build_risk_summary_records(
        symbols=symbols,
        volatility_latest=volatility_latest,
        var_latest=var_latest,
        volatility_status=volatility_status,
        run_id=resolved_run_id,
        ts_run=ts_run,
    )
    data_quality_records = [
        {
            "run_id": resolved_run_id,
            "ts_run": ts_run,
            "events_total": total_events,
            "events_deduped": len(deduped),
            "late_count": late_count,
            "late_rate": late_rate_value,
            "duplicate_rate": duplicate_rate,
            "late_status": late_status,
            "duplicate_status": duplicate_status,
            "contract_version": contract_version,
        }
    ]

    gold_risk_written = write_records(
        risk_summary_records,
        layer="gold",
        dataset="risk_summary",
        root_path=output_root,
        partition_field="ts_run",
        run_id=resolved_run_id,
        contract_version=contract_version,
    )
    gold_dq_written = write_records(
        data_quality_records,
        layer="gold",
        dataset="data_quality_metrics",
        root_path=output_root,
        partition_field="ts_run",
        run_id=resolved_run_id,
        contract_version=contract_version,
    )

    partitions = sorted({partition_path(event["ts_ingest"]) for event in deduped})
    run_metadata_path = output_root / "_runs" / f"{resolved_run_id}.json"

    summary = {
        "run_id": resolved_run_id,
        "contract_version": contract_version,
        "raw_events": bronze_written,
        "curated_records": gold_risk_written + gold_dq_written,
        "records_written": {
            "bronze_market_events": bronze_written,
            "silver_market_events": silver_written,
            "gold_risk_summary": gold_risk_written,
            "gold_data_quality_metrics": gold_dq_written,
        },
        "partitions": partitions,
        "late_rate": late_rate_value,
        "duplicate_rate": duplicate_rate,
        "volatility_latest": volatility_latest,
        "value_at_risk": var_latest,
        "volatility_status": volatility_status,
        "late_status": late_status,
        "duplicate_status": duplicate_status,
        "dq_passed": len(dq_failures) == 0,
        "dq_failures": dq_failures,
        "output_root": str(output_root),
        "run_metadata_path": str(run_metadata_path),
    }

    write_run_metadata(summary, root_path=output_root, run_id=resolved_run_id)

    if fail_on_dq and dq_failures:
        failure_message = "; ".join(dq_failures)
        raise DataQualityError(f"DQ checks failed for run_id={resolved_run_id}: {failure_message}")

    return summary


def main() -> None:
    args = _parse_args()
    summary = run_pipeline(
        input_path=args.input,
        thresholds_path=args.thresholds,
        contracts_path=args.contracts,
        output_root=args.output_root,
        run_id=args.run_id,
        fail_on_dq=not args.allow_dq_breach,
        late_seconds=args.late_seconds,
        window_minutes=args.window_minutes,
        vol_window=args.vol_window,
    )
    if args.summary_json is not None:
        with args.summary_json.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True, default=str)

    print("Pipeline run summary")
    print(f"Run ID: {summary['run_id']}")
    print(f"Contract version: {summary['contract_version']}")
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
    print(f"DQ passed: {summary['dq_passed']}")
    print(f"Run metadata: {summary['run_metadata_path']}")


if __name__ == "__main__":
    main()
