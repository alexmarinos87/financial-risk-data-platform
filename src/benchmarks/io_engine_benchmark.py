from __future__ import annotations

import argparse
import json
import shutil
import statistics
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


DEFAULT_OUTPUT_DIR = Path(".benchmarks/io_engine")
DEFAULT_ROW_COUNT = 50_000
DEFAULT_SYMBOL_COUNT = 20
DEFAULT_DAY_COUNT = 10
DEFAULT_REPETITIONS = 3


@dataclass(frozen=True)
class ScanMetrics:
    storage_format: str
    path: str
    duration_ms: float
    bytes_on_disk: int
    file_count: int
    matched_rows: int
    average_price: float
    total_volume: int


@dataclass(frozen=True)
class BenchmarkSummary:
    row_count: int
    symbol_count: int
    day_count: int
    target_symbol: str
    target_trade_date: str
    repetitions: int
    csv: ScanMetrics
    partitioned_parquet: ScanMetrics
    parquet_speedup_ratio: float
    parquet_storage_reduction_ratio: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def generate_market_events(
    row_count: int,
    *,
    symbol_count: int = DEFAULT_SYMBOL_COUNT,
    day_count: int = DEFAULT_DAY_COUNT,
    seed: int = 42,
) -> pd.DataFrame:
    if row_count <= 0:
        raise ValueError("row_count must be greater than zero")
    if symbol_count <= 0:
        raise ValueError("symbol_count must be greater than zero")
    if day_count <= 0:
        raise ValueError("day_count must be greater than zero")

    rng = np.random.default_rng(seed)
    row_index = np.arange(row_count)
    symbols = np.array([f"SYM{symbol_number:03d}" for symbol_number in range(symbol_count)])
    symbol_values = symbols[row_index % symbol_count]
    day_offsets = (row_index // symbol_count) % day_count

    base_ts = pd.Timestamp("2025-01-20T09:30:00Z")
    ts_event = base_ts + pd.to_timedelta(day_offsets, unit="D") + pd.to_timedelta(
        row_index % 390,
        unit="m",
    )
    ts_ingest = ts_event + pd.to_timedelta(rng.integers(1, 20, size=row_count), unit="s")
    trade_date = pd.Series(ts_event).dt.strftime("%Y-%m-%d")

    price_base = 100 + (row_index % symbol_count) * 1.75
    price_noise = rng.normal(0, 0.4, size=row_count).round(4)
    volume = rng.integers(100, 50_000, size=row_count)

    return pd.DataFrame(
        {
            "event_id": [f"bench-{number:012d}" for number in row_index],
            "symbol": symbol_values,
            "price": np.round(price_base + price_noise, 4),
            "volume": volume.astype(int),
            "trade_date": trade_date,
            "ts_event": ts_event,
            "ts_ingest": ts_ingest,
            "source": "benchmark",
        }
    )


def write_benchmark_inputs(
    events: pd.DataFrame,
    output_dir: Path,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / "market_events.csv"
    parquet_dir = output_dir / "market_events_parquet"

    if csv_path.exists():
        csv_path.unlink()
    if parquet_dir.exists():
        shutil.rmtree(parquet_dir)

    events.to_csv(csv_path, index=False)
    with duckdb.connect() as conn:
        conn.register("events", events)
        escaped_path = str(parquet_dir).replace("'", "''")
        conn.execute(
            "COPY events TO "
            f"'{escaped_path}' "
            "(FORMAT PARQUET, PARTITION_BY (symbol, trade_date))"
        )

    return csv_path, parquet_dir


def run_io_benchmark(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    row_count: int = DEFAULT_ROW_COUNT,
    symbol_count: int = DEFAULT_SYMBOL_COUNT,
    day_count: int = DEFAULT_DAY_COUNT,
    target_symbol: str | None = None,
    target_trade_date: str | None = None,
    repetitions: int = DEFAULT_REPETITIONS,
) -> BenchmarkSummary:
    events = generate_market_events(
        row_count,
        symbol_count=symbol_count,
        day_count=day_count,
    )
    csv_path, parquet_dir = write_benchmark_inputs(events, output_dir)

    selected_symbol = target_symbol or "SYM000"
    selected_trade_date = target_trade_date or str(events.loc[0, "trade_date"])
    measured_repetitions = max(1, repetitions)

    with duckdb.connect() as conn:
        csv_metrics = _measure_csv_scan(
            conn,
            csv_path,
            selected_symbol,
            selected_trade_date,
            repetitions=measured_repetitions,
        )
        parquet_metrics = _measure_partitioned_parquet_scan(
            conn,
            parquet_dir,
            selected_symbol,
            selected_trade_date,
            repetitions=measured_repetitions,
        )

    speedup = _safe_ratio(csv_metrics.duration_ms, parquet_metrics.duration_ms)
    storage_reduction = 1 - _safe_ratio(
        parquet_metrics.bytes_on_disk,
        csv_metrics.bytes_on_disk,
    )

    return BenchmarkSummary(
        row_count=row_count,
        symbol_count=symbol_count,
        day_count=day_count,
        target_symbol=selected_symbol,
        target_trade_date=selected_trade_date,
        repetitions=measured_repetitions,
        csv=csv_metrics,
        partitioned_parquet=parquet_metrics,
        parquet_speedup_ratio=speedup,
        parquet_storage_reduction_ratio=storage_reduction,
    )


def _measure_csv_scan(
    conn: duckdb.DuckDBPyConnection,
    csv_path: Path,
    target_symbol: str,
    target_trade_date: str,
    *,
    repetitions: int,
) -> ScanMetrics:
    escaped_path = str(csv_path).replace("'", "''")
    sql = (
        "SELECT COUNT(*) AS matched_rows, AVG(price) AS average_price, "
        "SUM(volume) AS total_volume "
        f"FROM read_csv_auto('{escaped_path}') "
        f"WHERE symbol = {_sql_literal(target_symbol)} "
        f"AND trade_date = {_sql_literal(target_trade_date)}"
    )
    duration_ms, row = _measure_query(conn, sql, repetitions)
    return ScanMetrics(
        storage_format="csv",
        path=str(csv_path),
        duration_ms=duration_ms,
        bytes_on_disk=csv_path.stat().st_size,
        file_count=1,
        matched_rows=int(row[0]),
        average_price=float(row[1] or 0.0),
        total_volume=int(row[2] or 0),
    )


def _measure_partitioned_parquet_scan(
    conn: duckdb.DuckDBPyConnection,
    parquet_dir: Path,
    target_symbol: str,
    target_trade_date: str,
    *,
    repetitions: int,
) -> ScanMetrics:
    parquet_glob = str(parquet_dir / "**" / "*.parquet").replace("'", "''")
    sql = (
        "SELECT COUNT(*) AS matched_rows, AVG(price) AS average_price, "
        "SUM(volume) AS total_volume "
        f"FROM read_parquet('{parquet_glob}', hive_partitioning = true) "
        f"WHERE symbol = {_sql_literal(target_symbol)} "
        f"AND trade_date = {_sql_literal(target_trade_date)}"
    )
    duration_ms, row = _measure_query(conn, sql, repetitions)
    return ScanMetrics(
        storage_format="partitioned_parquet",
        path=str(parquet_dir),
        duration_ms=duration_ms,
        bytes_on_disk=_path_size(parquet_dir),
        file_count=_file_count(parquet_dir),
        matched_rows=int(row[0]),
        average_price=float(row[1] or 0.0),
        total_volume=int(row[2] or 0),
    )


def _measure_query(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    repetitions: int,
) -> tuple[float, tuple[Any, ...]]:
    durations: list[float] = []
    last_row: tuple[Any, ...] | None = None
    for _ in range(repetitions):
        started_at = time.perf_counter()
        last_row = conn.execute(sql).fetchone()
        durations.append((time.perf_counter() - started_at) * 1_000)

    if last_row is None:
        raise RuntimeError("benchmark query did not return a result")
    return statistics.median(durations), last_row


def _path_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(child.stat().st_size for child in path.rglob("*") if child.is_file())


def _file_count(path: Path) -> int:
    if path.is_file():
        return 1
    return sum(1 for child in path.rglob("*") if child.is_file())


def _safe_ratio(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Benchmark CSV scans against partitioned Parquet scans.",
    )
    parser.add_argument("--rows", type=int, default=DEFAULT_ROW_COUNT)
    parser.add_argument("--symbols", type=int, default=DEFAULT_SYMBOL_COUNT)
    parser.add_argument("--days", type=int, default=DEFAULT_DAY_COUNT)
    parser.add_argument("--target-symbol")
    parser.add_argument("--target-trade-date")
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--summary-json", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    summary = run_io_benchmark(
        output_dir=args.output_dir,
        row_count=args.rows,
        symbol_count=args.symbols,
        day_count=args.days,
        target_symbol=args.target_symbol,
        target_trade_date=args.target_trade_date,
        repetitions=args.repetitions,
    )
    payload = summary.to_dict()

    if args.summary_json is not None:
        args.summary_json.parent.mkdir(parents=True, exist_ok=True)
        with args.summary_json.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")

    print("I/O benchmark summary")
    print(f"Rows generated: {summary.row_count}")
    print(f"Predicate: symbol={summary.target_symbol}, trade_date={summary.target_trade_date}")
    print(f"CSV scan: {summary.csv.duration_ms:.2f} ms, {summary.csv.bytes_on_disk:,} bytes")
    print(
        "Partitioned Parquet scan: "
        f"{summary.partitioned_parquet.duration_ms:.2f} ms, "
        f"{summary.partitioned_parquet.bytes_on_disk:,} bytes"
    )
    print(f"Parquet speedup ratio: {summary.parquet_speedup_ratio:.2f}x")
    print(f"Parquet storage reduction: {summary.parquet_storage_reduction_ratio:.2%}")


if __name__ == "__main__":
    main()
