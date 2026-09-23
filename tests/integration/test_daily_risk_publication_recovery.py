"""Exercise rejection and retry through real raw/curated Parquet, not writer spies."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from src.analytics.daily_risk import build_daily_risk_outputs
from src.common.exceptions import StorageError, ValidationError
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.ingestion.schemas import MarketEvent
from src.orchestration.run_daily_risk import main, run_daily_risk
from src.storage import parquet_io
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config

VALID_PRICES = [100.0, 110.0, 99.0, 108.9]
EXPECTED_COUNTS = {"daily_returns": 3, "daily_volatility": 2, "daily_risk_summary": 3}
TIMESTAMP_COLUMNS = {"ts_event", "ts_ingest", "window_start", "window_end"}
UTC_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _events(prices: list[float], *, start_day: int = 1) -> list[dict[str, Any]]:
    return [
        MarketEvent(
            event_id=alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            symbol="IBM", source="alpha_vantage", price=price, volume=1_000,
            ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
            ts_ingest=datetime(2026, 2, 1, 12, day, tzinfo=timezone.utc),
        ).model_dump()
        for day, price in enumerate(prices, start_day)
    ]


def _run(config_path: Path, end_day: int = 4) -> dict[str, Any]:
    return run_daily_risk(
        symbol="IBM", start_date=None, end_date=date(2026, 1, end_day),
        volatility_window=2, var_window=2, var_confidence=0.95,
        storage_config_path=config_path,
    )


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(path.relative_to(root)): path.read_bytes()
            for path in sorted(root.rglob("*")) if path.is_file()}


def _rows(root: Path, dataset: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with duckdb.connect() as connection:
        for path in sorted((root / "curated" / dataset).rglob("*.parquet")):
            cursor = connection.execute(
                "SELECT * FROM read_parquet(?, hive_partitioning=false) LIMIT 0", [str(path)],
            )
            columns = [description[0] for description in cursor.description]
            # Match the reader's UTC-microsecond boundary; fetching native
            # TIMESTAMPTZ objects would require an optional pytz dependency.
            identifiers = {column: '"' + column.replace('"', '""') + '"' for column in columns}
            projection = ", ".join(
                f"epoch_us({identifier}) AS {identifier}" if column in TIMESTAMP_COLUMNS
                else identifier for column, identifier in identifiers.items()
            )
            cursor = connection.execute(
                f"SELECT {projection} FROM read_parquet(?, hive_partitioning=false)", [str(path)],
            )
            for values in cursor.fetchall():
                record = dict(zip(columns, values, strict=True))
                for column in TIMESTAMP_COLUMNS.intersection(record):
                    record[column] = UTC_EPOCH + timedelta(microseconds=record[column])
                rows.append(record)
    return sorted(rows, key=lambda row: row["calculation_id"])


@pytest.mark.parametrize("prices,error", [
    ([1e-308, 1e308], "Daily returns"),
    ([1.0, 1e200, 1.0], "Daily volatility"),
])
@pytest.mark.parametrize("existing_output", [False, True])
def test_numerical_rejection_preserves_real_raw_curated_and_summary_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], prices: list[float],
    error: str, existing_output: bool,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    prefix = VALID_PRICES if existing_output else []
    if prefix:
        assert write_records(_events(prefix), kind="raw", storage_config=config) == len(prefix)
        _run(config_path)
        assert all(len(_rows(tmp_path, name)) == count for name, count in EXPECTED_COUNTS.items())
    assert write_records(
        _events(prices, start_day=len(prefix) + 1), kind="raw", storage_config=config,
    ) == len(prices)
    raw_before = _snapshot(tmp_path / "raw")
    curated_before = _snapshot(tmp_path / "curated")
    assert raw_before
    assert bool(curated_before) is existing_output
    summary_path = tmp_path / "summary.json"
    summary_path.write_bytes(b'{"last_success": "preserve"}\n')
    summary_before = summary_path.read_bytes()
    end_day = len(prefix) + len(prices)

    with pytest.raises(ValidationError, match=error):
        _run(config_path, end_day)
    assert main([
        "--symbol", "IBM", "--end-date", date(2026, 1, end_day).isoformat(),
        "--vol-window", "2", "--var-window", "2", "--storage-config", str(config_path),
        "--summary-json", str(summary_path),
    ]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "raw daily data or options were invalid" in captured.err
    assert _snapshot(tmp_path / "raw") == raw_before
    assert _snapshot(tmp_path / "curated") == curated_before
    assert summary_path.read_bytes() == summary_before

    if existing_output:
        # A bounded replay can select the earlier valid range without deleting
        # invalid later raw events or silently repairing the source history.
        replay = _run(config_path)
        assert all(item["records_written"] == 0 for item in replay["curated_output"].values())
        assert _snapshot(tmp_path / "raw") == raw_before
        assert _snapshot(tmp_path / "curated") == curated_before


@pytest.mark.parametrize("failure_after", [1, 4, 8])
def test_retry_converges_after_directory_sync_failure_following_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_after: int,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    events = _events(VALID_PRICES)
    assert write_records(events, kind="raw", storage_config=config) == len(events)
    raw_before = _snapshot(tmp_path / "raw")
    sync_attempts: list[Path] = []
    original_sync = parquet_io._fsync_directory

    def fail_directory_sync(path: Path) -> None:
        assert path.is_relative_to(tmp_path / "curated")
        sync_attempts.append(path)
        # create_parquet_file has already linked the valid final file. Only
        # its directory-durability confirmation is failed, not serialization.
        if len(sync_attempts) == failure_after:
            raise OSError("synthetic directory sync failure")
        original_sync(path)

    with monkeypatch.context() as patch:
        patch.setattr(parquet_io, "_fsync_directory", fail_directory_sync)
        with pytest.raises(StorageError, match="Daily curated publication failed; rerun is safe"):
            _run(config_path)
    assert len(sync_attempts) == failure_after
    partial = _snapshot(tmp_path / "curated")
    assert len(partial) == failure_after
    assert all(Path(name).suffix == ".parquet" for name in partial)
    before_counts = {name: len(_rows(tmp_path, name)) for name in EXPECTED_COUNTS}
    assert sum(before_counts.values()) == failure_after
    assert _snapshot(tmp_path / "raw") == raw_before

    recovered = _run(config_path)
    for dataset, selected in EXPECTED_COUNTS.items():
        assert recovered["curated_output"][dataset] == {
            "records_selected": selected,
            "records_written": selected - before_counts[dataset],
            "records_already_present": before_counts[dataset],
        }
    final = _snapshot(tmp_path / "curated")
    assert len(final) == sum(EXPECTED_COUNTS.values())
    assert all(final[name] == contents for name, contents in partial.items())
    expected = build_daily_risk_outputs(events, volatility_window=2, var_window=2)
    expected_rows = {
        "daily_returns": expected.returns, "daily_volatility": expected.volatility,
        "daily_risk_summary": expected.risk_summary,
    }
    for dataset, records in expected_rows.items():
        stored = _rows(tmp_path, dataset)
        assert stored == sorted(records, key=lambda row: row["calculation_id"])
        assert len({row["calculation_id"] for row in stored}) == len(records)

    replay = _run(config_path)
    assert all(item["records_written"] == 0 for item in replay["curated_output"].values())
    assert replay["latest_metrics"] == recovered["latest_metrics"]
    assert _snapshot(tmp_path / "curated") == final
    assert _snapshot(tmp_path / "raw") == raw_before


@pytest.mark.parametrize("obstruction", ["parent-file", "destination-directory"])
@pytest.mark.parametrize("existing_output", [False, True])
def test_real_cli_recovers_when_only_summary_publication_fails(
    tmp_path: Path, obstruction: str, existing_output: bool,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    events = _events(VALID_PRICES)
    assert write_records(events, kind="raw", storage_config=config) == len(events)
    if existing_output:
        _run(config_path)
    raw_before = _snapshot(tmp_path / "raw")
    curated_before = _snapshot(tmp_path / "curated")
    assert bool(curated_before) is existing_output
    summary_path = tmp_path / "reports" / "summary.json"
    if obstruction == "parent-file":
        marker = summary_path.parent
        marker.write_bytes(b"preserve-test-obstruction")
    else:
        summary_path.mkdir(parents=True)
        marker = summary_path / "keep.txt"
        marker.write_bytes(b"preserve-test-obstruction")

    def cli() -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "src.orchestration.run_daily_risk",
             "--symbol", "IBM", "--end-date", "2026-01-04",
             "--vol-window", "2", "--var-window", "2",
             "--storage-config", str(config_path), "--summary-json", str(summary_path)],
            cwd=Path(__file__).resolve().parents[2], capture_output=True,
            text=True, check=False, timeout=30,
        )

    failed = cli()
    assert failed.returncode == 1
    assert failed.stdout == ""
    assert failed.stderr.strip() == (
        "Daily risk pipeline failed: local storage operation failed; rerun is safe"
    )
    assert marker.read_bytes() == b"preserve-test-obstruction"
    assert not list(tmp_path.rglob(".daily-risk-summary-*"))
    assert _snapshot(tmp_path / "raw") == raw_before
    published = _snapshot(tmp_path / "curated")
    assert len(published) == sum(EXPECTED_COUNTS.values())
    if existing_output:
        assert published == curated_before
    expected = build_daily_risk_outputs(events, volatility_window=2, var_window=2)
    for dataset, records in {
        "daily_returns": expected.returns, "daily_volatility": expected.volatility,
        "daily_risk_summary": expected.risk_summary,
    }.items():
        stored = _rows(tmp_path, dataset)
        assert stored == sorted(records, key=lambda row: row["calculation_id"])
        assert len({row["calculation_id"] for row in stored}) == len(records)

    # Remove only the explicit obstruction created by this test, never output data.
    marker.unlink()
    if obstruction == "destination-directory":
        summary_path.rmdir()
    previous_metrics = None
    for _ in range(2):
        recovered = cli()
        assert recovered.returncode == 0, recovered.stderr
        assert recovered.stderr == ""
        summary = json.loads(recovered.stdout)
        assert summary_path.read_text(encoding="utf-8") == (
            json.dumps(summary, indent=2, sort_keys=True) + "\n"
        )
        assert summary["curated_output"] == {
            dataset: {"records_selected": count, "records_written": 0,
                      "records_already_present": count}
            for dataset, count in EXPECTED_COUNTS.items()
        }
        if previous_metrics is not None:
            assert summary["latest_metrics"] == previous_metrics
        previous_metrics = summary["latest_metrics"]
        assert _snapshot(tmp_path / "curated") == published
        assert _snapshot(tmp_path / "raw") == raw_before
        assert not list(tmp_path.rglob(".daily-risk-summary-*"))
