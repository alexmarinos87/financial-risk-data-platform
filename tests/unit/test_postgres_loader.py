from __future__ import annotations

import json
from pathlib import Path

from src.orchestration.run_pipeline import run_pipeline
from src.warehouse.postgres_loader import (
    LOAD_SPECS,
    TableLoadSpec,
    build_upsert_sql,
    collect_load_batches,
    load_batches_to_postgres,
)
from tests.storage_config_helpers import write_storage_config


def _write_demo_input(tmp_path: Path) -> Path:
    source = Path("tests/fixtures/demo_events.json")
    target = tmp_path / "demo_events.json"
    with source.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return target


def test_collect_load_batches_matches_demo_pipeline_outputs(tmp_path: Path) -> None:
    storage_config_path = write_storage_config(tmp_path)
    input_path = _write_demo_input(tmp_path)

    run_pipeline(
        input_path=input_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        late_seconds=60,
        window_minutes=5,
        vol_window=2,
        storage_config_path=storage_config_path,
    )

    batches = collect_load_batches(storage_config_path)

    assert {key: len(value) for key, value in batches.items()} == {
        "market_events_raw": 6,
        "returns_1m": 4,
        "volatility_5m": 2,
        "data_quality_metrics": 1,
        "risk_summary": 2,
        "external_signal_summary": 0,
    }
    assert batches["risk_summary"][0]["external_signal_count"] == 0
    assert "latest_external_signal_name" in batches["risk_summary"][0]


def test_load_batches_to_postgres_dry_run_returns_counts(tmp_path: Path) -> None:
    storage_config_path = write_storage_config(tmp_path)
    input_path = _write_demo_input(tmp_path)
    run_pipeline(
        input_path=input_path,
        thresholds_path=Path("config/risk_thresholds.yaml"),
        late_seconds=60,
        window_minutes=5,
        vol_window=2,
        storage_config_path=storage_config_path,
    )

    counts = load_batches_to_postgres(
        dsn="postgresql://example",
        storage_config_path=storage_config_path,
        dry_run=True,
    )

    assert counts["market_events_raw"] == 6
    assert counts["data_quality_metrics"] == 1


def test_build_upsert_sql_quotes_identifiers_and_conflict_key() -> None:
    statement = build_upsert_sql(
        TableLoadSpec(
            dataset_key="example",
            table_name="target_table",
            columns=("event_id", "value"),
            conflict_columns=("event_id",),
        )
    )

    assert 'INSERT INTO "risk_platform"."target_table"' in statement
    assert '("event_id", "value")' in statement
    assert 'ON CONFLICT ("event_id") DO UPDATE' in statement
    assert '"value" = EXCLUDED."value"' in statement


def test_all_load_specs_have_update_columns() -> None:
    assert all(spec.update_columns for spec in LOAD_SPECS)
