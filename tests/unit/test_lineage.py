from __future__ import annotations

from pathlib import Path

from src.orchestration.lineage import build_lineage_manifest
from tests.storage_config_helpers import build_storage_config


def test_build_lineage_manifest_tracks_source_to_reporting_flow(tmp_path: Path) -> None:
    manifest = build_lineage_manifest(
        run_id="run-1",
        input_path=Path("tests/fixtures/demo_events.json"),
        signals_path=None,
        storage_config=build_storage_config(tmp_path),
        raw_dataset="market_events",
        raw_payload_count=7,
        deduped_event_count=6,
        external_signal_count=0,
        raw_written=6,
        curated_writes={
            "returns_1m": 4,
            "volatility_5m": 2,
            "data_quality_metrics": 1,
            "risk_summary": 2,
            "external_signal_summary": 0,
        },
        partitions=["year=2025/month=01/day=20/hour=10"],
        quality_summary={
            "required_fields_status": "ok",
            "late_status": "critical",
            "duplicate_status": "critical",
        },
    )

    assert manifest["run_id"] == "run-1"
    assert manifest["source_inventory"][0] == {
        "name": "market_events",
        "source_type": "json_file",
        "location": "tests/fixtures/demo_events.json",
        "authoritative_key": "event_id",
        "records_received": 7,
        "records_after_deduplication": 6,
    }
    assert manifest["layers"][0]["datasets"][0]["records_written"] == 6
    assert manifest["layers"][2]["datasets"][0]["dataset"] == (
        "risk_platform.finance_risk_semantic_model"
    )
    assert manifest["transformations"][-1]["outputs"] == [
        "data_quality_metrics",
        "external_signal_summary",
        "returns_1m",
        "risk_summary",
        "volatility_5m",
    ]
    assert manifest["quality_checks"]["duplicate_status"] == "critical"
