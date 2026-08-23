from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from src.common.exceptions import StorageError
from src.storage.s3_writer import write_records
from src.warehouse.portfolio_risk_limit_decisions_loader import (
    DATASET_KEY,
    build_upsert_sql,
    collect_decision_records,
)


def _storage(tmp_path: Path) -> tuple[dict, Path]:
    config = {
        "storage": {
            "base_dir": str(tmp_path),
            "raw": {
                "base_path": str(tmp_path / "raw"),
                "dataset": "market_events",
            },
            "curated": {
                "base_path": str(tmp_path / "curated"),
                "datasets": {DATASET_KEY: DATASET_KEY},
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }
    path = tmp_path / "storage.yaml"
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    return config, path


def _decision(**overrides: object) -> dict[str, object]:
    decided_at = datetime(2026, 8, 22, 14, tzinfo=timezone.utc)
    record: dict[str, object] = {
        "decision_id": "decision-1",
        "model_version": "portfolio-risk-limit-decisions-v1",
        "notification_id": "notification-1",
        "decision": "acknowledged",
        "actor": "alex@example.com",
        "reason": "Investigating the breach",
        "decided_at": decided_at,
        "ts_ingest": decided_at + timedelta(minutes=1),
        "metric_name": "portfolio_volatility_annualized",
        "status": "critical",
    }
    record.update(overrides)
    return record


def test_collect_decisions_returns_zero_when_dataset_is_absent(
    tmp_path: Path,
) -> None:
    _, path = _storage(tmp_path)

    assert collect_decision_records(path) == []


def test_collect_decisions_retains_minimal_columns_and_full_json(
    tmp_path: Path,
) -> None:
    config, path = _storage(tmp_path)
    assert write_records(
        [_decision()],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=config,
    ) == 1

    records = collect_decision_records(path)

    assert len(records) == 1
    record = records[0]
    assert record["decision_id"] == "decision-1"
    assert record["notification_id"] == "notification-1"
    assert record["decision"] == "acknowledged"
    assert record["record_json"]["metric_name"] == (
        "portfolio_volatility_annualized"
    )
    assert record["record_json"]["decided_at"].endswith("+00:00")


def test_collect_decisions_accepts_calculation_id_as_legacy_identity(
    tmp_path: Path,
) -> None:
    config, path = _storage(tmp_path)
    decision = _decision()
    decision["calculation_id"] = decision.pop("decision_id")
    assert write_records(
        [decision],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=config,
    ) == 1

    assert collect_decision_records(path)[0]["decision_id"] == "decision-1"


def test_collect_decisions_rejects_invalid_contract(
    tmp_path: Path,
) -> None:
    config, path = _storage(tmp_path)
    invalid = _decision(decision="closed")
    assert write_records(
        [invalid],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=config,
    ) == 1

    with pytest.raises(StorageError, match="unsupported decision"):
        collect_decision_records(path)


def test_collect_decisions_rejects_symbolic_link_dataset(
    tmp_path: Path,
) -> None:
    _, path = _storage(tmp_path)
    target = tmp_path / "real"
    target.mkdir()
    dataset = tmp_path / "curated" / DATASET_KEY
    dataset.parent.mkdir(parents=True)
    try:
        dataset.symlink_to(target, target_is_directory=True)
    except OSError:
        pytest.skip("symbolic links are unavailable")

    with pytest.raises(StorageError, match="symbolic link"):
        collect_decision_records(path)


def test_upsert_uses_decision_identity_and_jsonb_record() -> None:
    sql = build_upsert_sql()

    assert 'INSERT INTO "risk_platform"."portfolio_risk_limit_decisions"' in sql
    assert 'ON CONFLICT ("decision_id")' in sql
    assert '"record_json" = EXCLUDED."record_json"' in sql
    assert "loaded_at = now()" in sql
