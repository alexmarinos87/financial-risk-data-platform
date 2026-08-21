from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.analytics.portfolio_attribution import build_portfolio_attribution
from src.analytics.portfolio_risk import (
    build_portfolio_risk_outputs,
    parse_portfolio_definition,
)
from src.common.exceptions import StorageError
from src.storage.s3_writer import write_records
from src.warehouse.portfolio_attribution_loader import (
    COLUMNS,
    DATASET_KEY,
    JSONB_COLUMNS,
    TABLE_NAME,
    _postgres_value,
    build_upsert_sql,
    collect_attribution_records,
    load_attribution_to_postgres,
)
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _definition():
    return parse_portfolio_definition(
        {
            "portfolios": {
                "us-tech-equal": {
                    "base_currency": "USD",
                    "constituents": [
                        {
                            "source": "alpha_vantage",
                            "symbol": "AAPL",
                            "weight": 0.5,
                        },
                        {
                            "source": "alpha_vantage",
                            "symbol": "MSFT",
                            "weight": 0.5,
                        },
                    ],
                }
            }
        },
        "us-tech-equal",
    )


def _component_returns() -> list[dict[str, object]]:
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    records: list[dict[str, object]] = []
    for day, aapl, msft in [
        (2, 0.10, 0.02),
        (3, -0.04, 0.00),
        (4, 0.06, 0.02),
        (5, 0.02, -0.01),
    ]:
        for symbol, value in (("AAPL", aapl), ("MSFT", msft)):
            records.append(
                {
                    "model_version": "daily-risk-v2",
                    "calculation_id": f"{symbol}-{day}",
                    "source": "alpha_vantage",
                    "symbol": symbol,
                    "source_event_id": f"{symbol}-event-{day}",
                    "ts_event": datetime(
                        2026,
                        1,
                        day,
                        tzinfo=timezone.utc,
                    ),
                    "ts_ingest": ingested_at + timedelta(minutes=day),
                    "return_1d": value,
                }
            )
    return records


def _snapshot() -> dict[str, object]:
    definition = _definition()
    portfolio = build_portfolio_risk_outputs(
        _component_returns(),
        definition=definition,
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
    )
    return build_portfolio_attribution(
        portfolio.returns,
        definition=definition,
        covariance_window=3,
    ).snapshot


def test_collect_attribution_records_preserves_json_evidence(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    snapshot = _snapshot()

    assert write_records(
        [snapshot],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=storage_config,
    ) == 1

    records = collect_attribution_records(storage_config_path)

    assert len(records) == 1
    record = records[0]
    assert tuple(record) == COLUMNS
    assert record["calculation_id"] == snapshot["calculation_id"]
    assert record["covariance_window"] == 3
    assert record["window_observations"] == 3
    assert record["portfolio_volatility_annualized"] > 0
    assert set(json.loads(record["weights_json"])) == {
        "alpha_vantage:AAPL",
        "alpha_vantage:MSFT",
    }
    covariance = json.loads(record["covariance_annualized_json"])
    assert set(covariance) == {
        "alpha_vantage:AAPL",
        "alpha_vantage:MSFT",
    }
    assert len(json.loads(record["input_calculation_ids_json"])) == 3


def test_attribution_loader_dry_run_is_safe_when_dataset_is_absent(
    tmp_path: Path,
) -> None:
    storage_config_path = write_storage_config(tmp_path)

    assert collect_attribution_records(storage_config_path) == []
    assert (
        load_attribution_to_postgres(
            dsn="postgresql://example",
            storage_config_path=storage_config_path,
            dry_run=True,
        )
        == 0
    )


def test_attribution_upsert_uses_calculation_id_and_jsonb_contract() -> None:
    statement = build_upsert_sql()

    assert TABLE_NAME == "portfolio_risk_attribution"
    assert 'INSERT INTO "risk_platform"."portfolio_risk_attribution"' in statement
    assert 'ON CONFLICT ("calculation_id") DO UPDATE' in statement
    assert (
        '"covariance_annualized_json" = EXCLUDED."covariance_annualized_json"'
        in statement
    )
    assert JSONB_COLUMNS == frozenset(
        {
            "weights_json",
            "input_calculation_ids_json",
            "covariance_annualized_json",
            "correlation_json",
            "constituent_volatility_annualized_json",
            "marginal_volatility_contribution_json",
            "component_volatility_contribution_json",
            "component_contribution_share_json",
        }
    )


def test_postgres_value_parses_standard_json_and_rejects_invalid_json() -> None:
    wrapped = _postgres_value(
        '{"alpha_vantage:AAPL":0.5}',
        jsonb=True,
        jsonb_wrapper=lambda value: value,
    )
    assert wrapped == {"alpha_vantage:AAPL": 0.5}

    with pytest.raises(StorageError, match="JSON evidence is invalid"):
        _postgres_value(
            "{not-json}",
            jsonb=True,
            jsonb_wrapper=lambda value: value,
        )


def test_attribution_loader_rejects_symbolic_link_dataset(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    curated_base = Path(storage_config["storage"]["curated"]["base_path"])
    curated_base.mkdir(parents=True)
    target = tmp_path / "target"
    target.mkdir()
    (curated_base / DATASET_KEY).symlink_to(target, target_is_directory=True)

    with pytest.raises(StorageError, match="symbolic link"):
        collect_attribution_records(storage_config_path)
