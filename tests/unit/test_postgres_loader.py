from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.analytics.portfolio_risk import (
    build_portfolio_risk_outputs,
    parse_portfolio_definition,
)
from src.ingestion.alpha_vantage_client import alpha_vantage_daily_event_id
from src.ingestion.schemas import MarketEvent
from src.orchestration.run_daily_risk import run_daily_risk
from src.orchestration.run_pipeline import run_pipeline
from src.storage.s3_writer import write_records
from src.warehouse.postgres_loader import (
    LOAD_SPECS,
    TableLoadSpec,
    build_upsert_sql,
    collect_load_batches,
    load_batches_to_postgres,
)
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _write_demo_input(tmp_path: Path) -> Path:
    source = Path("tests/fixtures/demo_events.json")
    target = tmp_path / "demo_events.json"
    with source.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    return target


def _daily_raw_events() -> list[dict[str, object]]:
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    return [
        MarketEvent(
            event_id=alpha_vantage_daily_event_id("IBM", date(2026, 1, day)),
            symbol="IBM",
            price=price,
            volume=1_000 + day,
            ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
            ts_ingest=ingested_at + timedelta(minutes=day),
            source="alpha_vantage",
        ).model_dump()
        for day, price in [(1, 100.0), (2, 101.0), (3, 99.0), (4, 102.0)]
    ]


def _portfolio_definition():
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


def _portfolio_component_returns() -> list[dict[str, object]]:
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    records: list[dict[str, object]] = []
    for day, aapl, msft in [
        (2, 0.10, 0.02),
        (3, -0.04, 0.00),
        (4, 0.06, 0.02),
    ]:
        for symbol, value in (("AAPL", aapl), ("MSFT", msft)):
            records.append(
                {
                    "model_version": "daily-risk-v2",
                    "calculation_id": f"{symbol}-{day}",
                    "source": "alpha_vantage",
                    "symbol": symbol,
                    "source_event_id": f"{symbol}-event-{day}",
                    "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
                    "ts_ingest": ingested_at + timedelta(minutes=day),
                    "return_1d": value,
                }
            )
    return records


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
        "daily_returns": 0,
        "daily_volatility": 0,
        "daily_risk_summary": 0,
        "portfolio_daily_returns": 0,
        "portfolio_daily_risk_summary": 0,
    }
    assert batches["risk_summary"][0]["external_signal_count"] == 0
    assert "latest_external_signal_name" in batches["risk_summary"][0]


def test_collect_load_batches_retains_daily_model_versions(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    assert write_records(
        _daily_raw_events(),
        kind="raw",
        dataset="market_events",
        storage_config=storage_config,
    ) == 4

    for volatility_window in (2, 3):
        run_daily_risk(
            symbol="IBM",
            start_date=None,
            end_date=date(2026, 1, 4),
            volatility_window=volatility_window,
            var_window=2,
            var_confidence=0.95,
            storage_config_path=storage_config_path,
        )

    batches = collect_load_batches(storage_config_path)

    assert len(batches["daily_returns"]) == 3
    assert len(batches["daily_volatility"]) == 3
    assert len(batches["daily_risk_summary"]) == 6

    latest_date_rows = [
        row
        for row in batches["daily_risk_summary"]
        if row["ts_event"].date() == date(2026, 1, 4)
    ]
    assert {row["volatility_window"] for row in latest_date_rows} == {2, 3}
    assert {row["annualization_days"] for row in latest_date_rows} == {252}
    assert len({row["calculation_id"] for row in latest_date_rows}) == 2


def test_collect_load_batches_preserves_portfolio_evidence(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    outputs = build_portfolio_risk_outputs(
        _portfolio_component_returns(),
        definition=_portfolio_definition(),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
    )

    assert write_records(
        list(outputs.returns),
        kind="curated",
        dataset="portfolio_daily_returns",
        storage_config=storage_config,
    ) == 3
    assert write_records(
        list(outputs.risk_summary),
        kind="curated",
        dataset="portfolio_daily_risk_summary",
        storage_config=storage_config,
    ) == 3

    batches = collect_load_batches(storage_config_path)

    assert len(batches["portfolio_daily_returns"]) == 3
    assert len(batches["portfolio_daily_risk_summary"]) == 3
    portfolio_return = batches["portfolio_daily_returns"][0]
    assert portfolio_return["weighting_method"] == "constant_weight_daily_rebalanced"
    assert json.loads(portfolio_return["weights_json"]) == {
        "alpha_vantage:AAPL": 0.5,
        "alpha_vantage:MSFT": 0.5,
    }
    assert set(json.loads(portfolio_return["component_calculation_ids_json"])) == {
        "alpha_vantage:AAPL",
        "alpha_vantage:MSFT",
    }


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
    assert counts["daily_returns"] == 0
    assert counts["daily_volatility"] == 0
    assert counts["daily_risk_summary"] == 0
    assert counts["portfolio_daily_returns"] == 0
    assert counts["portfolio_daily_risk_summary"] == 0


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


def test_daily_load_specs_use_calculation_id_as_the_version_key() -> None:
    daily_specs = {
        spec.table_name: spec
        for spec in LOAD_SPECS
        if spec.table_name
        in {"daily_returns", "daily_volatility", "daily_risk_summary"}
    }

    assert set(daily_specs) == {
        "daily_returns",
        "daily_volatility",
        "daily_risk_summary",
    }
    assert all(
        spec.conflict_columns == ("calculation_id",)
        for spec in daily_specs.values()
    )
    summary_spec = daily_specs["daily_risk_summary"]
    assert "volatility_window" in summary_spec.columns
    assert "annualization_days" in summary_spec.columns
    assert 'ON CONFLICT ("calculation_id") DO UPDATE' in build_upsert_sql(
        summary_spec
    )


def test_portfolio_load_specs_preserve_json_and_foreign_key_order() -> None:
    names = [spec.table_name for spec in LOAD_SPECS]
    portfolio_specs = {
        spec.table_name: spec
        for spec in LOAD_SPECS
        if spec.table_name
        in {"portfolio_daily_returns", "portfolio_daily_risk_summary"}
    }

    assert set(portfolio_specs) == {
        "portfolio_daily_returns",
        "portfolio_daily_risk_summary",
    }
    assert all(
        spec.conflict_columns == ("calculation_id",)
        for spec in portfolio_specs.values()
    )
    assert portfolio_specs["portfolio_daily_returns"].jsonb_columns == frozenset(
        {
            "weights_json",
            "component_calculation_ids_json",
            "component_returns_json",
            "contributions_json",
        }
    )
    assert portfolio_specs[
        "portfolio_daily_risk_summary"
    ].jsonb_columns == frozenset({"weights_json"})
    assert names.index("portfolio_daily_returns") < names.index(
        "portfolio_daily_risk_summary"
    )
    assert 'ON CONFLICT ("calculation_id") DO UPDATE' in build_upsert_sql(
        portfolio_specs["portfolio_daily_risk_summary"]
    )


def test_all_load_specs_have_update_columns() -> None:
    assert all(spec.update_columns for spec in LOAD_SPECS)
