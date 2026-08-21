from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.analytics.portfolio_risk import (
    WEIGHTING_METHOD,
    load_portfolio_definition,
)
from src.orchestration.run_portfolio_attribution_history import (
    run_portfolio_attribution_history,
)
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _portfolio_config(tmp_path: Path) -> Path:
    path = tmp_path / "portfolios.yaml"
    path.write_text(
        """
portfolios:
  us-tech-equal:
    base_currency: USD
    constituents:
      - source: alpha_vantage
        symbol: AAPL
        weight: 0.5
      - source: alpha_vantage
        symbol: MSFT
        weight: 0.5
""".lstrip(),
        encoding="utf-8",
    )
    return path


def _portfolio_returns(definition_fingerprint: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    for day, aapl, msft in [
        (2, 0.10, 0.02),
        (3, -0.04, 0.00),
        (4, 0.06, 0.02),
        (5, 0.01, -0.01),
        (6, -0.02, 0.03),
    ]:
        component_returns = {
            "alpha_vantage:AAPL": aapl,
            "alpha_vantage:MSFT": msft,
        }
        records.append(
            {
                "model_version": "portfolio-risk-v1",
                "calculation_id": f"portfolio-{day}",
                "portfolio_id": "us-tech-equal",
                "base_currency": "USD",
                "definition_fingerprint": definition_fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
                "ts_ingest": ingested_at + timedelta(minutes=day),
                "constituent_count": 2,
                "weights_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": 0.5,
                        "alpha_vantage:MSFT": 0.5,
                    },
                    sort_keys=True,
                ),
                "component_calculation_ids_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": f"AAPL-{day}",
                        "alpha_vantage:MSFT": f"MSFT-{day}",
                    },
                    sort_keys=True,
                ),
                "component_returns_json": json.dumps(
                    component_returns,
                    sort_keys=True,
                ),
                "contributions_json": json.dumps(
                    {
                        key: 0.5 * value
                        for key, value in component_returns.items()
                    },
                    sort_keys=True,
                ),
                "portfolio_return_1d": 0.5 * aapl + 0.5 * msft,
            }
        )
    return records


def test_portfolio_attribution_history_replays_rolling_snapshots(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    portfolio_config_path = _portfolio_config(tmp_path)
    definition = load_portfolio_definition(
        portfolio_config_path,
        "us-tech-equal",
    )
    assert (
        write_records(
            _portfolio_returns(definition.fingerprint),
            kind="curated",
            dataset="portfolio_daily_returns",
            storage_config=storage_config,
        )
        == 5
    )

    first = run_portfolio_attribution_history(
        portfolio_id="us-tech-equal",
        portfolio_config_path=portfolio_config_path,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 6),
        covariance_window=3,
        max_snapshots=10,
        storage_config_path=storage_config_path,
    )
    second = run_portfolio_attribution_history(
        portfolio_id="us-tech-equal",
        portfolio_config_path=portfolio_config_path,
        start_date=date(2026, 1, 5),
        end_date=date(2026, 1, 6),
        covariance_window=3,
        max_snapshots=10,
        storage_config_path=storage_config_path,
    )

    assert first["curated_output"]["portfolio_risk_attribution"] == {
        "records_selected": 2,
        "records_written": 2,
        "records_already_present": 0,
    }
    assert second["curated_output"]["portfolio_risk_attribution"] == {
        "records_selected": 2,
        "records_written": 0,
        "records_already_present": 2,
    }
    assert first["selection"]["first_snapshot_date"] == "2026-01-05"
    assert first["selection"]["last_snapshot_date"] == "2026-01-06"
    assert second["latest_metrics"]["calculation_id"] == first[
        "latest_metrics"
    ]["calculation_id"]
