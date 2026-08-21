from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.analytics.portfolio_attribution import build_portfolio_attribution
from src.analytics.portfolio_risk import WEIGHTING_METHOD, load_portfolio_definition
from src.orchestration.run_portfolio_risk_limits import run_portfolio_risk_limits
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


def _limits_config(tmp_path: Path) -> Path:
    path = tmp_path / "limits.yaml"
    path.write_text(
        """
policies:
  test-policy:
    portfolio_id: us-tech-equal
    covariance_window: 3
    annualization_days: 252
    limits:
      portfolio_volatility_annualized:
        warning: 0.30
        critical: 0.45
      largest_absolute_component_contribution_share:
        warning: 0.65
        critical: 0.80
""".lstrip(),
        encoding="utf-8",
    )
    return path


def _portfolio_returns(fingerprint: str) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    ingested = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    for day, aapl, msft in [(2, 0.10, 0.02), (3, -0.04, 0.0), (4, 0.06, 0.02)]:
        event = datetime(2026, 1, day, tzinfo=timezone.utc)
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
                "definition_fingerprint": fingerprint,
                "weighting_method": WEIGHTING_METHOD,
                "ts_event": event,
                "ts_ingest": ingested + timedelta(minutes=day),
                "constituent_count": 2,
                "weights_json": json.dumps(
                    {"alpha_vantage:AAPL": 0.5, "alpha_vantage:MSFT": 0.5}
                ),
                "component_calculation_ids_json": json.dumps(
                    {
                        "alpha_vantage:AAPL": f"AAPL-{day}",
                        "alpha_vantage:MSFT": f"MSFT-{day}",
                    }
                ),
                "component_returns_json": json.dumps(component_returns),
                "portfolio_return_1d": 0.5 * aapl + 0.5 * msft,
            }
        )
    return records


def test_portfolio_risk_limits_read_attribution_and_replay(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    portfolio_config = _portfolio_config(tmp_path)
    limits_config = _limits_config(tmp_path)
    definition = load_portfolio_definition(portfolio_config, "us-tech-equal")
    attribution = build_portfolio_attribution(
        _portfolio_returns(definition.fingerprint),
        definition=definition,
        covariance_window=3,
        end_date=date(2026, 1, 4),
    )
    assert write_records(
        [attribution.snapshot],
        kind="curated",
        dataset="portfolio_risk_attribution",
        storage_config=storage_config,
    ) == 1

    first = run_portfolio_risk_limits(
        policy_id="test-policy",
        limits_config_path=limits_config,
        portfolio_config_path=portfolio_config,
        end_date=date(2026, 1, 4),
        storage_config_path=storage_config_path,
    )
    second = run_portfolio_risk_limits(
        policy_id="test-policy",
        limits_config_path=limits_config,
        portfolio_config_path=portfolio_config,
        end_date=date(2026, 1, 4),
        storage_config_path=storage_config_path,
    )

    first_output = first["curated_output"]["portfolio_risk_limit_evaluations"]
    second_output = second["curated_output"]["portfolio_risk_limit_evaluations"]
    assert first_output["records_selected"] == 2
    assert first_output["records_written"] == 2
    assert second_output["records_written"] == 0
    assert second_output["records_already_present"] == 2
    assert second["latest_status"] == first["latest_status"]
