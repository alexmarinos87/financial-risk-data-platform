from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.orchestration.run_portfolio_risk import run_portfolio_risk
from src.storage.s3_writer import write_records
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _daily_returns() -> list[dict]:
    records: list[dict] = []
    ingested_at = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
    for day, aapl, msft in [(2, 0.10, 0.02), (3, -0.04, 0.0), (4, 0.06, 0.02)]:
        for symbol, value in [("AAPL", aapl), ("MSFT", msft)]:
            records.append(
                {
                    "model_version": "daily-risk-v2",
                    "calculation_id": f"{symbol}-{day}",
                    "source": "alpha_vantage",
                    "symbol": symbol,
                    "source_event_id": f"{symbol}-event-{day}",
                    "previous_source_event_id": f"{symbol}-event-{day - 1}",
                    "ts_event": datetime(2026, 1, day, tzinfo=timezone.utc),
                    "ts_ingest": ingested_at + timedelta(minutes=day),
                    "return_1d": value,
                }
            )
    return records


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


def test_portfolio_risk_reads_daily_returns_and_replays_outputs(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    portfolio_config_path = _portfolio_config(tmp_path)
    assert (
        write_records(
            _daily_returns(),
            kind="curated",
            dataset="daily_returns",
            storage_config=storage_config,
        )
        == 6
    )

    first = run_portfolio_risk(
        portfolio_id="us-tech-equal",
        portfolio_config_path=portfolio_config_path,
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=storage_config_path,
    )
    second = run_portfolio_risk(
        portfolio_id="us-tech-equal",
        portfolio_config_path=portfolio_config_path,
        start_date=None,
        end_date=date(2026, 1, 4),
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
        storage_config_path=storage_config_path,
    )

    assert first["curated_output"]["portfolio_daily_returns"]["records_written"] == 3
    assert (
        first["curated_output"]["portfolio_daily_risk_summary"]["records_written"]
        == 3
    )
    assert second["curated_output"]["portfolio_daily_returns"]["records_written"] == 0
    assert (
        second["curated_output"]["portfolio_daily_risk_summary"]["records_written"]
        == 0
    )
    assert second["latest_metrics"]["calculation_id"] == first["latest_metrics"][
        "calculation_id"
    ]
