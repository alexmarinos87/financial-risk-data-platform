from __future__ import annotations

import math
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from src.analytics import daily_risk
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent
from src.orchestration import run_daily_risk as runner
from test_daily_risk_finite_outputs import events
from test_run_daily_risk import _config


def run(source: list[MarketEvent], writes: list[dict[str, Any]]) -> dict[str, Any]:
    def writer(records: list[dict[str, Any]], **kwargs: Any) -> int:
        writes.extend(records)
        return len(records)

    return runner.run_daily_risk(
        symbol="IBM", start_date=None, end_date=date(2026, 1, 10),
        volatility_window=2, var_window=2, var_confidence=0.95,
        storage_config_path=Path("unused.yaml"), reader=lambda **kwargs: source,
        writer=writer, config_loader=lambda path: _config(),
    )


@pytest.mark.parametrize("prices", [
    [1e-308, 1e308], [1e-308, 1e308, 1e308],
    [1.0, 1e200, 1.0], [100, 101, 1e-308, 1e308],
])
def test_numeric_failure_precedes_every_curated_write(prices: list[float]) -> None:
    writes: list[dict[str, Any]] = []
    with pytest.raises(ValidationError):
        run(events(prices), writes)
    assert writes == []


def test_later_invalid_group_does_not_partially_publish_earlier_group() -> None:
    source = events([100, 101, 102], symbol="AAA") + events([1e-308, 1e308], symbol="ZZZ")
    writes: list[dict[str, Any]] = []
    with pytest.raises(ValidationError, match="Daily returns"):
        run(source, writes)
    assert writes == []


@pytest.mark.parametrize("quantile", [float("nan"), float("inf"), -float("inf")])
def test_bad_quantile_cannot_be_published_as_zero_loss(
    monkeypatch: pytest.MonkeyPatch, quantile: float,
) -> None:
    monkeypatch.setattr(daily_risk, "value_at_risk", lambda *args, **kwargs: quantile)
    writes: list[dict[str, Any]] = []
    with pytest.raises(ValidationError, match="quantile is not finite"):
        run(events([100, 110, 99]), writes)
    assert writes == []


def test_finite_history_still_publishes_all_three_datasets() -> None:
    writes: list[dict[str, Any]] = []
    summary = run(events([100, 110, 99, 108.9]), writes)
    assert len(writes) == 8
    assert {name: value["records_written"] for name, value in summary["curated_output"].items()} == {
        "daily_returns": 3, "daily_volatility": 2, "daily_risk_summary": 3,
    }
    assert all(math.isfinite(v) for record in writes for v in record.values() if isinstance(v, float))


def test_cli_failure_neither_writes_curated_data_nor_creates_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    writes: list[dict[str, Any]] = []
    source = events([1e-308, 1e308], symbol="PRIVATE-SYMBOL")
    monkeypatch.setattr(runner, "load_storage_config", lambda path: _config())
    monkeypatch.setattr(runner, "load_alpha_vantage_daily_events", lambda **kwargs: source)

    def writer(records: list[dict[str, Any]], **kwargs: Any) -> int:
        writes.extend(records)
        return len(records)

    monkeypatch.setattr(runner, "write_records", writer)
    destination = tmp_path / "summary.json"
    result = runner.main([
        "--symbol", "IBM", "--end-date", "2026-01-10", "--vol-window", "2",
        "--var-window", "2", "--summary-json", str(destination),
    ])
    output = capsys.readouterr()
    assert result == 1
    assert output.out == ""
    assert "raw daily data or options were invalid" in output.err
    assert "PRIVATE" not in output.err
    assert writes == []
    assert not destination.exists()
