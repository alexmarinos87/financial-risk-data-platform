"""Invalid numerical requests must not touch configuration, raw data or writers."""

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics import daily_risk
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent
from src.orchestration import run_daily_risk as runner

INVALID_OPTIONS = [
    pytest.param("volatility_window", 1, id="vol-too-small"),
    pytest.param("volatility_window", 253, id="vol-too-large"),
    pytest.param("volatility_window", True, id="vol-boolean"),
    pytest.param("volatility_window", 2.5, id="vol-fraction"),
    pytest.param("var_window", 1, id="var-too-small"),
    pytest.param("var_window", 2521, id="var-too-large"),
    pytest.param("var_window", "2", id="var-text"),
    pytest.param("var_confidence", 0.0, id="confidence-zero"),
    pytest.param("var_confidence", 1.0, id="confidence-one"),
    pytest.param("var_confidence", float("inf"), id="confidence-infinite"),
    pytest.param("var_confidence", float("nan"), id="confidence-nan"),
    pytest.param("var_confidence", 10**1000, id="confidence-overflow"),
    pytest.param("var_confidence", True, id="confidence-boolean"),
    pytest.param("var_confidence", "private-value", id="confidence-text"),
]


def _events() -> list[MarketEvent]:
    return [
        MarketEvent(
            event_id=f"preflight-{day}", source="alpha_vantage", symbol="IBM",
            price=price, volume=1,
            ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
            ts_ingest=datetime(2026, 1, day, 1, tzinfo=timezone.utc),
        ) for day, price in enumerate([100.0, 110.0, 99.0, 108.9], 1)
    ]


def _config() -> dict[str, Any]:
    return {"storage": {
        "base_dir": "unused", "format": "parquet", "partitioning": {"granularity": "hour"},
        "raw": {"base_path": "unused/raw", "dataset": "market_events_raw"},
        "curated": {"base_path": "unused/curated", "datasets": {
            "daily_returns": "daily_returns", "daily_volatility": "daily_volatility",
            "daily_risk_summary": "daily_risk_summary",
        }},
    }}


def _options() -> dict[str, Any]:
    return {"symbol": "IBM", "start_date": None, "end_date": date(2026, 1, 4),
            "volatility_window": 2, "var_window": 2, "var_confidence": 0.95,
            "storage_config_path": Path("must-not-be-read.yaml")}


@pytest.mark.parametrize("key,value", INVALID_OPTIONS)
@pytest.mark.parametrize("use_defaults", [False, True])
def test_invalid_request_rejects_before_every_io_boundary(
    monkeypatch: pytest.MonkeyPatch, key: str, value: Any, use_defaults: bool,
) -> None:
    calls: list[str] = []

    def config_loader(path: Path) -> dict[str, Any]:
        calls.append("configuration")
        return _config()

    def reader(**kwargs: Any) -> list[MarketEvent]:
        calls.append("reader")
        return _events()

    def writer(*args: Any, **kwargs: Any) -> int:
        calls.append("writer")
        return 1

    options = {**_options(), key: value}
    if use_defaults:
        monkeypatch.setattr(runner, "load_storage_config", config_loader)
        monkeypatch.setattr(runner, "load_alpha_vantage_daily_events", reader)
        monkeypatch.setattr(runner, "write_records", writer)
    else:
        options.update(config_loader=config_loader, reader=reader, writer=writer)
    with pytest.raises(ValidationError) as actual:
        runner.run_daily_risk(**options)
    assert calls == []  # Spies record attempts, even if a handler swallows their errors.
    with pytest.raises(ValidationError) as expected:
        daily_risk.build_daily_risk_outputs(_events(), **{key: value})
    assert str(actual.value) == str(expected.value)
    assert "private-value" not in str(actual.value)


@pytest.mark.parametrize("flag,value", [
    ("--vol-window", "1"), ("--vol-window", "253"),
    ("--var-window", "1"), ("--var-window", "2521"),
])
def test_cli_semantic_rejection_precedes_storage_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
    flag: str, value: str,
) -> None:
    calls: list[str] = []

    def loader(path: Path) -> dict[str, Any]:
        calls.append("configuration")
        return _config()

    def reader(**kwargs: Any) -> list[MarketEvent]:
        calls.append("reader")
        return _events()

    def writer(*args: Any, **kwargs: Any) -> int:
        calls.append("writer")
        return 1

    monkeypatch.setattr(runner, "load_storage_config", loader)
    monkeypatch.setattr(runner, "load_alpha_vantage_daily_events", reader)
    monkeypatch.setattr(runner, "write_records", writer)
    destination = tmp_path / "summary.json"
    assert runner.main(["--symbol", "IBM", "--end-date", "2026-01-04", flag, value,
                        "--summary-json", str(destination)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "raw daily data or options were invalid" in captured.err
    assert calls == []
    assert not destination.exists()


@pytest.mark.parametrize("confidence", [0.5, 0.95, 0.99])
def test_valid_request_retains_full_builder_output_and_io_order(confidence: float) -> None:
    calls: list[str] = []
    writes: dict[str, list[dict[str, Any]]] = {}
    source = _events()
    before = [event.model_dump() for event in source]

    def loader(path: Path) -> dict[str, Any]:
        calls.append("configuration")
        return _config()

    def reader(**kwargs: Any) -> list[MarketEvent]:
        calls.append("reader")
        return source

    def writer(records: list[dict[str, Any]], **kwargs: Any) -> int:
        calls.append("writer")
        writes.setdefault(kwargs["dataset"], []).extend(records)
        return len(records)

    result = runner.run_daily_risk(**{**_options(), "var_confidence": confidence},
                                   config_loader=loader, reader=reader, writer=writer)
    expected = daily_risk.build_daily_risk_outputs(
        source, volatility_window=2, var_window=2, var_confidence=confidence,
    )
    assert writes == {"daily_returns": list(expected.returns),
                      "daily_volatility": list(expected.volatility),
                      "daily_risk_summary": list(expected.risk_summary)}
    assert calls == ["configuration", "reader"] + ["writer"] * 8
    assert result["parameters"]["var_confidence"] == confidence
    assert [event.model_dump() for event in source] == before


def test_confidence_is_normalised_before_reader_and_not_reconverted_from_custom_input() -> None:
    conversions: list[bool] = []

    class Confidence(float):
        def __float__(self) -> float:
            conversions.append(True)
            return 0.95

    def reader(**kwargs: Any) -> list[MarketEvent]:
        assert conversions == [True]
        return _events()

    result = runner.run_daily_risk(**{**_options(), "var_confidence": Confidence(0.95)},
                                   config_loader=lambda path: _config(), reader=reader,
                                   writer=lambda records, **kwargs: len(records))
    assert conversions == [True]
    assert result["parameters"]["var_confidence"] == 0.95
