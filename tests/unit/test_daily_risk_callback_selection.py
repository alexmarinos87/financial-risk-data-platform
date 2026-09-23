"""Explicit I/O dependencies are selected by presence, never by truth value."""

from collections.abc import Callable
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.daily_risk import build_daily_risk_outputs
from src.common.exceptions import StorageError, ValidationError
from src.ingestion.schemas import MarketEvent
from src.orchestration import run_daily_risk as runner

DEFAULT_NAMES = {
    "config_loader": "load_storage_config",
    "reader": "load_alpha_vantage_daily_events",
    "writer": "write_records",
}


class Callback:
    def __init__(self, function: Callable[..., Any]) -> None:
        self.function = function
        self.truth_checks = 0

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self.function(*args, **kwargs)


class FalseCallback(Callback):
    def __bool__(self) -> bool:
        self.truth_checks += 1
        return False


class EmptyCallback(Callback):
    def __len__(self) -> int:
        self.truth_checks += 1
        return 0


class NoTruthCallback(Callback):
    def __bool__(self) -> bool:
        self.truth_checks += 1
        raise AssertionError("dependency truth value must not be evaluated")


def _callbacks() -> tuple[dict[str, Any], list[str], dict[str, list[Any]], list[MarketEvent]]:
    events = [MarketEvent(
        event_id=f"callback-{day}", source="alpha_vantage", symbol="IBM",
        price=price, volume=10,
        ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
        ts_ingest=datetime(2026, 1, day, 1, tzinfo=timezone.utc),
    ) for day, price in enumerate([100.0, 110.0, 99.0, 108.9], 1)]
    calls: list[str] = []
    writes: dict[str, list[Any]] = {}

    def loader(path: Path) -> dict[str, Any]:
        calls.append("config_loader")
        return {"storage": {
            "base_dir": "unused", "format": "parquet", "partitioning": {"granularity": "hour"},
            "raw": {"base_path": "unused/raw", "dataset": "market_events"},
            "curated": {"base_path": "unused/curated", "datasets": {
                name: name for name in runner.DAILY_DATASETS.values()
            }},
        }}

    def reader(**kwargs: Any) -> list[MarketEvent]:
        calls.append("reader")
        return events

    def writer(records: list[dict[str, Any]], **kwargs: Any) -> int:
        calls.append("writer")
        writes.setdefault(kwargs["dataset"], []).extend(records)
        return len(records)

    return {"config_loader": loader, "reader": reader, "writer": writer}, calls, writes, events


def run(callbacks: dict[str, Any], *, volatility_window: int = 2) -> dict[str, Any]:
    return runner.run_daily_risk(
        symbol="IBM", start_date=None, end_date=date(2026, 1, 4),
        volatility_window=volatility_window, var_window=2, var_confidence=0.95,
        storage_config_path=Path("must-not-be-read.yaml"), **callbacks,
    )


@pytest.mark.parametrize("name", DEFAULT_NAMES)
@pytest.mark.parametrize("callback_type", [FalseCallback, EmptyCallback, NoTruthCallback])
def test_explicit_dependency_is_used_without_truth_testing(
    monkeypatch: pytest.MonkeyPatch, name: str, callback_type: type[Callback],
) -> None:
    callbacks, calls, writes, events = _callbacks()
    before = [event.model_dump() for event in events]
    selected = callback_type(callbacks[name])
    callbacks[name] = selected

    def forbidden_default(*args: Any, **kwargs: Any) -> Any:
        calls.append("unexpected_default")
        raise AssertionError("an explicit dependency was discarded")

    for default_name in DEFAULT_NAMES.values():
        monkeypatch.setattr(runner, default_name, forbidden_default)
    result = run(callbacks)
    expected = build_daily_risk_outputs(events, volatility_window=2, var_window=2)
    assert writes == {"daily_returns": list(expected.returns),
                      "daily_volatility": list(expected.volatility),
                      "daily_risk_summary": list(expected.risk_summary)}
    assert calls == ["config_loader", "reader"] + ["writer"] * 8
    assert selected.truth_checks == 0
    assert sum(item["records_written"] for item in result["curated_output"].values()) == 8
    assert [event.model_dump() for event in events] == before


@pytest.mark.parametrize("explicit_none", [False, True])
def test_omitted_and_none_dependencies_still_select_defaults(
    monkeypatch: pytest.MonkeyPatch, explicit_none: bool,
) -> None:
    callbacks, calls, writes, _ = _callbacks()
    for name, default_name in DEFAULT_NAMES.items():
        monkeypatch.setattr(runner, default_name, callbacks[name])
    run(dict.fromkeys(DEFAULT_NAMES) if explicit_none else {})
    assert calls == ["config_loader", "reader"] + ["writer"] * 8
    assert sum(map(len, writes.values())) == 8


@pytest.mark.parametrize("name", DEFAULT_NAMES)
def test_failing_false_dependency_is_not_replaced_by_a_default(
    monkeypatch: pytest.MonkeyPatch, name: str,
) -> None:
    callbacks, calls, _, _ = _callbacks()

    def failure(*args: Any, **kwargs: Any) -> Any:
        calls.append("selected_failure")
        raise OSError("private adapter detail")

    def forbidden_default(*args: Any, **kwargs: Any) -> Any:
        calls.append("unexpected_default")
        raise AssertionError("no fallback is permitted")

    selected = FalseCallback(failure)
    callbacks[name] = selected
    monkeypatch.setattr(runner, DEFAULT_NAMES[name], forbidden_default)
    with pytest.raises(StorageError) as caught:
        run(callbacks)
    assert calls[-1] == "selected_failure"
    assert "unexpected_default" not in calls
    assert "private adapter detail" not in str(caught.value)
    assert selected.truth_checks == 0


def test_invalid_parameters_do_not_activate_or_truth_test_dependencies() -> None:
    callbacks, calls, _, _ = _callbacks()
    selected = {name: NoTruthCallback(function) for name, function in callbacks.items()}
    with pytest.raises(ValidationError):
        run(selected, volatility_window=1)
    assert calls == []
    assert all(callback.truth_checks == 0 for callback in selected.values())
