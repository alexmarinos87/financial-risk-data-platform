"""The fetch must remain bounded even when an earlier count is no longer current."""

import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from src.common.exceptions import StorageError
from src.orchestration import run_daily_risk as runner

ERROR = "^Raw daily storage exceeds the row scan limit$"


def _rows(count: int) -> list[tuple[Any, ...]]:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return [
        (f"event-{day}", "IBM", 100.0 + day, 10,
         (datetime(2026, 1, day, tzinfo=timezone.utc) - epoch) // timedelta(microseconds=1),
         (datetime(2026, 1, day, 1, tzinfo=timezone.utc) - epoch) // timedelta(microseconds=1),
         "alpha_vantage")
        for day in range(1, count + 1)
    ]


def _backend(
    monkeypatch: pytest.MonkeyPatch, counted: int, rows: list[tuple[Any, ...]],
) -> Any:
    state = SimpleNamespace(queries=[], fetched=None, closed=False)

    class Connection:
        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *args: Any) -> None:
            state.closed = True

        def execute(self, sql: str, params: list[Any]) -> "Connection":
            state.queries.append((sql, params))
            return self

        def fetchone(self) -> tuple[int]:
            return (counted,)

        def fetchall(self) -> list[tuple[Any, ...]]:
            sql, params = state.queries[-1]
            selected = rows[:params[-1]] if sql.endswith("LIMIT ?") else rows
            state.fetched = len(selected)
            return selected

    monkeypatch.setitem(sys.modules, "duckdb", SimpleNamespace(connect=Connection))
    monkeypatch.setattr(runner, "_raw_parquet_files", lambda config: [Path("fixture.parquet")])
    monkeypatch.setattr(
        runner, "alpha_vantage_daily_event_id", lambda symbol, day: f"event-{day.day}",
    )
    return state


def _load() -> list[Any]:
    return runner.load_alpha_vantage_daily_events(
        storage_config={}, symbol="IBM", end_date=date(2026, 1, 9),
    )


@pytest.mark.parametrize("limit", [3, 100_000])
def test_select_has_its_own_bound_without_changing_order_or_filters(
    monkeypatch: pytest.MonkeyPatch, limit: int,
) -> None:
    state = _backend(monkeypatch, 2, _rows(2))
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", limit)
    assert len(_load()) == 2
    count_sql, count_params = state.queries[0]
    select_sql, select_params = state.queries[1]
    assert "COUNT(*)" in count_sql
    assert select_sql.endswith("ORDER BY ts_event, event_id LIMIT ?")
    assert select_params == [*count_params, limit + 1]
    assert state.fetched == 2 and state.closed


@pytest.mark.parametrize("actual", [4, 7])
@pytest.mark.parametrize("through_runner", [False, True])
def test_growth_after_count_rejects_instead_of_returning_a_partial_history(
    monkeypatch: pytest.MonkeyPatch, actual: int, through_runner: bool,
) -> None:
    state = _backend(monkeypatch, 2, _rows(actual))
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    writes: list[bool] = []

    def writer(*args: Any, **kwargs: Any) -> int:
        writes.append(True)
        return 1

    with pytest.raises(StorageError, match=ERROR):
        if through_runner:
            monkeypatch.setattr(runner, "_require_daily_datasets", lambda config: None)
            runner.run_daily_risk(
                symbol="IBM", start_date=None, end_date=date(2026, 1, 9),
                volatility_window=2, var_window=2, var_confidence=0.95,
                storage_config_path=Path("unused.yaml"), config_loader=lambda path: {},
                writer=writer,
            )
        else:
            _load()
    assert state.fetched == 4 and state.closed
    assert writes == []


@pytest.mark.parametrize("actual", [2, 3])
def test_accepted_history_remains_complete_when_count_changes_within_budget(
    monkeypatch: pytest.MonkeyPatch, actual: int,
) -> None:
    state = _backend(monkeypatch, 2, _rows(actual))
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    events = _load()
    assert [event.event_id for event in events] == [f"event-{day}" for day in range(1, actual + 1)]
    assert state.fetched == actual and state.closed


def test_excess_count_still_rejects_before_select(monkeypatch: pytest.MonkeyPatch) -> None:
    state = _backend(monkeypatch, 4, _rows(4))
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    with pytest.raises(StorageError, match=ERROR):
        _load()
    assert len(state.queries) == 1
    assert state.fetched is None and state.closed


def test_oversized_result_rejects_before_materializing_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _backend(monkeypatch, 2, [()] * 4)
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    with pytest.raises(StorageError, match=ERROR):
        _load()
    assert state.fetched == 4 and state.closed


def test_zero_count_does_not_authorize_a_later_oversized_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _backend(monkeypatch, 0, _rows(4))
    monkeypatch.setattr(runner, "MAX_RAW_ROWS", 3)
    with pytest.raises(StorageError, match=ERROR):
        _load()
    assert state.fetched == 4 and state.closed
