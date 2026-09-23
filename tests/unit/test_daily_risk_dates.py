"""Calendar-date validation precedes event consumption and runner I/O."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.daily_risk import build_daily_risk_outputs
from src.common.exceptions import ValidationError
from src.orchestration import run_daily_risk as runner

class CalendarSubclass(date):
    pass


INVALID_DATES = [
    pytest.param(CalendarSubclass(2024, 2, 29), id="date-subclass"),
    pytest.param("2024-02-29", id="text"), pytest.param(False, id="boolean"),
    pytest.param(20240229, id="integer"),
    pytest.param(datetime(2024, 2, 29), id="naive-datetime"),
    pytest.param(datetime(2024, 2, 29, tzinfo=timezone.utc), id="aware-datetime"),
]


def _events() -> list[dict[str, Any]]:
    first = datetime(2024, 2, 28, tzinfo=timezone.utc)
    return [
        {"event_id": f"calendar-{i}", "symbol": "IBM", "price": price, "volume": 1,
         "source": "alpha_vantage", "ts_event": first + timedelta(days=i),
         "ts_ingest": first + timedelta(days=i, hours=1)}
        for i, price in enumerate([100.0, 110.0, 99.0])
    ]


@pytest.mark.parametrize("value", INVALID_DATES)
@pytest.mark.parametrize("field", ["start_date", "end_date"])
def test_bad_dates_reject_without_consuming_history(field: str, value: Any) -> None:
    consumed: list[bool] = []

    def history() -> Any:
        consumed.append(True)
        yield from _events()

    with pytest.raises(ValidationError, match=f"^{field} must be a calendar date or None$"):
        build_daily_risk_outputs(history(), **{field: value})
    assert consumed == []


@pytest.mark.parametrize("value", INVALID_DATES)
@pytest.mark.parametrize("field", ["start_date", "end_date"])
def test_bad_dates_reject_before_runner_io(field: str, value: Any) -> None:
    calls: list[str] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append("attempted I/O")
        raise AssertionError("bad dates must not reach I/O")

    options = {"start_date": None, "end_date": date(2024, 3, 1), field: value}
    with pytest.raises(ValidationError, match=f"^{field} must be a calendar date or None$"):
        runner.run_daily_risk(
            symbol="IBM", **options, volatility_window=2, var_window=2, var_confidence=0.95,
            storage_config_path=Path("must-not-read.yaml"), config_loader=forbidden,
            reader=forbidden, writer=forbidden,
        )
    assert calls == []


@pytest.mark.parametrize("end", [None, date.max])
def test_reader_unrepresentable_end_dates_reject_before_configuration(end: Any) -> None:
    calls: list[bool] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append(True)
        raise AssertionError("unsupported end date reached configuration")

    with pytest.raises(ValidationError, match="^end_date"):
        runner.run_daily_risk(
            symbol="IBM", start_date=None, end_date=end, volatility_window=2, var_window=2,
            var_confidence=0.95, storage_config_path=Path("unused.yaml"), config_loader=forbidden,
            reader=forbidden, writer=forbidden,
        )
    assert calls == []


def test_reversed_calendar_range_does_not_consume_history() -> None:
    consumed: list[bool] = []

    def history() -> Any:
        consumed.append(True)
        yield from _events()

    with pytest.raises(ValidationError, match="^start_date must be on or before end_date$"):
        build_daily_risk_outputs(history(), start_date=date(2024, 3, 1), end_date=date(2024, 2, 29))
    assert consumed == []


def test_valid_leap_day_selection_is_inclusive_and_preserves_history() -> None:
    whole = build_daily_risk_outputs(_events(), volatility_window=2, var_window=2)
    selected = build_daily_risk_outputs(
        _events(), start_date=date(2024, 2, 29), end_date=date(2024, 2, 29),
        volatility_window=2, var_window=2,
    )
    assert selected.returns == whole.returns[:1]
    assert selected.risk_summary == whole.risk_summary[:1]
    assert selected.volatility == ()


def test_builder_keeps_optional_and_maximum_date_support() -> None:
    whole = build_daily_risk_outputs(_events(), volatility_window=2, var_window=2)
    assert whole == build_daily_risk_outputs(
        _events(), start_date=date.min, end_date=date.max, volatility_window=2, var_window=2,
    )


def test_cli_end_date_limit_rejects_before_storage_and_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[bool] = []

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        calls.append(True)
        raise AssertionError("invalid calendar request reached storage")

    monkeypatch.setattr(runner, "load_storage_config", forbidden)
    destination = tmp_path / "summary.json"
    result = runner.main([
        "--symbol", "IBM", "--end-date", "9999-12-31", "--summary-json", str(destination),
    ])
    captured = capsys.readouterr()
    assert result == 1
    assert captured.out == ""
    assert "raw daily data or options were invalid" in captured.err
    assert calls == []
    assert not destination.exists()
