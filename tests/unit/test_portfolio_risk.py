from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.analytics.portfolio_risk import (
    MODEL_VERSION,
    WEIGHTING_METHOD,
    build_portfolio_risk_outputs,
    load_portfolio_definition,
    parse_portfolio_definition,
)
from src.common.exceptions import ValidationError


def _definition_payload() -> dict:
    return {
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
    }


def _record(
    symbol: str,
    day: int,
    value: float,
    *,
    calculation_id: str | None = None,
    ingested_at: datetime | None = None,
) -> dict:
    event = datetime(2026, 1, day, tzinfo=timezone.utc)
    return {
        "model_version": "daily-risk-v2",
        "calculation_id": calculation_id or f"{symbol}-{day}",
        "source": "alpha_vantage",
        "symbol": symbol,
        "source_event_id": f"{symbol}-event-{day}",
        "ts_event": event,
        "ts_ingest": ingested_at or event + timedelta(hours=1),
        "return_1d": value,
    }


def _complete_records() -> list[dict]:
    return [
        _record(symbol, day, value)
        for day, aapl, msft in [
            (2, 0.10, 0.02),
            (3, -0.04, 0.0),
            (4, 0.06, 0.02),
        ]
        for symbol, value in [("AAPL", aapl), ("MSFT", msft)]
    ]


def test_parse_portfolio_definition_normalises_and_fingerprints() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )

    assert definition.portfolio_id == "us-tech-equal"
    assert definition.base_currency == "USD"
    assert [item.symbol for item in definition.constituents] == ["AAPL", "MSFT"]
    assert definition.fingerprint.startswith("portfolio-")


def test_load_portfolio_definition_reads_yaml(tmp_path: Path) -> None:
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

    assert (
        load_portfolio_definition(path, "us-tech-equal").portfolio_id
        == "us-tech-equal"
    )


def test_build_portfolio_risk_outputs_calculates_weighted_metrics() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )

    outputs = build_portfolio_risk_outputs(
        _complete_records(),
        definition=definition,
        volatility_window=2,
        var_window=2,
        var_confidence=0.95,
    )

    assert len(outputs.returns) == 3
    assert outputs.returns[0]["portfolio_return_1d"] == pytest.approx(0.06)
    assert outputs.returns[1]["portfolio_return_1d"] == pytest.approx(-0.02)
    assert outputs.returns[0]["model_version"] == MODEL_VERSION
    assert outputs.returns[0]["weighting_method"] == WEIGHTING_METHOD
    assert outputs.returns[0]["calculation_id"].startswith(
        f"{MODEL_VERSION}-return-"
    )

    latest = outputs.risk_summary[-1]
    assert latest["portfolio_return_1d"] == pytest.approx(0.04)
    assert latest["volatility_annualized"] > 0
    assert latest["historical_var_loss"] == pytest.approx(0.017)
    assert latest["maximum_drawdown"] == pytest.approx(-0.02)
    assert latest["history_status"] == "ready"
    assert latest["weighting_method"] == WEIGHTING_METHOD
    assert latest["aligned_observations"] == 3
    assert outputs.diagnostics["weighting_method"] == WEIGHTING_METHOD


def test_latest_component_version_wins_for_the_same_date() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    late = datetime(2026, 2, 1, tzinfo=timezone.utc)
    records = [
        _record("AAPL", 2, 0.10, calculation_id="old"),
        _record(
            "AAPL",
            2,
            0.20,
            calculation_id="new",
            ingested_at=late,
        ),
        _record("MSFT", 2, 0.00),
        _record("AAPL", 3, 0.00),
        _record("MSFT", 3, 0.00),
    ]

    outputs = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=2,
        var_window=2,
    )

    assert outputs.returns[0]["portfolio_return_1d"] == pytest.approx(0.10)
    assert outputs.returns[0]["ts_ingest"] == late
    assert '"alpha_vantage:AAPL":"new"' in outputs.returns[0][
        "component_calculation_ids_json"
    ]


def test_conflicting_reuse_of_a_calculation_id_fails_closed() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    records = [
        _record("AAPL", 2, 0.10, calculation_id="same-id"),
        _record("AAPL", 2, 0.20, calculation_id="same-id"),
        _record("MSFT", 2, 0.00),
        _record("AAPL", 3, 0.00),
        _record("MSFT", 3, 0.00),
    ]

    with pytest.raises(ValidationError, match="conflicting records"):
        build_portfolio_risk_outputs(
            records,
            definition=definition,
            volatility_window=2,
            var_window=2,
        )


def test_identical_duplicate_input_is_ignored() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    aapl = _record("AAPL", 2, 0.10)
    records = [
        aapl,
        dict(aapl),
        _record("MSFT", 2, 0.00),
        _record("AAPL", 3, 0.00),
        _record("MSFT", 3, 0.00),
    ]

    outputs = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=2,
        var_window=2,
    )

    assert len(outputs.returns) == 2
    assert outputs.diagnostics["matched_input_records"] == 5
    assert outputs.diagnostics["current_component_records"] == 4


def test_input_order_does_not_change_calculation_identity() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    records = _complete_records()

    forward = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=2,
        var_window=2,
    )
    reverse = build_portfolio_risk_outputs(
        reversed(records),
        definition=definition,
        volatility_window=2,
        var_window=2,
    )

    assert forward.returns == reverse.returns
    assert forward.risk_summary == reverse.risk_summary


def test_incomplete_dates_are_dropped_and_reported() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    records = [
        _record("AAPL", 2, 0.10),
        _record("MSFT", 2, 0.02),
        _record("AAPL", 3, 0.03),
        _record("AAPL", 4, 0.04),
        _record("MSFT", 4, 0.02),
    ]

    outputs = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=2,
        var_window=2,
    )

    assert [record["ts_event"].date() for record in outputs.returns] == [
        date(2026, 1, 2),
        date(2026, 1, 4),
    ]
    assert outputs.diagnostics["dropped_incomplete_dates"] == 1


def test_start_date_filters_outputs_but_keeps_history_context() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )

    outputs = build_portfolio_risk_outputs(
        _complete_records(),
        definition=definition,
        volatility_window=2,
        var_window=2,
        start_date=date(2026, 1, 4),
    )

    assert len(outputs.returns) == 1
    assert outputs.risk_summary[0]["aligned_observations"] == 3
    assert outputs.risk_summary[0]["history_status"] == "ready"


def test_maximum_drawdown_includes_the_initial_portfolio_value() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    records = [
        _record("AAPL", 2, -0.10),
        _record("MSFT", 2, -0.10),
        _record("AAPL", 3, 0.0),
        _record("MSFT", 3, 0.0),
    ]

    outputs = build_portfolio_risk_outputs(
        records,
        definition=definition,
        volatility_window=2,
        var_window=2,
    )

    assert outputs.risk_summary[0]["maximum_drawdown"] == pytest.approx(-0.10)


@pytest.mark.parametrize(
    "payload",
    [
        {
            "portfolios": {
                "bad": {
                    "base_currency": "USD",
                    "constituents": [
                        {
                            "source": "alpha_vantage",
                            "symbol": "AAPL",
                            "weight": 0.6,
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
        {
            "portfolios": {
                "bad": {
                    "base_currency": "USD",
                    "constituents": [
                        {
                            "source": "alpha_vantage",
                            "symbol": "AAPL",
                            "weight": 0.5,
                        },
                        {
                            "source": "alpha_vantage",
                            "symbol": "AAPL",
                            "weight": 0.5,
                        },
                    ],
                }
            }
        },
    ],
)
def test_invalid_portfolio_definitions_fail_closed(payload: dict) -> None:
    with pytest.raises(ValidationError):
        parse_portfolio_definition(payload, "bad")


def test_missing_constituent_and_insufficient_alignment_fail_closed() -> None:
    definition = parse_portfolio_definition(
        _definition_payload(),
        "us-tech-equal",
    )
    with pytest.raises(ValidationError, match="missing configured constituents"):
        build_portfolio_risk_outputs(
            [_record("AAPL", 2, 0.1), _record("AAPL", 3, 0.2)],
            definition=definition,
            volatility_window=2,
            var_window=2,
        )

    with pytest.raises(ValidationError, match="at least two fully aligned"):
        build_portfolio_risk_outputs(
            [_record("AAPL", 2, 0.1), _record("MSFT", 2, 0.2)],
            definition=definition,
            volatility_window=2,
            var_window=2,
        )
