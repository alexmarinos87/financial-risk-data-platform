from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from src.analytics.portfolio_mandates import (
    filter_records_to_mandate,
    load_portfolio_mandate,
    mandate_metadata,
    parse_portfolio_mandates,
    select_portfolio_mandate,
    validate_mandate_range,
)
from src.analytics.portfolio_risk import parse_portfolio_definition
from src.common.exceptions import ValidationError


def _constituents(aapl_weight: float, msft_weight: float) -> list[dict[str, object]]:
    return [
        {
            "source": "alpha_vantage",
            "symbol": "AAPL",
            "weight": aapl_weight,
        },
        {
            "source": "alpha_vantage",
            "symbol": "MSFT",
            "weight": msft_weight,
        },
    ]


def _multi_mandate_payload() -> dict[str, Any]:
    return {
        "portfolios": {
            "us-tech": {
                "description": "Effective-dated portfolio",
                "mandates": [
                    {
                        "mandate_id": "us-tech-v1",
                        "effective_from": "2025-01-01",
                        "effective_to": "2026-01-01",
                        "base_currency": "USD",
                        "constituents": _constituents(0.5, 0.5),
                    },
                    {
                        "mandate_id": "us-tech-v2",
                        "effective_from": "2026-01-01",
                        "effective_to": None,
                        "base_currency": "USD",
                        "constituents": _constituents(0.6, 0.4),
                    },
                ],
            }
        }
    }


def _mandate_entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
    portfolios = cast(dict[str, Any], payload["portfolios"])
    portfolio = cast(dict[str, Any], portfolios["us-tech"])
    return cast(list[dict[str, Any]], portfolio["mandates"])


def test_current_portfolio_config_has_selectable_effective_mandates() -> None:
    mandate = load_portfolio_mandate(
        Path("config/portfolios.yaml"),
        "us-tech-equal",
        date(2026, 1, 26),
    )

    assert mandate.mandate_id == "us-tech-equal-v1"
    assert mandate.effective_from == date(2020, 1, 1)
    assert mandate.effective_to is None
    assert mandate.contains(date(2026, 1, 26))
    assert mandate.fingerprint.startswith("portfolio-mandate-")
    assert mandate.constituent_definition_fingerprint.startswith("portfolio-")
    assert mandate_metadata(mandate) == {
        "mandate_id": "us-tech-equal-v1",
        "mandate_fingerprint": mandate.fingerprint,
        "constituent_definition_fingerprint": (
            mandate.constituent_definition_fingerprint
        ),
        "effective_from": "2020-01-01",
        "effective_to": None,
    }

    payload = yaml.safe_load(
        Path("config/portfolios.yaml").read_text(encoding="utf-8")
    )
    legacy = parse_portfolio_definition(payload, "us-tech-equal")
    assert legacy.base_currency == mandate.base_currency
    assert legacy.constituents == mandate.constituents


def test_selector_uses_inclusive_start_and_exclusive_end() -> None:
    payload = _multi_mandate_payload()

    first = select_portfolio_mandate(payload, "us-tech", date(2025, 12, 31))
    second = select_portfolio_mandate(payload, "us-tech", date(2026, 1, 1))

    assert first.mandate_id == "us-tech-v1"
    assert second.mandate_id == "us-tech-v2"
    assert first.fingerprint != second.fingerprint
    assert first.constituent_definition_fingerprint != (
        second.constituent_definition_fingerprint
    )


def test_mandate_identity_changes_even_when_weights_are_unchanged() -> None:
    payload = _multi_mandate_payload()
    _mandate_entries(payload)[1]["constituents"] = _constituents(0.5, 0.5)

    first, second = parse_portfolio_mandates(payload, "us-tech")

    assert (
        first.constituent_definition_fingerprint
        == second.constituent_definition_fingerprint
    )
    assert first.fingerprint != second.fingerprint


def test_overlaps_duplicate_ids_and_uncovered_dates_fail_closed() -> None:
    payload = _multi_mandate_payload()
    _mandate_entries(payload)[1]["effective_from"] = "2025-12-01"
    with pytest.raises(ValidationError, match="must not overlap"):
        parse_portfolio_mandates(payload, "us-tech")

    payload = _multi_mandate_payload()
    _mandate_entries(payload)[1]["mandate_id"] = "us-tech-v1"
    with pytest.raises(ValidationError, match="IDs must be unique"):
        parse_portfolio_mandates(payload, "us-tech")

    payload = _multi_mandate_payload()
    _mandate_entries(payload)[0]["effective_to"] = "2025-06-01"
    with pytest.raises(ValidationError, match="no unique mandate"):
        select_portfolio_mandate(payload, "us-tech", date(2025, 8, 1))


def test_requested_ranges_must_not_cross_mandate_boundaries() -> None:
    mandate = select_portfolio_mandate(
        _multi_mandate_payload(),
        "us-tech",
        date(2026, 3, 1),
    )

    validate_mandate_range(
        mandate,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 1),
    )
    with pytest.raises(ValidationError, match="split the request"):
        validate_mandate_range(
            mandate,
            start_date=date(2025, 12, 31),
            end_date=date(2026, 3, 1),
        )


def test_record_filter_keeps_only_dates_owned_by_the_mandate() -> None:
    mandate = select_portfolio_mandate(
        _multi_mandate_payload(),
        "us-tech",
        date(2026, 1, 2),
    )
    records = [
        {
            "calculation_id": "before",
            "ts_event": datetime(2025, 12, 31, tzinfo=timezone.utc),
        },
        {
            "calculation_id": "inside",
            "ts_event": "2026-01-01T00:00:00Z",
        },
    ]

    filtered = filter_records_to_mandate(records, mandate)

    assert [record["calculation_id"] for record in filtered] == ["inside"]
    with pytest.raises(ValidationError, match="must be aware"):
        filter_records_to_mandate(
            [{"ts_event": datetime(2026, 1, 1)}],
            mandate,
        )
