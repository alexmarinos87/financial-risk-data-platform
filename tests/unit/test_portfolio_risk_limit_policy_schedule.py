from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pytest

from src.analytics.portfolio_risk_limit_policy_schedule import (
    ScheduledRiskLimitPolicy,
    evaluate_portfolio_risk_limit_schedule,
    legacy_policy_schedule,
    parse_portfolio_risk_limit_policy_schedule,
)
from src.analytics.portfolio_risk_limits import (
    CONCENTRATION_METRIC,
    VOLATILITY_METRIC,
    parse_portfolio_risk_limit_policy,
)
from src.common.exceptions import ValidationError

FINGERPRINT = "portfolio-definition-test"


def _payload() -> dict[str, object]:
    return {
        "policies": {
            "test-policy": {
                "portfolio_id": "us-tech-equal",
                "covariance_window": 3,
                "annualization_days": 252,
                "versions": [
                    {
                        "effective_from": "2026-01-01",
                        "effective_to": "2026-07-01",
                        "limits": {
                            VOLATILITY_METRIC: {
                                "warning": 0.30,
                                "critical": 0.45,
                            },
                            CONCENTRATION_METRIC: {
                                "warning": 0.65,
                                "critical": 0.80,
                            },
                        },
                    },
                    {
                        "effective_from": "2026-07-01",
                        "limits": {
                            VOLATILITY_METRIC: {
                                "warning": 0.25,
                                "critical": 0.40,
                            },
                            CONCENTRATION_METRIC: {
                                "warning": 0.55,
                                "critical": 0.75,
                            },
                        },
                    },
                ],
            }
        }
    }


def _record(
    event_date: date,
    *,
    volatility: float = 0.28,
    aapl_share: float = 0.60,
    calculation_id: str | None = None,
    ingested_at: datetime | None = None,
) -> dict[str, object]:
    event = datetime.combine(
        event_date,
        datetime.min.time(),
        timezone.utc,
    )
    return {
        "model_version": "portfolio-attribution-v1",
        "calculation_id": (
            calculation_id
            or f"attribution-{event_date.isoformat()}"
        ),
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": FINGERPRINT,
        "weighting_method": (
            "constant_weight_daily_rebalanced"
        ),
        "covariance_method": "sample_annualized",
        "correlation_method": "pearson",
        "covariance_window": 3,
        "annualization_days": 252,
        "ts_event": event,
        "ts_ingest": (
            ingested_at
            or event + timedelta(hours=1)
        ),
        "portfolio_volatility_annualized": volatility,
        "volatility_status": (
            "positive" if volatility else "zero"
        ),
        "component_contribution_share_json": json.dumps(
            {
                "alpha_vantage:AAPL": aapl_share,
                "alpha_vantage:MSFT": 1.0 - aapl_share,
            },
            sort_keys=True,
        ),
    }


def test_schedule_selects_thresholds_by_event_date() -> None:
    schedule = parse_portfolio_risk_limit_policy_schedule(
        _payload(),
        "test-policy",
    )
    output = evaluate_portfolio_risk_limit_schedule(
        [
            _record(date(2026, 6, 30)),
            _record(date(2026, 7, 1)),
        ],
        schedule=schedule,
        definition_fingerprint=FINGERPRINT,
    )

    volatility = [
        item
        for item in output.evaluations
        if item["metric_name"] == VOLATILITY_METRIC
    ]
    assert [
        item["status"]
        for item in volatility
    ] == ["ok", "warning"]
    assert [
        item["warning_threshold"]
        for item in volatility
    ] == [0.30, 0.25]
    assert volatility[0]["policy_effective_from"] == date(
        2026,
        1,
        1,
    )
    assert volatility[0]["policy_effective_to"] == date(
        2026,
        7,
        1,
    )
    assert volatility[1]["policy_effective_from"] == date(
        2026,
        7,
        1,
    )
    assert volatility[1]["policy_effective_to"] is None
    assert {
        item["policy_period_source"]
        for item in output.evaluations
    } == {"configured"}
    assert len(
        output.diagnostics["policy_fingerprints"]
    ) == 2


def test_policy_period_changes_calculation_identity() -> None:
    schedule = parse_portfolio_risk_limit_policy_schedule(
        _payload(),
        "test-policy",
    )
    first = evaluate_portfolio_risk_limit_schedule(
        [
            _record(
                date(2026, 6, 30),
                calculation_id="shared-attribution",
            )
        ],
        schedule=schedule,
        definition_fingerprint=FINGERPRINT,
    )
    second = evaluate_portfolio_risk_limit_schedule(
        [
            _record(
                date(2026, 7, 1),
                calculation_id="shared-attribution",
            )
        ],
        schedule=schedule,
        definition_fingerprint=FINGERPRINT,
    )

    assert {
        item["calculation_id"]
        for item in first.evaluations
    }.isdisjoint(
        item["calculation_id"]
        for item in second.evaluations
    )
    assert (
        first.evaluations[0]["policy_fingerprint"]
        != second.evaluations[0]["policy_fingerprint"]
    )


def test_schedule_requires_contiguous_periods() -> None:
    gap = _payload()
    gap["policies"]["test-policy"]["versions"][1][
        "effective_from"
    ] = "2026-07-02"
    with pytest.raises(
        ValidationError,
        match="contiguous",
    ):
        parse_portfolio_risk_limit_policy_schedule(
            gap,
            "test-policy",
        )

    overlap = _payload()
    overlap["policies"]["test-policy"]["versions"][1][
        "effective_from"
    ] = "2026-06-30"
    with pytest.raises(
        ValidationError,
        match="overlap",
    ):
        parse_portfolio_risk_limit_policy_schedule(
            overlap,
            "test-policy",
        )

    open_early = _payload()
    del open_early["policies"]["test-policy"][
        "versions"
    ][0]["effective_to"]
    with pytest.raises(
        ValidationError,
        match="final",
    ):
        parse_portfolio_risk_limit_policy_schedule(
            open_early,
            "test-policy",
        )


def test_schedule_fails_when_history_not_covered() -> None:
    schedule = parse_portfolio_risk_limit_policy_schedule(
        _payload(),
        "test-policy",
    )
    with pytest.raises(
        ValidationError,
        match=(
            "does not cover 2025-12-31 "
            "exactly once"
        ),
    ):
        evaluate_portfolio_risk_limit_schedule(
            [_record(date(2025, 12, 31))],
            schedule=schedule,
            definition_fingerprint=FINGERPRINT,
        )


def test_flat_policy_remains_legacy_unbounded() -> None:
    versions = _payload()["policies"]["test-policy"][
        "versions"
    ]
    flat_payload = {
        "policies": {
            "test-policy": {
                "portfolio_id": "us-tech-equal",
                "covariance_window": 3,
                "annualization_days": 252,
                "limits": versions[0]["limits"],
            }
        }
    }
    schedule = parse_portfolio_risk_limit_policy_schedule(
        flat_payload,
        "test-policy",
    )
    assert len(schedule) == 1
    assert schedule[0].period_source == (
        "legacy_unbounded"
    )
    assert schedule[0].effective_to is None

    policy = parse_portfolio_risk_limit_policy(
        flat_payload,
        "test-policy",
    )
    assert (
        legacy_policy_schedule(policy)[0].fingerprint
        == schedule[0].fingerprint
    )


def test_injected_schedule_identity_is_consistent() -> None:
    schedule = list(
        parse_portfolio_risk_limit_policy_schedule(
            _payload(),
            "test-policy",
        )
    )
    other_policy = parse_portfolio_risk_limit_policy(
        {
            "policies": {
                "test-policy": {
                    "portfolio_id": "other-portfolio",
                    "covariance_window": 3,
                    "annualization_days": 252,
                    "limits": _payload()["policies"][
                        "test-policy"
                    ]["versions"][0]["limits"],
                }
            }
        },
        "test-policy",
    )
    schedule[1] = ScheduledRiskLimitPolicy(
        policy=other_policy,
        effective_from=schedule[1].effective_from,
        effective_to=None,
    )
    with pytest.raises(
        ValidationError,
        match="share policy, portfolio",
    ):
        evaluate_portfolio_risk_limit_schedule(
            [_record(date(2026, 7, 1))],
            schedule=schedule,
            definition_fingerprint=FINGERPRINT,
        )
