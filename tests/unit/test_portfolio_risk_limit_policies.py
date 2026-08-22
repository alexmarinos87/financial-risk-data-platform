from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest
import yaml

from src.analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
    load_effective_portfolio_risk_limit_policy,
    parse_portfolio_risk_limit_policies,
    policy_metadata,
    select_portfolio_risk_limit_policy,
    validate_policy_range,
)
from src.common.exceptions import ValidationError


def _version(
    version_id: str,
    effective_from: str,
    effective_to: str | None,
    *,
    volatility_warning: float = 0.30,
    portfolio_id: str = "us-tech-equal",
) -> dict[str, object]:
    return {
        "policy_version_id": version_id,
        "effective_from": effective_from,
        "effective_to": effective_to,
        "portfolio_id": portfolio_id,
        "covariance_window": 20,
        "annualization_days": 252,
        "limits": {
            "portfolio_volatility_annualized": {
                "warning": volatility_warning,
                "critical": 0.45,
            },
            "largest_absolute_component_contribution_share": {
                "warning": 0.65,
                "critical": 0.80,
            },
        },
    }


def _history(*versions: dict[str, object]) -> dict[str, object]:
    return {
        "policies": {
            "us-tech-standard": {
                "versions": list(versions),
            }
        }
    }


def test_direct_policy_is_a_single_effective_dated_version() -> None:
    payload = {
        "policies": {
            "us-tech-standard": _version(
                "us-tech-standard-v1",
                "2026-01-01",
                None,
            )
        }
    }

    versions = parse_portfolio_risk_limit_policies(
        payload,
        "us-tech-standard",
    )
    selected = select_portfolio_risk_limit_policy(
        payload,
        "us-tech-standard",
        date(2026, 8, 22),
    )

    assert len(versions) == 1
    assert selected == versions[0]
    assert isinstance(selected, EffectiveDatedPortfolioRiskLimitPolicy)
    assert selected.policy_version_id == "us-tech-standard-v1"
    assert selected.contains(date(2026, 1, 1))
    assert selected.contains(date(2099, 1, 1))
    assert selected.fingerprint.startswith("risk-limit-policy-version-")


def test_policy_history_selects_inclusive_exclusive_versions() -> None:
    payload = _history(
        _version("us-tech-standard-v1", "2026-01-01", "2026-07-01"),
        _version(
            "us-tech-standard-v2",
            "2026-07-01",
            None,
            volatility_warning=0.25,
        ),
    )

    assert select_portfolio_risk_limit_policy(
        payload,
        "us-tech-standard",
        date(2026, 6, 30),
    ).policy_version_id == "us-tech-standard-v1"
    assert select_portfolio_risk_limit_policy(
        payload,
        "us-tech-standard",
        date(2026, 7, 1),
    ).policy_version_id == "us-tech-standard-v2"


def test_renewal_keeps_limit_identity_but_changes_temporal_identity() -> None:
    versions = parse_portfolio_risk_limit_policies(
        _history(
            _version("us-tech-standard-v1", "2026-01-01", "2026-07-01"),
            _version("us-tech-standard-v2", "2026-07-01", None),
        ),
        "us-tech-standard",
    )

    first, second = versions
    assert first.limit_definition_fingerprint == second.limit_definition_fingerprint
    assert first.fingerprint != second.fingerprint
    assert policy_metadata(second) == {
        "policy_id": "us-tech-standard",
        "policy_version_id": "us-tech-standard-v2",
        "policy_fingerprint": second.fingerprint,
        "limit_definition_fingerprint": second.limit_definition_fingerprint,
        "effective_from": "2026-07-01",
        "effective_to": None,
    }


def test_gaps_are_valid_but_uncovered_dates_fail_selection() -> None:
    payload = _history(
        _version("us-tech-standard-v1", "2026-01-01", "2026-04-01"),
        _version("us-tech-standard-v2", "2026-05-01", None),
    )

    versions = parse_portfolio_risk_limit_policies(
        payload,
        "us-tech-standard",
    )
    assert len(versions) == 2
    with pytest.raises(ValidationError, match="no unique version"):
        select_portfolio_risk_limit_policy(
            payload,
            "us-tech-standard",
            date(2026, 4, 15),
        )


def test_overlaps_duplicate_ids_and_mixed_definitions_fail_closed() -> None:
    with pytest.raises(ValidationError, match="must not overlap"):
        parse_portfolio_risk_limit_policies(
            _history(
                _version("v1", "2026-01-01", "2026-08-01"),
                _version("v2", "2026-07-01", None),
            ),
            "us-tech-standard",
        )

    with pytest.raises(ValidationError, match="version IDs must be unique"):
        parse_portfolio_risk_limit_policies(
            _history(
                _version("same", "2026-01-01", "2026-07-01"),
                _version("same", "2026-07-01", None),
            ),
            "us-tech-standard",
        )

    mixed = _history(_version("v1", "2026-01-01", None))
    mixed["policies"]["us-tech-standard"]["portfolio_id"] = "us-tech-equal"
    with pytest.raises(ValidationError, match="must not mix"):
        parse_portfolio_risk_limit_policies(mixed, "us-tech-standard")


def test_open_ended_version_must_be_final_and_portfolio_is_stable() -> None:
    with pytest.raises(ValidationError, match="must be final"):
        parse_portfolio_risk_limit_policies(
            _history(
                _version("v1", "2026-01-01", None),
                _version("v2", "2026-07-01", None),
            ),
            "us-tech-standard",
        )

    with pytest.raises(ValidationError, match="same portfolio"):
        parse_portfolio_risk_limit_policies(
            _history(
                _version("v1", "2026-01-01", "2026-07-01"),
                _version(
                    "v2",
                    "2026-07-01",
                    None,
                    portfolio_id="other-portfolio",
                ),
            ),
            "us-tech-standard",
        )


def test_policy_range_must_stay_inside_one_version() -> None:
    policy = select_portfolio_risk_limit_policy(
        _history(
            _version("v1", "2026-01-01", "2026-07-01"),
            _version("v2", "2026-07-01", None),
        ),
        "us-tech-standard",
        date(2026, 6, 30),
    )

    validate_policy_range(
        policy,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 6, 30),
    )
    with pytest.raises(ValidationError, match="crosses"):
        validate_policy_range(
            policy,
            start_date=date(2025, 12, 31),
            end_date=date(2026, 6, 30),
        )
    with pytest.raises(ValidationError, match="outside"):
        validate_policy_range(
            policy,
            start_date=None,
            end_date=date(2026, 7, 1),
        )


def test_loader_and_strict_date_contract(tmp_path: Path) -> None:
    path = tmp_path / "limits.yaml"
    path.write_text(
        yaml.safe_dump(
            _history(_version("v1", "2026-01-01", None)),
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    loaded = load_effective_portfolio_risk_limit_policy(
        path,
        "us-tech-standard",
        date(2026, 8, 22),
    )
    assert loaded.policy_version_id == "v1"

    invalid = _history(_version("v1", "2026-01-01", None))
    invalid["policies"]["us-tech-standard"]["versions"][0][
        "effective_from"
    ] = datetime(2026, 1, 1)
    with pytest.raises(ValidationError, match="calendar date"):
        parse_portfolio_risk_limit_policies(invalid, "us-tech-standard")
