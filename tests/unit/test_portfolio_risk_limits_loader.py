from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest

from src.analytics.portfolio_risk_limit_policy_schedule import (
    evaluate_portfolio_risk_limit_schedule,
    parse_portfolio_risk_limit_policy_schedule,
)
from src.analytics.portfolio_risk_limits import (
    evaluate_portfolio_risk_limits,
    parse_portfolio_risk_limit_policy,
)
from src.common.exceptions import StorageError
from src.storage.s3_writer import write_records
from src.warehouse.portfolio_risk_limits_loader import (
    COLUMNS,
    _add_legacy_policy_period,
    build_upsert_sql,
    collect_limit_records,
    load_limits_to_postgres,
)
from tests.storage_config_helpers import (
    build_storage_config,
    write_storage_config,
)


def _attribution(
    event_date: date = date(2026, 1, 4),
) -> dict[str, object]:
    event = datetime.combine(
        event_date,
        datetime.min.time(),
        timezone.utc,
    )
    return {
        "model_version": "portfolio-attribution-v1",
        "calculation_id": (
            f"attribution-{event_date.isoformat()}"
        ),
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": "definition-a",
        "weighting_method": (
            "constant_weight_daily_rebalanced"
        ),
        "covariance_method": "sample_annualized",
        "correlation_method": "pearson",
        "covariance_window": 3,
        "annualization_days": 252,
        "ts_event": event,
        "ts_ingest": event + timedelta(hours=1),
        "portfolio_volatility_annualized": 0.50,
        "volatility_status": "positive",
        "component_contribution_share_json": json.dumps(
            {
                "alpha_vantage:AAPL": 0.82,
                "alpha_vantage:MSFT": 0.18,
            }
        ),
    }


def _flat_evaluations() -> list[dict]:
    policy = parse_portfolio_risk_limit_policy(
        {
            "policies": {
                "test-policy": {
                    "portfolio_id": "us-tech-equal",
                    "covariance_window": 3,
                    "annualization_days": 252,
                    "limits": {
                        "portfolio_volatility_annualized": {
                            "warning": 0.30,
                            "critical": 0.45,
                        },
                        "largest_absolute_component_contribution_share": {
                            "warning": 0.65,
                            "critical": 0.80,
                        },
                    },
                }
            }
        },
        "test-policy",
    )
    return list(
        evaluate_portfolio_risk_limits(
            [_attribution()],
            policy=policy,
            definition_fingerprint="definition-a",
        ).evaluations
    )


def _scheduled_evaluations() -> list[dict]:
    schedule = parse_portfolio_risk_limit_policy_schedule(
        {
            "policies": {
                "test-policy": {
                    "portfolio_id": "us-tech-equal",
                    "covariance_window": 3,
                    "annualization_days": 252,
                    "versions": [
                        {
                            "effective_from": "2026-01-01",
                            "limits": {
                                "portfolio_volatility_annualized": {
                                    "warning": 0.30,
                                    "critical": 0.45,
                                },
                                "largest_absolute_component_contribution_share": {
                                    "warning": 0.65,
                                    "critical": 0.80,
                                },
                            },
                        }
                    ],
                }
            }
        },
        "test-policy",
    )
    return list(
        evaluate_portfolio_risk_limit_schedule(
            [_attribution()],
            schedule=schedule,
            definition_fingerprint="definition-a",
        ).evaluations
    )


def test_loader_collects_effective_dated_evaluations(
    tmp_path,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    assert write_records(
        _scheduled_evaluations(),
        kind="curated",
        dataset=(
            "portfolio_risk_limit_evaluations"
        ),
        storage_config=config,
    ) == 2

    records = collect_limit_records(config_path)
    assert len(records) == 2
    assert set(records[0]) == set(COLUMNS)
    assert {
        record["policy_effective_from"]
        for record in records
    } == {date(2026, 1, 1)}
    assert {
        record["policy_effective_to"]
        for record in records
    } == {None}
    assert {
        record["policy_period_source"]
        for record in records
    } == {"configured"}
    assert load_limits_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=config_path,
        dry_run=True,
    ) == 2


def test_legacy_dataset_gets_one_day_period(
    tmp_path,
) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    assert write_records(
        _flat_evaluations(),
        kind="curated",
        dataset=(
            "portfolio_risk_limit_evaluations"
        ),
        storage_config=config,
    ) == 2

    records = collect_limit_records(config_path)
    assert {
        record["policy_effective_from"]
        for record in records
    } == {date(2026, 1, 4)}
    assert {
        record["policy_effective_to"]
        for record in records
    } == {date(2026, 1, 5)}
    assert {
        record["policy_period_source"]
        for record in records
    } == {"inferred_event_date"}


def test_partial_policy_period_columns_fail_closed() -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_event": datetime(
                    2026,
                    1,
                    4,
                    tzinfo=timezone.utc,
                ),
                "policy_effective_from": date(
                    2026,
                    1,
                    1,
                ),
            }
        ]
    )
    with pytest.raises(
        StorageError,
        match="columns are incomplete",
    ):
        _add_legacy_policy_period(frame)


def test_mixed_legacy_and_scheduled_rows_are_supported() -> None:
    frame = pd.DataFrame(
        [
            {
                "ts_event": datetime(
                    2026,
                    1,
                    4,
                    tzinfo=timezone.utc,
                ),
                "policy_effective_from": None,
                "policy_effective_to": None,
                "policy_period_source": None,
            },
            {
                "ts_event": datetime(
                    2026,
                    7,
                    2,
                    tzinfo=timezone.utc,
                ),
                "policy_effective_from": date(
                    2026,
                    7,
                    1,
                ),
                "policy_effective_to": None,
                "policy_period_source": "configured",
            },
        ]
    )
    result = _add_legacy_policy_period(frame)
    assert result.loc[
        0,
        "policy_period_source",
    ] == "inferred_event_date"
    assert result.loc[
        0,
        "policy_effective_to",
    ] == date(2026, 1, 5)
    assert result.loc[
        1,
        "policy_effective_from",
    ] == date(2026, 7, 1)


def test_loader_returns_zero_when_dataset_absent(
    tmp_path,
) -> None:
    config_path = write_storage_config(tmp_path)
    assert collect_limit_records(config_path) == []
    assert load_limits_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=config_path,
        dry_run=True,
    ) == 0


def test_upsert_includes_policy_period_evidence() -> None:
    statement = build_upsert_sql()
    assert (
        'INSERT INTO "risk_platform".'
        '"portfolio_risk_limit_evaluations"'
        in statement
    )
    assert (
        'ON CONFLICT ("calculation_id") DO UPDATE'
        in statement
    )
    assert (
        '"policy_effective_from" = '
        'EXCLUDED."policy_effective_from"'
        in statement
    )
    assert (
        '"policy_period_source" = '
        'EXCLUDED."policy_period_source"'
        in statement
    )
    assert len(COLUMNS) == 31
