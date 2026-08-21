from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from src.analytics.portfolio_risk_limits import (
    evaluate_portfolio_risk_limits,
    parse_portfolio_risk_limit_policy,
)
from src.storage.s3_writer import write_records
from src.warehouse.portfolio_risk_limits_loader import (
    COLUMNS,
    build_upsert_sql,
    collect_limit_records,
    load_limits_to_postgres,
)
from tests.storage_config_helpers import build_storage_config, write_storage_config


def _evaluations() -> list[dict]:
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
    event = datetime(2026, 1, 4, tzinfo=timezone.utc)
    output = evaluate_portfolio_risk_limits(
        [
            {
                "model_version": "portfolio-attribution-v1",
                "calculation_id": "attribution-4",
                "portfolio_id": "us-tech-equal",
                "base_currency": "USD",
                "definition_fingerprint": "definition-a",
                "weighting_method": "constant_weight_daily_rebalanced",
                "covariance_method": "sample_annualized",
                "correlation_method": "pearson",
                "covariance_window": 3,
                "annualization_days": 252,
                "ts_event": event,
                "ts_ingest": event + timedelta(hours=1),
                "portfolio_volatility_annualized": 0.50,
                "volatility_status": "positive",
                "component_contribution_share_json": json.dumps(
                    {"alpha_vantage:AAPL": 0.82, "alpha_vantage:MSFT": 0.18}
                ),
            }
        ],
        policy=policy,
        definition_fingerprint="definition-a",
    )
    return list(output.evaluations)


def test_loader_collects_evaluations_and_dry_run_counts(tmp_path) -> None:
    config = build_storage_config(tmp_path)
    config_path = write_storage_config(tmp_path)
    assert write_records(
        _evaluations(),
        kind="curated",
        dataset="portfolio_risk_limit_evaluations",
        storage_config=config,
    ) == 2

    records = collect_limit_records(config_path)
    assert len(records) == 2
    assert set(records[0]) == set(COLUMNS)
    assert {record["status"] for record in records} == {"critical"}
    assert load_limits_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=config_path,
        dry_run=True,
    ) == 2


def test_loader_returns_zero_when_dataset_is_absent(tmp_path) -> None:
    config_path = write_storage_config(tmp_path)
    assert collect_limit_records(config_path) == []
    assert load_limits_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=config_path,
        dry_run=True,
    ) == 0


def test_upsert_uses_calculation_id_and_updates_evidence() -> None:
    statement = build_upsert_sql()
    assert 'INSERT INTO "risk_platform"."portfolio_risk_limit_evaluations"' in statement
    assert 'ON CONFLICT ("calculation_id") DO UPDATE' in statement
    assert '"policy_fingerprint" = EXCLUDED."policy_fingerprint"' in statement
    assert len(COLUMNS) == 28
