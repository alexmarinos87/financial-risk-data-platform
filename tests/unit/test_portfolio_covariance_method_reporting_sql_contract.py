from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest
import yaml


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _extract_statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    end = sql.index(";", start) + 1
    return sql[start:end]


def test_covariance_method_schema_registers_ewma_contract_and_view() -> None:
    schema = _read_sql("sql/portfolio_covariance_method_schema.sql")

    for required in (
        "chk_portfolio_attribution_ewma_v1",
        "portfolio-attribution-ewma-v1",
        "ewma_zero_mean_lambda_0_94_annualized",
        "implied_from_ewma_covariance",
        "portfolio_covariance_method_comparison",
        "ewma_minus_sample_volatility",
        "ewma_to_sample_volatility_ratio",
        "higher_volatility_model",
        "input_calculation_ids_json = sample.input_calculation_ids_json",
    ):
        assert required in schema


def test_covariance_method_view_pairs_only_exactly_aligned_inputs() -> None:
    schema = _read_sql("sql/portfolio_covariance_method_schema.sql")
    view = _extract_statement(
        schema,
        "CREATE OR REPLACE VIEW risk_platform.portfolio_covariance_method_comparison AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.latest_portfolio_risk_attribution (
                portfolio_id TEXT,
                base_currency TEXT,
                definition_fingerprint TEXT,
                weighting_method TEXT,
                covariance_window INTEGER,
                window_start TIMESTAMPTZ,
                window_end TIMESTAMPTZ,
                window_observations INTEGER,
                annualization_days INTEGER,
                ts_event TIMESTAMPTZ,
                input_calculation_ids_json TEXT,
                calculation_id TEXT,
                ts_ingest TIMESTAMPTZ,
                portfolio_variance_annualized DOUBLE,
                portfolio_volatility_annualized DOUBLE,
                model_version TEXT,
                covariance_method TEXT,
                correlation_method TEXT
            )
            """
        )
        metric_time = datetime(2026, 1, 6, tzinfo=timezone.utc)
        window_start = datetime(2026, 1, 4, tzinfo=timezone.utc)
        sample_ingest = datetime(2026, 2, 1, tzinfo=timezone.utc)
        ewma_ingest = datetime(2026, 2, 2, tzinfo=timezone.utc)
        common = (
            "us-tech",
            "USD",
            "definition-a",
            "constant_weight_daily_rebalanced",
            3,
            window_start,
            metric_time,
            3,
            252,
            metric_time,
            '["r4","r5","r6"]',
        )
        rows = [
            (
                *common,
                "sample-6",
                sample_ingest,
                0.04,
                0.20,
                "portfolio-attribution-v1",
                "sample_annualized",
                "pearson",
            ),
            (
                *common,
                "ewma-6",
                ewma_ingest,
                0.0625,
                0.25,
                "portfolio-attribution-ewma-v1",
                "ewma_zero_mean_lambda_0_94_annualized",
                "implied_from_ewma_covariance",
            ),
            (
                *common[:-1],
                '["different","inputs","here"]',
                "ewma-misaligned",
                ewma_ingest,
                0.09,
                0.30,
                "portfolio-attribution-ewma-v1",
                "ewma_zero_mean_lambda_0_94_annualized",
                "implied_from_ewma_covariance",
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        connection.executemany(
            "INSERT INTO risk_platform.latest_portfolio_risk_attribution VALUES "
            f"({placeholders})",
            rows,
        )
        connection.execute(view)

        result = connection.execute(
            """
            SELECT
                sample_calculation_id,
                ewma_calculation_id,
                sample_portfolio_volatility_annualized,
                ewma_portfolio_volatility_annualized,
                ewma_minus_sample_volatility,
                ewma_to_sample_volatility_ratio,
                higher_volatility_model
            FROM risk_platform.portfolio_covariance_method_comparison
            """
        ).fetchall()
        assert len(result) == 1
        row = result[0]
        assert row[0:4] == ("sample-6", "ewma-6", 0.20, 0.25)
        assert row[4] == pytest.approx(0.05)
        assert row[5] == pytest.approx(1.25)
        assert row[6] == "ewma"


def test_covariance_method_reconciliation_covers_pair_math_and_grain() -> None:
    consistency = _read_sql(
        "sql/portfolio_covariance_method_consistency_checks.sql"
    )
    for check_name in (
        "portfolio_covariance_method_pairs_complete",
        "portfolio_covariance_method_difference_reconciles",
        "portfolio_covariance_method_ratio_reconciles",
        "portfolio_covariance_method_higher_label_reconciles",
        "portfolio_covariance_method_grain_unique",
    ):
        assert check_name in consistency


def test_docker_initializes_covariance_method_schema_after_attribution() -> None:
    compose = yaml.safe_load(Path("docker-compose.yml").read_text(encoding="utf-8"))
    volumes = compose["services"]["postgres"]["volumes"]
    attribution = (
        "./sql/portfolio_attribution_schema.sql:"
        "/docker-entrypoint-initdb.d/04_portfolio_attribution_schema.sql:ro"
    )
    comparison = (
        "./sql/portfolio_covariance_method_schema.sql:"
        "/docker-entrypoint-initdb.d/10_portfolio_covariance_method_schema.sql:ro"
    )
    assert attribution in volumes
    assert comparison in volumes
    assert volumes.index(attribution) < volumes.index(comparison)
