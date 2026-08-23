from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _extract_statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    end = sql.index(";", start) + 1
    return sql[start:end]


def test_method_schema_constrains_v2_and_exposes_comparison_view() -> None:
    sql = _read_sql("sql/portfolio_risk_limits_method_schema.sql")

    assert "portfolio_risk_limit_evaluations_v2_method_contract" in sql
    assert "portfolio-risk-limits-v2" in sql
    assert "portfolio-attribution-v1" in sql
    assert "portfolio-attribution-ewma-v1" in sql
    assert "sample_annualized" in sql
    assert "ewma_zero_mean_lambda_0_94_annualized" in sql
    assert (
        "CREATE OR REPLACE VIEW\n"
        "    risk_platform.portfolio_risk_limit_method_comparison"
    ) in sql


def test_method_comparison_view_pairs_aligned_current_evaluations() -> None:
    sql = _read_sql("sql/portfolio_risk_limits_method_schema.sql")
    view_sql = _extract_statement(
        sql,
        "CREATE OR REPLACE VIEW\n"
        "    risk_platform.portfolio_risk_limit_method_comparison AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE
                risk_platform.latest_portfolio_risk_limit_evaluations (
                    calculation_id TEXT,
                    model_version TEXT,
                    policy_id TEXT,
                    policy_fingerprint TEXT,
                    portfolio_id TEXT,
                    base_currency TEXT,
                    definition_fingerprint TEXT,
                    attribution_calculation_id TEXT,
                    attribution_model_version TEXT,
                    weighting_method TEXT,
                    covariance_method TEXT,
                    correlation_method TEXT,
                    covariance_window INTEGER,
                    annualization_days INTEGER,
                    ts_event TIMESTAMPTZ,
                    ts_ingest TIMESTAMPTZ,
                    metric_name TEXT,
                    subject_type TEXT,
                    subject_key TEXT,
                    unit TEXT,
                    observed_value DOUBLE,
                    warning_threshold DOUBLE,
                    critical_threshold DOUBLE,
                    status TEXT,
                    is_breach BOOLEAN
                )
            """
        )
        metric_time = datetime(2026, 3, 31, tzinfo=timezone.utc)
        sample_ingest = datetime(2026, 4, 1, 10, tzinfo=timezone.utc)
        ewma_ingest = datetime(2026, 4, 1, 11, tzinfo=timezone.utc)
        rows = [
            (
                "sample-evaluation",
                "portfolio-risk-limits-v2",
                "us-tech-standard",
                "sample-policy-fingerprint",
                "us-tech-equal",
                "USD",
                "definition-a",
                "sample-attribution",
                "portfolio-attribution-v1",
                "constant_weight_daily_rebalanced",
                "sample_annualized",
                "pearson",
                20,
                252,
                metric_time,
                sample_ingest,
                "portfolio_volatility_annualized",
                "portfolio",
                "us-tech-equal",
                "annualized_decimal",
                0.32,
                0.30,
                0.45,
                "warning",
                True,
            ),
            (
                "ewma-evaluation",
                "portfolio-risk-limits-v2",
                "us-tech-standard",
                "ewma-policy-fingerprint",
                "us-tech-equal",
                "USD",
                "definition-a",
                "ewma-attribution",
                "portfolio-attribution-ewma-v1",
                "constant_weight_daily_rebalanced",
                "ewma_zero_mean_lambda_0_94_annualized",
                "implied_from_ewma_covariance",
                20,
                252,
                metric_time,
                ewma_ingest,
                "portfolio_volatility_annualized",
                "portfolio",
                "us-tech-equal",
                "annualized_decimal",
                0.48,
                0.30,
                0.45,
                "critical",
                True,
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        connection.executemany(
            "INSERT INTO "
            "risk_platform.latest_portfolio_risk_limit_evaluations "
            f"VALUES ({placeholders})",
            rows,
        )
        connection.execute(view_sql)

        result = connection.execute(
            """
            SELECT
                sample_evaluation_calculation_id,
                ewma_evaluation_calculation_id,
                observed_difference_ewma_minus_sample,
                absolute_observed_difference,
                sample_status,
                ewma_status,
                status_disagreement,
                higher_observed_method,
                more_severe_method,
                comparison_ts
            FROM risk_platform.portfolio_risk_limit_method_comparison
            """
        ).fetchone()

    assert result is not None
    assert result[0:2] == ("sample-evaluation", "ewma-evaluation")
    assert result[2] == pytest.approx(0.16)
    assert result[3] == pytest.approx(0.16)
    assert result[4:9] == (
        "warning",
        "critical",
        True,
        "ewma",
        "ewma",
    )
    assert result[9] == ewma_ingest


def test_method_consistency_suite_covers_comparison_contract() -> None:
    sql = _read_sql(
        "sql/portfolio_risk_limits_method_consistency_checks.sql"
    )
    for check_name in (
        "method_aware_risk_limit_contracts_supported",
        "method_aware_risk_limit_exact_pairs_exposed",
        "method_aware_risk_limit_difference_reconciles",
        "method_aware_risk_limit_absolute_difference_reconciles",
        "method_aware_risk_limit_disagreement_flag_reconciles",
        "method_aware_risk_limit_comparison_grain_unique",
        "method_aware_risk_limit_higher_method_reconciles",
        "method_aware_risk_limit_severity_method_reconciles",
    ):
        assert check_name in sql
