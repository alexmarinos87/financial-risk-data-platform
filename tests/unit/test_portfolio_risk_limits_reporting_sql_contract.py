from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    return sql[start : sql.index(";", start) + 1]


def test_limit_schema_exposes_history_and_reporting_views() -> None:
    sql = _read("sql/portfolio_risk_limits_schema.sql")
    assert (
        "CREATE TABLE IF NOT EXISTS "
        "risk_platform.portfolio_risk_limit_evaluations" in sql
    )
    assert "attribution_calculation_id TEXT NOT NULL REFERENCES" in sql
    assert "portfolio-risk-limits-v1" in sql
    assert "warning_threshold > 0" in sql
    assert "critical_threshold > warning_threshold" in sql
    for view in (
        "latest_portfolio_risk_limit_evaluations",
        "portfolio_risk_limit_breaches",
        "portfolio_risk_limit_snapshot_status",
    ):
        assert f"CREATE OR REPLACE VIEW risk_platform.{view}" in sql


def test_latest_view_ranks_corrections_without_collapsing_policy_versions() -> None:
    sql = _read("sql/portfolio_risk_limits_schema.sql")
    latest = _statement(
        sql,
        "CREATE OR REPLACE VIEW "
        "risk_platform.latest_portfolio_risk_limit_evaluations AS",
    )
    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.portfolio_risk_limit_evaluations (
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
                observed_signed_value DOUBLE,
                warning_threshold DOUBLE,
                critical_threshold DOUBLE,
                status TEXT,
                is_breach BOOLEAN,
                breach_threshold DOUBLE,
                breach_excess DOUBLE
            )
            """
        )
        metric_time = datetime(2026, 1, 4, tzinfo=timezone.utc)
        old_time = datetime(2026, 1, 10, tzinfo=timezone.utc)
        new_time = datetime(2026, 2, 1, tzinfo=timezone.utc)
        common = (
            "portfolio-risk-limits-v1",
            "test-policy",
            "policy-a",
            "us-tech-equal",
            "USD",
            "definition-a",
            "portfolio-attribution-v1",
            "constant_weight_daily_rebalanced",
            "sample_annualized",
            "pearson",
            20,
            252,
            metric_time,
        )
        rows = [
            (
                "old-vol",
                *common[:6],
                "attribution-old",
                *common[6:],
                old_time,
                "portfolio_volatility_annualized",
                "portfolio",
                "us-tech-equal",
                "annualized_decimal",
                0.30,
                0.30,
                0.30,
                0.45,
                "warning",
                True,
                0.30,
                0.0,
            ),
            (
                "new-vol",
                *common[:6],
                "attribution-new",
                *common[6:],
                new_time,
                "portfolio_volatility_annualized",
                "portfolio",
                "us-tech-equal",
                "annualized_decimal",
                0.50,
                0.50,
                0.30,
                0.45,
                "critical",
                True,
                0.45,
                0.05,
            ),
            (
                "concentration",
                *common[:6],
                "attribution-new",
                *common[6:],
                new_time,
                "largest_absolute_component_contribution_share",
                "constituent",
                "alpha_vantage:AAPL",
                "absolute_share",
                0.82,
                0.82,
                0.65,
                0.80,
                "critical",
                True,
                0.80,
                0.02,
            ),
            (
                "other-policy",
                common[0],
                common[1],
                "policy-b",
                *common[3:6],
                "attribution-new",
                *common[6:],
                new_time,
                "portfolio_volatility_annualized",
                "portfolio",
                "us-tech-equal",
                "annualized_decimal",
                0.50,
                0.50,
                0.35,
                0.55,
                "warning",
                True,
                0.35,
                0.15,
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        connection.executemany(
            "INSERT INTO risk_platform.portfolio_risk_limit_evaluations "
            f"VALUES ({placeholders})",
            rows,
        )
        connection.execute(latest)
        assert connection.execute(
            """
            SELECT calculation_id, policy_fingerprint, metric_name
            FROM risk_platform.latest_portfolio_risk_limit_evaluations
            ORDER BY policy_fingerprint, metric_name
            """
        ).fetchall() == [
            (
                "concentration",
                "policy-a",
                "largest_absolute_component_contribution_share",
            ),
            ("new-vol", "policy-a", "portfolio_volatility_annualized"),
            ("other-policy", "policy-b", "portfolio_volatility_annualized"),
        ]


def test_consistency_sql_covers_limit_evidence() -> None:
    sql = _read("sql/portfolio_risk_limits_consistency_checks.sql")
    for name in (
        "portfolio_risk_limit_attribution_references_valid",
        "portfolio_risk_limit_attribution_metadata_aligns",
        "portfolio_risk_limit_uses_current_attribution_versions",
        "portfolio_risk_limit_observed_values_match_attribution",
        "portfolio_risk_limit_calculation_ids_unique",
        "latest_portfolio_risk_limit_grain_unique",
        "latest_portfolio_risk_limit_selects_current_version",
        "portfolio_risk_limit_snapshot_has_two_metrics",
        "portfolio_risk_limit_breach_view_matches_current_breaches",
        "portfolio_risk_limit_snapshot_status_values_reconcile",
    ):
        assert name in sql
    assert "jsonb_each_text" in sql
