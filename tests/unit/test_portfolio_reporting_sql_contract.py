from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import duckdb


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _extract_statement(sql: str, prefix: str) -> str:
    start = sql.index(prefix)
    end = sql.index(";", start) + 1
    return sql[start:end]


def test_portfolio_schema_exposes_versioned_serving_contract() -> None:
    schema_sql = _read_sql("sql/portfolio_schema.sql")

    for table_name in (
        "portfolio_daily_returns",
        "portfolio_daily_risk_summary",
    ):
        assert f"CREATE TABLE IF NOT EXISTS risk_platform.{table_name}" in schema_sql

    assert schema_sql.count("calculation_id TEXT PRIMARY KEY") == 2
    assert "REFERENCES\n        risk_platform.portfolio_daily_returns" in schema_sql
    assert "weights_json JSONB NOT NULL" in schema_sql
    assert "component_calculation_ids_json JSONB NOT NULL" in schema_sql
    assert "component_returns_json JSONB NOT NULL" in schema_sql
    assert "contributions_json JSONB NOT NULL" in schema_sql
    assert "constant_weight_daily_rebalanced" in schema_sql
    assert "history_status <> 'ready'" in schema_sql
    assert "idx_portfolio_daily_returns_components" in schema_sql

    for view_name in (
        "latest_portfolio_daily_returns",
        "latest_portfolio_daily_risk_summary",
        "portfolio_risk_semantic_model",
        "portfolio_daily_contribution_model",
    ):
        assert f"CREATE OR REPLACE VIEW risk_platform.{view_name}" in schema_sql

    assert "definition_fingerprint" in schema_sql
    assert "jsonb_each_text" in schema_sql
    assert "component_calculation_ids_json ->> contribution.key" in schema_sql


def test_portfolio_current_views_rank_versions_without_collapsing_definitions() -> None:
    schema_sql = _read_sql("sql/portfolio_schema.sql")
    latest_returns = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW risk_platform.latest_portfolio_daily_returns AS",
    )
    latest_summaries = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW risk_platform.latest_portfolio_daily_risk_summary AS",
    )
    semantic_view = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW risk_platform.portfolio_risk_semantic_model AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.portfolio_daily_returns (
                calculation_id TEXT,
                model_version TEXT,
                portfolio_id TEXT,
                base_currency TEXT,
                definition_fingerprint TEXT,
                weighting_method TEXT,
                ts_event TIMESTAMPTZ,
                ts_ingest TIMESTAMPTZ,
                constituent_count INTEGER,
                weights_json TEXT,
                component_calculation_ids_json TEXT,
                component_returns_json TEXT,
                contributions_json TEXT,
                portfolio_return_1d DOUBLE
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE risk_platform.portfolio_daily_risk_summary (
                calculation_id TEXT,
                model_version TEXT,
                portfolio_id TEXT,
                base_currency TEXT,
                definition_fingerprint TEXT,
                weighting_method TEXT,
                portfolio_return_calculation_id TEXT,
                ts_event TIMESTAMPTZ,
                ts_ingest TIMESTAMPTZ,
                portfolio_return_1d DOUBLE,
                volatility_annualized DOUBLE,
                volatility_window INTEGER,
                annualization_days INTEGER,
                historical_var_loss DOUBLE,
                var_confidence DOUBLE,
                var_window INTEGER,
                var_observations INTEGER,
                maximum_drawdown DOUBLE,
                aligned_observations INTEGER,
                constituent_count INTEGER,
                weights_json TEXT,
                history_status TEXT,
                input_first_calculation_id TEXT,
                input_last_calculation_id TEXT
            )
            """
        )

        metric_time = datetime(2026, 1, 4, tzinfo=timezone.utc)
        old_time = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
        new_time = datetime(2026, 2, 1, 12, tzinfo=timezone.utc)
        weighting_method = "constant_weight_daily_rebalanced"
        weights = '{"alpha_vantage:AAPL":0.5,"alpha_vantage:MSFT":0.5}'
        components = '{"alpha_vantage:AAPL":"aapl-4","alpha_vantage:MSFT":"msft-4"}'
        component_returns = '{"alpha_vantage:AAPL":0.06,"alpha_vantage:MSFT":0.02}'
        contributions = '{"alpha_vantage:AAPL":0.03,"alpha_vantage:MSFT":0.01}'

        return_rows = [
            (
                "return-old",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-a",
                weighting_method,
                metric_time,
                old_time,
                2,
                weights,
                components,
                component_returns,
                contributions,
                0.04,
            ),
            (
                "return-new",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-a",
                weighting_method,
                metric_time,
                new_time,
                2,
                weights,
                components,
                component_returns,
                contributions,
                0.04,
            ),
            (
                "return-alt-definition",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-b",
                weighting_method,
                metric_time,
                old_time,
                2,
                '{"alpha_vantage:AAPL":0.6,"alpha_vantage:MSFT":0.4}',
                components,
                component_returns,
                '{"alpha_vantage:AAPL":0.036,"alpha_vantage:MSFT":0.008}',
                0.044,
            ),
        ]
        return_placeholders = ", ".join(["?"] * len(return_rows[0]))
        connection.executemany(
            "INSERT INTO risk_platform.portfolio_daily_returns VALUES "
            f"({return_placeholders})",
            return_rows,
        )

        summary_rows = [
            (
                "summary-old",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-a",
                weighting_method,
                "return-old",
                metric_time,
                old_time,
                0.04,
                0.20,
                20,
                252,
                0.02,
                0.95,
                60,
                3,
                -0.02,
                3,
                2,
                weights,
                "partial",
                "return-day-1",
                "return-old",
            ),
            (
                "summary-new",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-a",
                weighting_method,
                "return-new",
                metric_time,
                new_time,
                0.04,
                0.19,
                20,
                252,
                0.018,
                0.95,
                60,
                3,
                -0.02,
                3,
                2,
                weights,
                "partial",
                "return-day-1",
                "return-new",
            ),
            (
                "summary-window-10",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-a",
                weighting_method,
                "return-new",
                metric_time,
                new_time,
                0.04,
                0.18,
                10,
                252,
                0.018,
                0.95,
                60,
                3,
                -0.02,
                3,
                2,
                weights,
                "partial",
                "return-day-1",
                "return-new",
            ),
            (
                "summary-alt-definition",
                "portfolio-risk-v1",
                "us-tech",
                "USD",
                "definition-b",
                weighting_method,
                "return-alt-definition",
                metric_time,
                old_time,
                0.044,
                0.21,
                20,
                252,
                0.021,
                0.95,
                60,
                3,
                -0.01,
                3,
                2,
                '{"alpha_vantage:AAPL":0.6,"alpha_vantage:MSFT":0.4}',
                "partial",
                "return-day-1-alt",
                "return-alt-definition",
            ),
        ]
        summary_placeholders = ", ".join(["?"] * len(summary_rows[0]))
        connection.executemany(
            "INSERT INTO risk_platform.portfolio_daily_risk_summary VALUES "
            f"({summary_placeholders})",
            summary_rows,
        )

        connection.execute(latest_returns)
        connection.execute(latest_summaries)
        connection.execute(semantic_view)

        assert connection.execute(
            """
            SELECT calculation_id, definition_fingerprint
            FROM risk_platform.latest_portfolio_daily_returns
            ORDER BY definition_fingerprint
            """
        ).fetchall() == [
            ("return-new", "definition-a"),
            ("return-alt-definition", "definition-b"),
        ]
        assert connection.execute(
            """
            SELECT calculation_id, definition_fingerprint, volatility_window
            FROM risk_platform.latest_portfolio_daily_risk_summary
            ORDER BY definition_fingerprint, volatility_window
            """
        ).fetchall() == [
            ("summary-window-10", "definition-a", 10),
            ("summary-new", "definition-a", 20),
            ("summary-alt-definition", "definition-b", 20),
        ]
        assert connection.execute(
            "SELECT COUNT(*) FROM risk_platform.portfolio_risk_semantic_model"
        ).fetchone() == (3,)


def test_portfolio_consistency_checks_cover_serving_evidence() -> None:
    consistency_sql = _read_sql("sql/portfolio_risk_consistency_checks.sql")

    for check_name in (
        "portfolio_summary_rows_present",
        "portfolio_components_reference_daily_returns",
        "portfolio_component_json_keys_align",
        "portfolio_weights_sum_to_one",
        "portfolio_contributions_sum_to_return",
        "portfolio_contributions_match_weighted_components",
        "portfolio_summaries_reference_portfolio_returns",
        "portfolio_calculation_ids_unique",
        "latest_portfolio_return_grain_unique",
        "latest_portfolio_return_selects_current_version",
        "latest_portfolio_summary_parameter_grain_unique",
        "latest_portfolio_summary_selects_current_version",
        "ready_portfolio_history_has_required_observations",
        "portfolio_semantic_rows_match_latest_summary",
        "portfolio_contribution_rows_match_constituent_counts",
    ):
        assert check_name in consistency_sql

    assert "jsonb_object_keys" in consistency_sql
    assert "jsonb_each_text" in consistency_sql
    assert "component_calculation_ids_json" in consistency_sql
