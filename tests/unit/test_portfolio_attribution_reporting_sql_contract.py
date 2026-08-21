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


def test_attribution_schema_exposes_versioned_serving_contract() -> None:
    schema_sql = _read_sql("sql/portfolio_attribution_schema.sql")

    assert (
        "CREATE TABLE IF NOT EXISTS "
        "risk_platform.portfolio_risk_attribution"
    ) in schema_sql
    assert "calculation_id TEXT PRIMARY KEY" in schema_sql
    assert "input_first_calculation_id TEXT NOT NULL REFERENCES" in schema_sql
    assert "risk_platform.portfolio_daily_returns" in schema_sql
    assert "covariance_annualized_json JSONB NOT NULL" in schema_sql
    assert "correlation_json JSONB NOT NULL" in schema_sql
    assert "component_volatility_contribution_json JSONB NOT NULL" in schema_sql
    assert "component_contribution_share_json JSONB NOT NULL" in schema_sql
    assert "portfolio-attribution-v1" in schema_sql
    assert "sample_annualized" in schema_sql
    assert "undefined_zero_variance" in schema_sql
    assert "ABS(euler_residual) <= 0.00000001" in schema_sql

    for view_name in (
        "latest_portfolio_risk_attribution",
        "portfolio_attribution_semantic_model",
        "portfolio_covariance_model",
        "portfolio_correlation_model",
        "portfolio_volatility_contribution_model",
    ):
        assert (
            f"CREATE OR REPLACE VIEW risk_platform.{view_name}"
            in schema_sql
        )

    assert "jsonb_each(" in schema_sql
    assert "jsonb_each_text(" in schema_sql
    assert "NULLIF(matrix_cell.value::TEXT, 'null')" in schema_sql


def test_latest_attribution_view_preserves_definition_and_window_grains() -> None:
    schema_sql = _read_sql("sql/portfolio_attribution_schema.sql")
    latest_view = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW "
        "risk_platform.latest_portfolio_risk_attribution AS",
    )
    semantic_view = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW "
        "risk_platform.portfolio_attribution_semantic_model AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.portfolio_risk_attribution (
                calculation_id TEXT,
                model_version TEXT,
                portfolio_id TEXT,
                base_currency TEXT,
                definition_fingerprint TEXT,
                weighting_method TEXT,
                covariance_method TEXT,
                correlation_method TEXT,
                covariance_window INTEGER,
                window_start TIMESTAMPTZ,
                window_end TIMESTAMPTZ,
                window_observations INTEGER,
                annualization_days INTEGER,
                ts_event TIMESTAMPTZ,
                ts_ingest TIMESTAMPTZ,
                constituent_count INTEGER,
                weights_json TEXT,
                input_calculation_ids_json TEXT,
                input_first_calculation_id TEXT,
                input_last_calculation_id TEXT,
                covariance_annualized_json TEXT,
                correlation_json TEXT,
                constituent_volatility_annualized_json TEXT,
                marginal_volatility_contribution_json TEXT,
                component_volatility_contribution_json TEXT,
                component_contribution_share_json TEXT,
                portfolio_variance_annualized DOUBLE,
                portfolio_volatility_annualized DOUBLE,
                volatility_status TEXT,
                correlation_status TEXT,
                undefined_correlation_cells INTEGER,
                euler_residual DOUBLE
            )
            """
        )

        metric_time = datetime(2026, 1, 31, tzinfo=timezone.utc)
        window_start = datetime(2026, 1, 2, tzinfo=timezone.utc)
        old_time = datetime(2026, 2, 1, 12, tzinfo=timezone.utc)
        new_time = datetime(2026, 2, 2, 12, tzinfo=timezone.utc)
        weights = '{"alpha_vantage:AAPL":0.5,"alpha_vantage:MSFT":0.5}'
        inputs = '["portfolio-return-1","portfolio-return-2"]'
        covariance = (
            '{"alpha_vantage:AAPL":{"alpha_vantage:AAPL":0.04,'
            '"alpha_vantage:MSFT":0.01},'
            '"alpha_vantage:MSFT":{"alpha_vantage:AAPL":0.01,'
            '"alpha_vantage:MSFT":0.09}}'
        )
        correlation = (
            '{"alpha_vantage:AAPL":{"alpha_vantage:AAPL":1.0,'
            '"alpha_vantage:MSFT":0.1666666667},'
            '"alpha_vantage:MSFT":{"alpha_vantage:AAPL":0.1666666667,'
            '"alpha_vantage:MSFT":1.0}}'
        )
        vector = '{"alpha_vantage:AAPL":0.2,"alpha_vantage:MSFT":0.3}'
        marginal = '{"alpha_vantage:AAPL":0.15,"alpha_vantage:MSFT":0.2}'
        component = '{"alpha_vantage:AAPL":0.075,"alpha_vantage:MSFT":0.1}'
        shares = (
            '{"alpha_vantage:AAPL":0.4285714286,'
            '"alpha_vantage:MSFT":0.5714285714}'
        )

        def row(
            calculation_id: str,
            definition: str,
            covariance_window: int,
            ingested_at: datetime,
        ) -> tuple[object, ...]:
            return (
                calculation_id,
                "portfolio-attribution-v1",
                "us-tech",
                "USD",
                definition,
                "constant_weight_daily_rebalanced",
                "sample_annualized",
                "pearson",
                covariance_window,
                window_start,
                metric_time,
                covariance_window,
                252,
                metric_time,
                ingested_at,
                2,
                weights,
                inputs,
                "portfolio-return-1",
                "portfolio-return-2",
                covariance,
                correlation,
                vector,
                marginal,
                component,
                shares,
                0.030625,
                0.175,
                "positive",
                "complete",
                0,
                0.0,
            )

        rows = [
            row("attribution-old", "definition-a", 20, old_time),
            row("attribution-new", "definition-a", 20, new_time),
            row("attribution-window-10", "definition-a", 10, old_time),
            row("attribution-definition-b", "definition-b", 20, old_time),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        connection.executemany(
            "INSERT INTO risk_platform.portfolio_risk_attribution VALUES "
            f"({placeholders})",
            rows,
        )

        connection.execute(latest_view)
        connection.execute(semantic_view)

        assert connection.execute(
            """
            SELECT calculation_id, definition_fingerprint, covariance_window
            FROM risk_platform.latest_portfolio_risk_attribution
            ORDER BY definition_fingerprint, covariance_window
            """
        ).fetchall() == [
            ("attribution-window-10", "definition-a", 10),
            ("attribution-new", "definition-a", 20),
            ("attribution-definition-b", "definition-b", 20),
        ]
        assert connection.execute(
            "SELECT COUNT(*) "
            "FROM risk_platform.portfolio_attribution_semantic_model"
        ).fetchone() == (3,)


def test_attribution_consistency_checks_cover_matrix_and_euler_evidence() -> None:
    consistency_sql = _read_sql(
        "sql/portfolio_attribution_consistency_checks.sql"
    )

    for check_name in (
        "portfolio_attribution_rows_present",
        "portfolio_attribution_inputs_reference_portfolio_returns",
        "latest_portfolio_attribution_uses_current_returns",
        "portfolio_attribution_window_evidence_aligns",
        "portfolio_attribution_json_shapes_align",
        "portfolio_attribution_weights_sum_to_one",
        "portfolio_covariance_is_symmetric_and_nonnegative_on_diagonal",
        "portfolio_correlation_is_symmetric_and_bounded",
        "portfolio_correlation_null_count_matches_status",
        "portfolio_variance_matches_squared_volatility",
        "portfolio_volatility_contributions_reconcile",
        "portfolio_attribution_calculation_ids_unique",
        "latest_portfolio_attribution_grain_unique",
        "latest_portfolio_attribution_selects_current_version",
        "portfolio_attribution_semantic_rows_match_latest",
        "portfolio_covariance_rows_match_matrix_grain",
        "portfolio_correlation_rows_match_matrix_grain",
        "portfolio_attribution_contribution_rows_match_constituents",
    ):
        assert check_name in consistency_sql

    assert "jsonb_array_elements_text" in consistency_sql
    assert "jsonb_object_keys" in consistency_sql
    assert (
        "POWER(attribution.portfolio_volatility_annualized, 2)"
        in consistency_sql
    )
