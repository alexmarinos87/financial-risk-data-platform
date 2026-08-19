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


def test_postgres_schema_exposes_finance_reporting_contract() -> None:
    schema_sql = _read_sql("sql/postgres_schema.sql")

    assert "CREATE TABLE IF NOT EXISTS risk_platform.symbol_dimension_history" in schema_sql
    assert "CREATE OR REPLACE VIEW risk_platform.current_symbol_dimension" in schema_sql
    assert "CREATE OR REPLACE VIEW risk_platform.finance_risk_semantic_model" in schema_sql
    assert "idx_symbol_dimension_history_current" in schema_sql
    assert "WHERE is_current" in schema_sql


def test_postgres_schema_exposes_daily_risk_serving_contract() -> None:
    schema_sql = _read_sql("sql/postgres_schema.sql")

    for table_name in ("daily_returns", "daily_volatility", "daily_risk_summary"):
        assert f"CREATE TABLE IF NOT EXISTS risk_platform.{table_name}" in schema_sql
        assert "calculation_id TEXT PRIMARY KEY" in schema_sql

    assert "CREATE OR REPLACE VIEW risk_platform.latest_daily_risk_summary" in schema_sql
    assert "CREATE OR REPLACE VIEW risk_platform.daily_risk_semantic_model" in schema_sql
    assert "PARTITION BY" in schema_sql
    assert "volatility_window" in schema_sql
    assert "annualization_days" in schema_sql
    assert "AND risk.source = dim.source" in schema_sql
    assert "history_status <> 'ready'" in schema_sql


def test_latest_daily_risk_view_selects_newest_version_per_parameter_grain() -> None:
    schema_sql = _read_sql("sql/postgres_schema.sql")
    latest_view = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW risk_platform.latest_daily_risk_summary AS",
    )
    semantic_view = _extract_statement(
        schema_sql,
        "CREATE OR REPLACE VIEW risk_platform.daily_risk_semantic_model AS",
    )

    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA risk_platform")
        connection.execute(
            """
            CREATE TABLE risk_platform.daily_risk_summary (
                calculation_id TEXT,
                model_version TEXT,
                source TEXT,
                symbol TEXT,
                source_event_id TEXT,
                ts_event TIMESTAMPTZ,
                ts_ingest TIMESTAMPTZ,
                price_close DOUBLE,
                return_1d DOUBLE,
                volatility_annualized DOUBLE,
                volatility_window INTEGER,
                annualization_days INTEGER,
                historical_var_loss DOUBLE,
                var_confidence DOUBLE,
                var_window INTEGER,
                var_observations INTEGER,
                maximum_drawdown DOUBLE,
                price_observations INTEGER,
                return_observations INTEGER,
                history_status TEXT,
                input_first_event_id TEXT,
                input_last_event_id TEXT,
                var_input_first_event_id TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE risk_platform.current_symbol_dimension (
                symbol_dimension_id BIGINT,
                symbol TEXT,
                source TEXT,
                asset_class TEXT,
                reporting_currency TEXT,
                sector TEXT,
                effective_from TIMESTAMPTZ,
                change_reason TEXT,
                record_hash TEXT
            )
            """
        )

        metric_time = datetime(2026, 1, 4, tzinfo=timezone.utc)
        rows = [
            (
                "calc-old",
                "daily-risk-v2",
                "alpha_vantage",
                "IBM",
                "event-4",
                metric_time,
                datetime(2026, 1, 10, 12, tzinfo=timezone.utc),
                102.0,
                0.03,
                0.2,
                20,
                252,
                0.02,
                0.95,
                60,
                3,
                -0.02,
                4,
                3,
                "partial",
                "event-1",
                "event-4",
                "event-1",
            ),
            (
                "calc-new",
                "daily-risk-v2",
                "alpha_vantage",
                "IBM",
                "event-4",
                metric_time,
                datetime(2026, 2, 1, 12, tzinfo=timezone.utc),
                102.0,
                0.03,
                0.19,
                20,
                252,
                0.018,
                0.95,
                60,
                3,
                -0.02,
                4,
                3,
                "partial",
                "event-1",
                "event-4",
                "event-1",
            ),
            (
                "calc-window-10",
                "daily-risk-v2",
                "alpha_vantage",
                "IBM",
                "event-4",
                metric_time,
                datetime(2026, 1, 10, 12, tzinfo=timezone.utc),
                102.0,
                0.03,
                0.18,
                10,
                252,
                0.02,
                0.95,
                60,
                3,
                -0.02,
                4,
                3,
                "partial",
                "event-1",
                "event-4",
                "event-1",
            ),
        ]
        placeholders = ", ".join(["?"] * len(rows[0]))
        connection.executemany(
            f"INSERT INTO risk_platform.daily_risk_summary VALUES ({placeholders})",
            rows,
        )
        connection.executemany(
            "INSERT INTO risk_platform.current_symbol_dimension VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    1,
                    "IBM",
                    "alpha_vantage",
                    "equity",
                    "USD",
                    "Technology",
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    "initial",
                    "hash-alpha",
                ),
                (
                    2,
                    "IBM",
                    "stooq",
                    "equity",
                    "USD",
                    "Legacy source",
                    datetime(2025, 1, 1, tzinfo=timezone.utc),
                    "initial",
                    "hash-stooq",
                ),
            ],
        )

        connection.execute(latest_view)
        connection.execute(semantic_view)

        assert connection.execute(
            "SELECT COUNT(*) FROM risk_platform.daily_risk_summary"
        ).fetchone() == (3,)
        assert connection.execute(
            """
            SELECT calculation_id, volatility_window
            FROM risk_platform.latest_daily_risk_summary
            ORDER BY volatility_window
            """
        ).fetchall() == [("calc-window-10", 10), ("calc-new", 20)]
        assert connection.execute(
            """
            SELECT sector
            FROM risk_platform.daily_risk_semantic_model
            WHERE calculation_id = 'calc-new'
            """
        ).fetchone() == ("Technology",)


def test_demo_data_seeds_scd_type_2_symbol_history() -> None:
    demo_sql = _read_sql("sql/postgres_demo_data.sql")

    assert "risk_platform.symbol_dimension_history" in demo_sql
    assert "'AAPL'" in demo_sql
    assert "'MSFT'" in demo_sql
    assert "'2025-01-20T10:00:00Z'" in demo_sql
    assert "'sector_alignment'" in demo_sql


def test_consistency_checks_cover_reporting_view_shape() -> None:
    consistency_sql = _read_sql("sql/consistency_checks.sql")

    assert "current_symbol_dimension_rows_expected" in consistency_sql
    assert "finance_reporting_rows_to_latest_risk_symbols" in consistency_sql
    assert "risk_platform.finance_risk_semantic_model" in consistency_sql


def test_daily_consistency_checks_cover_versioned_serving_contract() -> None:
    consistency_sql = _read_sql("sql/daily_risk_consistency_checks.sql")

    for check_name in (
        "daily_summary_rows_present",
        "daily_returns_reference_raw_events",
        "daily_volatility_references_raw_and_return_date",
        "daily_summaries_reference_raw_events",
        "daily_calculation_ids_unique",
        "latest_daily_summary_parameter_grain_unique",
        "latest_daily_summary_selects_current_version",
        "ready_daily_history_has_required_observations",
        "daily_semantic_rows_match_latest_summary",
    ):
        assert check_name in consistency_sql
