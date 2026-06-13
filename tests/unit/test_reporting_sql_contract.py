from __future__ import annotations

from pathlib import Path


def _read_sql(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_postgres_schema_exposes_finance_reporting_contract() -> None:
    schema_sql = _read_sql("sql/postgres_schema.sql")

    assert "CREATE TABLE IF NOT EXISTS risk_platform.symbol_dimension_history" in schema_sql
    assert "CREATE OR REPLACE VIEW risk_platform.current_symbol_dimension" in schema_sql
    assert "CREATE OR REPLACE VIEW risk_platform.finance_risk_semantic_model" in schema_sql
    assert "idx_symbol_dimension_history_current" in schema_sql
    assert "WHERE is_current" in schema_sql


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
