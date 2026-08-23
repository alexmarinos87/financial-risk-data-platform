from pathlib import Path


def test_market_freshness_schema_exposes_current_and_exception_views() -> None:
    sql = Path("sql/market_freshness_schema.sql").read_text(
        encoding="utf-8"
    )

    for required in (
        "risk_platform.daily_market_freshness",
        "risk_platform.latest_daily_market_freshness",
        "risk_platform.daily_market_freshness_exceptions",
        "risk_platform.current_daily_market_freshness",
        "freshness_status IN ('current', 'gap_detected', 'stale')",
        "jsonb_array_length(missing_sessions_json)",
        "ORDER BY ts_ingest DESC, calculation_id DESC",
    ):
        assert required in sql


def test_market_freshness_reconciliation_covers_identity_and_status() -> None:
    sql = Path(
        "sql/market_freshness_consistency_checks.sql"
    ).read_text(encoding="utf-8")

    for required in (
        "daily_market_freshness_json_and_counts_valid",
        "daily_market_freshness_status_valid",
        "daily_market_freshness_calculation_ids_unique",
        "latest_daily_market_freshness_grain_unique",
        "latest_daily_market_freshness_selects_current_version",
        "current_daily_market_freshness_selects_latest_as_of",
        "daily_market_freshness_exception_rows_match",
    ):
        assert required in sql
