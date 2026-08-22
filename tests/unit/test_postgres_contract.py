from __future__ import annotations

from datetime import timezone
from pathlib import Path

import pytest

from src.orchestration.build_postgres_contract_fixture import (
    CONTRACT_OBSERVATIONS,
    CONTRACT_SYMBOLS,
    EXPECTED_ATTRIBUTION_SNAPSHOTS,
    EXPECTED_DAILY_RETURNS_PER_SYMBOL,
    EXPECTED_LIMIT_EVALUATIONS,
    build_contract_daily_events,
)
from src.warehouse.postgres_consistency import (
    normalise_check_rows,
    validate_check_path,
)


def test_contract_daily_events_are_deterministic_and_canonical() -> None:
    first = build_contract_daily_events()
    second = build_contract_daily_events()

    assert first == second
    assert len(first) == CONTRACT_OBSERVATIONS * len(CONTRACT_SYMBOLS)
    assert len({record["event_id"] for record in first}) == len(first)
    assert {record["symbol"] for record in first} == set(CONTRACT_SYMBOLS)
    assert all(record["source"] == "alpha_vantage" for record in first)
    assert all(record["price"] > 0 for record in first)
    assert all(record["volume"] >= 0 for record in first)
    assert all(record["ts_event"].tzinfo is timezone.utc for record in first)
    assert all(record["ts_ingest"] > record["ts_event"] for record in first)


def test_contract_expected_counts_cover_rolling_attribution_and_limits() -> None:
    assert EXPECTED_DAILY_RETURNS_PER_SYMBOL == CONTRACT_OBSERVATIONS - 1
    assert EXPECTED_ATTRIBUTION_SNAPSHOTS == 6
    assert EXPECTED_LIMIT_EVALUATIONS == 12


def test_normalise_check_rows_requires_named_contract_columns() -> None:
    results = normalise_check_rows(
        source="example.sql",
        columns=("check_name", "expected", "actual", "status"),
        rows=[("row_count", "2", "2", "PASS")],
    )

    assert results[0].status == "pass"
    assert results[0].check_name == "row_count"

    with pytest.raises(ValueError, match="missing columns"):
        normalise_check_rows(
            source="example.sql",
            columns=("check_name", "status"),
            rows=[("row_count", "pass")],
        )

    with pytest.raises(ValueError, match="either pass or fail"):
        normalise_check_rows(
            source="example.sql",
            columns=("check_name", "expected", "actual", "status"),
            rows=[("row_count", "2", "2", "unknown")],
        )


def test_validate_check_path_is_bounded_to_sql_directory(tmp_path: Path) -> None:
    sql_root = tmp_path / "sql"
    sql_root.mkdir()
    valid = sql_root / "checks.sql"
    valid.write_text("SELECT 1;\n", encoding="utf-8")
    outside = tmp_path / "outside.sql"
    outside.write_text("SELECT 1;\n", encoding="utf-8")

    assert validate_check_path(valid, sql_root=sql_root) == valid.resolve()
    with pytest.raises(ValueError, match="stay under sql"):
        validate_check_path(outside, sql_root=sql_root)


def test_makefile_waits_for_final_postgres_process() -> None:
    makefile = Path("Makefile").read_text(encoding="utf-8")

    assert 'test "$$(cat /proc/1/comm)" = postgres' in makefile
    assert "pg_isready -U risk_user -d risk_platform" in makefile
