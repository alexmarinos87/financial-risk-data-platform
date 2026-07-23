from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from scripts.seed_reference_data import (
    SymbolDimensionRow,
    build_record_hash,
    build_symbol_dimension_rows,
    build_symbol_dimension_upsert_sql,
    find_symbol_dimension_row_as_of,
    format_symbol_dimension_sql,
    load_symbol_versions,
    parse_symbol_versions,
    validate_symbol_dimension_rows,
)


def _sample_config() -> dict:
    return {
        "symbols": {
            "equities": [
                {
                    "symbol": "aapl",
                    "source": "STOOQ",
                    "asset_class": "Equity",
                    "reporting_currency": "usd",
                    "sector": "technology",
                    "effective_from": "2024-01-01T00:00:00Z",
                    "change_reason": "initial_load",
                },
                {
                    "symbol": "AAPL",
                    "source": "stooq",
                    "asset_class": "equity",
                    "reporting_currency": "USD",
                    "sector": "information_technology",
                    "effective_from": "2025-01-20T10:00:00Z",
                    "change_reason": "sector_alignment",
                },
                {
                    "symbol": "MSFT",
                    "source": "stooq",
                    "asset_class": "equity",
                    "reporting_currency": "USD",
                    "sector": "information_technology",
                    "effective_from": "2025-01-01",
                },
            ],
            "fx": [
                {
                    "symbol": "EURUSD",
                    "source": "stooq",
                    "reporting_currency": "USD",
                    "effective_from": "2025-01-01T00:00:00Z",
                }
            ],
        }
    }


def test_load_symbol_versions_normalises_config_records(tmp_path: Path) -> None:
    path = tmp_path / "symbols.yaml"
    path.write_text(yaml.safe_dump(_sample_config()), encoding="utf-8")

    versions = load_symbol_versions(path)

    assert versions[0].symbol == "AAPL"
    assert versions[0].source == "stooq"
    assert versions[0].asset_class == "equity"
    assert versions[0].reporting_currency == "USD"
    assert versions[2].effective_from == datetime(2025, 1, 1, tzinfo=timezone.utc)
    assert versions[3].asset_class == "fx"
    assert versions[3].sector is None


def test_build_symbol_dimension_rows_closes_previous_versions() -> None:
    rows = build_symbol_dimension_rows(parse_symbol_versions(_sample_config()))
    aapl_rows = [row for row in rows if row.symbol == "AAPL"]

    assert len(aapl_rows) == 2
    assert aapl_rows[0].effective_to == aapl_rows[1].effective_from
    assert aapl_rows[0].is_current is False
    assert aapl_rows[1].effective_to is None
    assert aapl_rows[1].is_current is True
    assert aapl_rows[0].record_hash != aapl_rows[1].record_hash


def test_build_symbol_dimension_rows_marks_one_current_row_per_symbol_source() -> None:
    rows = build_symbol_dimension_rows(parse_symbol_versions(_sample_config()))
    current_by_key: dict[tuple[str, str], int] = {}

    for row in rows:
        if row.is_current:
            current_by_key[(row.symbol, row.source)] = (
                current_by_key.get((row.symbol, row.source), 0) + 1
            )

    assert current_by_key == {
        ("AAPL", "stooq"): 1,
        ("EURUSD", "stooq"): 1,
        ("MSFT", "stooq"): 1,
    }


@pytest.mark.parametrize(
    ("as_of", "expected_sector"),
    [
        (datetime(2025, 1, 20, 9, 59, 59, tzinfo=timezone.utc), "technology"),
        (
            datetime(2025, 1, 20, 10, 0, tzinfo=timezone.utc),
            "information_technology",
        ),
    ],
)
def test_find_symbol_dimension_row_as_of_uses_half_open_intervals(
    as_of: datetime,
    expected_sector: str,
) -> None:
    rows = build_symbol_dimension_rows(parse_symbol_versions(_sample_config()))

    row = find_symbol_dimension_row_as_of(
        rows,
        symbol=" aapl ",
        source="STOOQ",
        as_of=as_of,
    )

    assert row is not None
    assert row.sector == expected_sector


def test_find_symbol_dimension_row_as_of_returns_none_outside_history() -> None:
    rows = build_symbol_dimension_rows(parse_symbol_versions(_sample_config()))

    row = find_symbol_dimension_row_as_of(
        rows,
        symbol="AAPL",
        source="stooq",
        as_of=datetime(2023, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    )

    assert row is None


def test_duplicate_effective_dates_are_rejected() -> None:
    config = _sample_config()
    config["symbols"]["equities"][1]["effective_from"] = "2024-01-01T00:00:00Z"
    versions = parse_symbol_versions(config)

    with pytest.raises(ValueError, match="Duplicate effective_from for AAPL/stooq"):
        build_symbol_dimension_rows(versions)


def test_overlapping_effective_intervals_are_rejected() -> None:
    rows = [
        SymbolDimensionRow(
            symbol="AAPL",
            source="stooq",
            asset_class="equity",
            reporting_currency="USD",
            sector="technology",
            effective_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            effective_to=datetime(2025, 3, 1, tzinfo=timezone.utc),
            is_current=False,
            change_reason="initial_load",
            record_hash="first",
        ),
        SymbolDimensionRow(
            symbol="AAPL",
            source="stooq",
            asset_class="equity",
            reporting_currency="USD",
            sector="information_technology",
            effective_from=datetime(2025, 2, 1, tzinfo=timezone.utc),
            effective_to=None,
            is_current=True,
            change_reason="sector_alignment",
            record_hash="second",
        ),
    ]

    with pytest.raises(ValueError, match="Overlapping effective intervals for AAPL/stooq"):
        validate_symbol_dimension_rows(rows)


def test_record_hash_is_deterministic_for_dimension_attributes() -> None:
    first = parse_symbol_versions(_sample_config())[0]
    second = parse_symbol_versions(_sample_config())[0]

    assert build_record_hash(first) == build_record_hash(second)


def test_existing_symbol_mapping_shape_is_supported() -> None:
    versions = parse_symbol_versions(
        {
            "symbols": {
                "equities": [
                    {
                        "AAPL": [
                            {"source": "test"},
                            {"asset_class": "equity"},
                            {"reporting_currency": "usd"},
                            {"sector": "information_technology"},
                            {"effective_from": "21/07/2026"},
                        ]
                    }
                ]
            }
        }
    )

    assert versions[0].symbol == "AAPL"
    assert versions[0].source == "test"
    assert versions[0].effective_from == datetime(2026, 7, 21, tzinfo=timezone.utc)


def test_generated_sql_uses_idempotent_upsert_contract() -> None:
    rows = build_symbol_dimension_rows(parse_symbol_versions(_sample_config()))
    sql = format_symbol_dimension_sql(rows)

    assert 'INSERT INTO "risk_platform"."symbol_dimension_history"' in sql
    assert 'ON CONFLICT ("symbol", "source", "effective_from") DO UPDATE' in sql
    assert "'AAPL'" in sql
    assert "'sector_alignment'" in sql
    assert "NULL" in sql


def test_build_symbol_dimension_upsert_sql_has_expected_conflict_key() -> None:
    sql = build_symbol_dimension_upsert_sql()

    assert '("symbol", "source", "effective_from")' in sql
    assert '"effective_to" = EXCLUDED."effective_to"' in sql
    assert "loaded_at = now()" in sql
