from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_PATH = Path("config/symbols.yaml")
DEFAULT_POSTGRES_DSN = "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
SYMBOL_DIMENSION_COLUMNS = (
    "symbol",
    "source",
    "asset_class",
    "reporting_currency",
    "sector",
    "effective_from",
    "effective_to",
    "is_current",
    "change_reason",
    "record_hash",
)
SYMBOL_DIMENSION_CONFLICT_COLUMNS = ("symbol", "source", "effective_from")
ASSET_CLASS_BY_GROUP = {
    "equities": "equity",
    "fx": "fx",
}


@dataclass(frozen=True)
class SymbolVersion:
    symbol: str
    source: str
    asset_class: str
    reporting_currency: str
    sector: str | None
    effective_from: datetime
    change_reason: str

    @property
    def natural_key(self) -> tuple[str, str]:
        return self.symbol, self.source


@dataclass(frozen=True)
class SymbolDimensionRow:
    symbol: str
    source: str
    asset_class: str
    reporting_currency: str
    sector: str | None
    effective_from: datetime
    effective_to: datetime | None
    is_current: bool
    change_reason: str
    record_hash: str

    @property
    def values(self) -> tuple[Any, ...]:
        return (
            self.symbol,
            self.source,
            self.asset_class,
            self.reporting_currency,
            self.sector,
            self.effective_from,
            self.effective_to,
            self.is_current,
            self.change_reason,
            self.record_hash,
        )


def load_symbol_versions(path: Path = DEFAULT_CONFIG_PATH) -> list[SymbolVersion]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)
    return parse_symbol_versions(payload)


def parse_symbol_versions(payload: dict[str, Any]) -> list[SymbolVersion]:
    if not isinstance(payload, dict):
        raise ValueError("Symbol config must be a YAML object")

    symbols = payload.get("symbols")
    if isinstance(symbols, list):
        records = [_coerce_symbol_record(item, group_name=None) for item in symbols]
    elif isinstance(symbols, dict):
        records = []
        for group_name, group_items in symbols.items():
            if not isinstance(group_items, list):
                raise ValueError(f"Symbol group '{group_name}' must be a list")
            records.extend(
                _coerce_symbol_record(item, group_name=str(group_name)) for item in group_items
            )
    else:
        raise ValueError("Symbol config must contain 'symbols' as a list or grouped object")

    return [_build_symbol_version(record) for record in records]


def build_symbol_dimension_rows(versions: list[SymbolVersion]) -> list[SymbolDimensionRow]:
    grouped: dict[tuple[str, str], list[SymbolVersion]] = {}
    for version in versions:
        grouped.setdefault(version.natural_key, []).append(version)

    rows: list[SymbolDimensionRow] = []
    for natural_key in sorted(grouped):
        ordered_versions = sorted(grouped[natural_key], key=lambda item: item.effective_from)
        _validate_unique_effective_dates(natural_key, ordered_versions)

        for index, version in enumerate(ordered_versions):
            next_version = (
                ordered_versions[index + 1] if index + 1 < len(ordered_versions) else None
            )
            rows.append(
                SymbolDimensionRow(
                    symbol=version.symbol,
                    source=version.source,
                    asset_class=version.asset_class,
                    reporting_currency=version.reporting_currency,
                    sector=version.sector,
                    effective_from=version.effective_from,
                    effective_to=next_version.effective_from if next_version is not None else None,
                    is_current=next_version is None,
                    change_reason=version.change_reason,
                    record_hash=build_record_hash(version),
                )
            )

    validate_symbol_dimension_rows(rows)
    return rows


def validate_symbol_dimension_rows(rows: list[SymbolDimensionRow]) -> None:
    grouped: dict[tuple[str, str], list[SymbolDimensionRow]] = {}
    for row in rows:
        if row.effective_to is not None and row.effective_to <= row.effective_from:
            raise ValueError(f"Invalid effective interval for {row.symbol}/{row.source}")
        if row.is_current and row.effective_to is not None:
            raise ValueError(f"Current row must not have effective_to for {row.symbol}/{row.source}")
        if not row.is_current and row.effective_to is None:
            raise ValueError(f"Historical row must have effective_to for {row.symbol}/{row.source}")
        grouped.setdefault((row.symbol, row.source), []).append(row)

    for symbol, source in sorted(grouped):
        ordered_rows = sorted(grouped[(symbol, source)], key=lambda item: item.effective_from)
        current_count = sum(1 for row in ordered_rows if row.is_current)
        if current_count != 1:
            raise ValueError(f"Expected one current row for {symbol}/{source}, got {current_count}")

        for previous, current in zip(ordered_rows, ordered_rows[1:]):
            if previous.effective_to is not None and previous.effective_to > current.effective_from:
                raise ValueError(f"Overlapping effective intervals for {symbol}/{source}")


def build_record_hash(version: SymbolVersion) -> str:
    payload = {
        "asset_class": version.asset_class,
        "reporting_currency": version.reporting_currency,
        "sector": version.sector,
        "source": version.source,
        "symbol": version.symbol,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return digest


def build_symbol_dimension_upsert_sql(schema_name: str = "risk_platform") -> str:
    quoted_columns = ", ".join(_quote_identifier(column) for column in SYMBOL_DIMENSION_COLUMNS)
    placeholders = ", ".join(["%s"] * len(SYMBOL_DIMENSION_COLUMNS))
    conflict_columns = ", ".join(
        _quote_identifier(column) for column in SYMBOL_DIMENSION_CONFLICT_COLUMNS
    )
    update_columns = [
        column
        for column in SYMBOL_DIMENSION_COLUMNS
        if column not in SYMBOL_DIMENSION_CONFLICT_COLUMNS
    ]
    updates = ", ".join(
        f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
        for column in update_columns
    )
    return (
        f"INSERT INTO {_quote_identifier(schema_name)}."
        f"{_quote_identifier('symbol_dimension_history')} "
        f"({quoted_columns}) VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_columns}) DO UPDATE SET {updates}, loaded_at = now()"
    )


def format_symbol_dimension_sql(
    rows: list[SymbolDimensionRow],
    *,
    schema_name: str = "risk_platform",
) -> str:
    validate_symbol_dimension_rows(rows)
    statement_prefix = build_symbol_dimension_upsert_sql(schema_name).split(" VALUES ", maxsplit=1)[
        0
    ]
    update_clause = build_symbol_dimension_upsert_sql(schema_name).split(
        " ON CONFLICT ",
        maxsplit=1,
    )[1]

    statements = [
        f"{statement_prefix} VALUES ({', '.join(_sql_literal(value) for value in row.values)}) "
        f"ON CONFLICT {update_clause};"
        for row in _apply_order(rows)
    ]
    return "\n\n".join(statements) + ("\n" if statements else "")


def apply_symbol_dimension_rows(
    rows: list[SymbolDimensionRow],
    *,
    dsn: str,
    schema_name: str = "risk_platform",
) -> int:
    validate_symbol_dimension_rows(rows)
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL reference-data seeding requires psycopg. "
            "Run `make setup` to install dependencies."
        ) from exc

    statement = build_symbol_dimension_upsert_sql(schema_name)
    ordered_rows = _apply_order(rows)
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(statement, [row.values for row in ordered_rows])
        conn.commit()
    return len(ordered_rows)


def _coerce_symbol_record(item: Any, *, group_name: str | None) -> dict[str, Any]:
    if isinstance(item, str):
        raise ValueError(
            f"Symbol '{item}' must be an object with source, reporting_currency "
            "and effective_from"
        )
    if not isinstance(item, dict):
        raise ValueError("Each symbol entry must be an object")

    if "symbol" in item:
        record = dict(item)
    elif len(item) == 1:
        symbol, attributes = next(iter(item.items()))
        record = {"symbol": symbol}
        record.update(_coerce_attributes(attributes))
    else:
        raise ValueError("Symbol entry must include a 'symbol' field")

    if group_name is not None and "asset_class" not in record:
        record["asset_class"] = ASSET_CLASS_BY_GROUP.get(group_name, group_name.rstrip("s"))
    return record


def _coerce_attributes(attributes: Any) -> dict[str, Any]:
    if isinstance(attributes, dict):
        return dict(attributes)
    if isinstance(attributes, list):
        record: dict[str, Any] = {}
        for item in attributes:
            if not isinstance(item, dict):
                raise ValueError("Symbol attribute lists must contain objects")
            record.update(item)
        return record
    raise ValueError("Symbol attributes must be an object or list of objects")


def _build_symbol_version(record: dict[str, Any]) -> SymbolVersion:
    missing = [
        field
        for field in ("symbol", "source", "asset_class", "reporting_currency", "effective_from")
        if record.get(field) in (None, "")
    ]
    if missing:
        raise ValueError(f"Missing required symbol fields: {missing}")

    symbol = str(record["symbol"]).strip().upper()
    reporting_currency = str(record["reporting_currency"]).strip().upper()
    if len(reporting_currency) != 3:
        raise ValueError(f"reporting_currency must be a 3-character code for {symbol}")

    sector = record.get("sector")
    return SymbolVersion(
        symbol=symbol,
        source=str(record["source"]).strip().lower(),
        asset_class=str(record["asset_class"]).strip().lower(),
        reporting_currency=reporting_currency,
        sector=None if sector in (None, "") else str(sector).strip().lower(),
        effective_from=_parse_effective_timestamp(record["effective_from"]),
        change_reason=str(record.get("change_reason") or "initial_load").strip().lower(),
    )


def _parse_effective_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        parsed = _parse_datetime_text(text)
    else:
        raise ValueError("effective_from must be a date or timestamp")

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_datetime_text(value: str) -> datetime:
    for date_format in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, date_format).replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("effective_from must be ISO-8601 or DD/MM/YYYY") from exc


def _validate_unique_effective_dates(
    natural_key: tuple[str, str],
    versions: list[SymbolVersion],
) -> None:
    seen: set[datetime] = set()
    for version in versions:
        if version.effective_from in seen:
            symbol, source = natural_key
            raise ValueError(f"Duplicate effective_from for {symbol}/{source}")
        seen.add(version.effective_from)


def _apply_order(rows: list[SymbolDimensionRow]) -> list[SymbolDimensionRow]:
    return sorted(
        rows,
        key=lambda row: (row.symbol, row.source, row.is_current, row.effective_from),
    )


def _quote_identifier(value: str) -> str:
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return f"'{_format_timestamp(value)}'"
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


def _format_timestamp(value: datetime) -> str:
    ts = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate or apply deterministic SCD Type 2 symbol reference rows."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to symbol reference-data YAML.",
    )
    parser.add_argument(
        "--schema",
        default="risk_platform",
        help="Target PostgreSQL schema.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
        help="PostgreSQL connection string used with --apply.",
    )
    parser.add_argument(
        "--output-sql",
        type=Path,
        help="Optional path for generated upsert SQL. Omit to print SQL.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply rows to PostgreSQL instead of only generating SQL.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    versions = load_symbol_versions(args.config)
    rows = build_symbol_dimension_rows(versions)

    if args.apply:
        row_count = apply_symbol_dimension_rows(rows, dsn=args.dsn, schema_name=args.schema)
        print(f"Applied {row_count} symbol dimension rows to {args.schema}.")
        return

    sql = format_symbol_dimension_sql(rows, schema_name=args.schema)
    if args.output_sql is not None:
        args.output_sql.parent.mkdir(parents=True, exist_ok=True)
        args.output_sql.write_text(sql, encoding="utf-8")
        print(f"Wrote {len(rows)} symbol dimension rows to {args.output_sql}.")
        return

    print(sql, end="")


if __name__ == "__main__":
    main()
