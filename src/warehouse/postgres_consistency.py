from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_POSTGRES_DSN = (
    "postgresql://risk_user:risk_password@localhost:5433/risk_platform"
)
DEFAULT_CHECK_PATHS = (
    Path("sql/consistency_checks.sql"),
    Path("sql/daily_risk_consistency_checks.sql"),
    Path("sql/portfolio_risk_consistency_checks.sql"),
    Path("sql/portfolio_attribution_consistency_checks.sql"),
    Path("sql/portfolio_risk_limits_consistency_checks.sql"),
)
MAX_CHECK_BYTES = 2_000_000
VALID_STATUSES = frozenset({"pass", "fail"})
REQUIRED_COLUMNS = ("check_name", "expected", "actual", "status")


@dataclass(frozen=True, slots=True)
class ConsistencyResult:
    source: str
    check_name: str
    expected: str
    actual: str
    status: str


def validate_check_path(path: Path, *, sql_root: Path = Path("sql")) -> Path:
    root = sql_root.resolve()
    if path.is_symlink():
        raise ValueError("PostgreSQL consistency paths must not be symbolic links")
    resolved = path.resolve()
    if not resolved.is_relative_to(root):
        raise ValueError("PostgreSQL consistency paths must stay under sql/")
    if resolved.suffix.lower() != ".sql" or not resolved.is_file():
        raise ValueError("PostgreSQL consistency path must be a regular SQL file")
    if resolved.stat().st_size > MAX_CHECK_BYTES:
        raise ValueError("PostgreSQL consistency SQL exceeds the size limit")
    return resolved


def normalise_check_rows(
    *,
    source: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> tuple[ConsistencyResult, ...]:
    positions = {name: index for index, name in enumerate(columns)}
    missing = [name for name in REQUIRED_COLUMNS if name not in positions]
    if missing:
        raise ValueError(
            "PostgreSQL consistency query is missing columns: " + ", ".join(missing)
        )

    results: list[ConsistencyResult] = []
    for row in rows:
        try:
            status = str(row[positions["status"]]).strip().lower()
            result = ConsistencyResult(
                source=source,
                check_name=str(row[positions["check_name"]]),
                expected=str(row[positions["expected"]]),
                actual=str(row[positions["actual"]]),
                status=status,
            )
        except (IndexError, TypeError):
            raise ValueError(
                "PostgreSQL consistency query returned an incompatible row"
            ) from None
        if not result.check_name.strip():
            raise ValueError("PostgreSQL consistency check names must not be empty")
        if status not in VALID_STATUSES:
            raise ValueError(
                "PostgreSQL consistency statuses must be either pass or fail"
            )
        results.append(result)

    if not results:
        raise ValueError("PostgreSQL consistency query returned no checks")
    return tuple(results)


def run_consistency_checks(
    *,
    dsn: str,
    check_paths: Sequence[Path],
) -> tuple[ConsistencyResult, ...]:
    if not check_paths:
        raise ValueError("At least one PostgreSQL consistency file is required")
    validated_paths = [validate_check_path(path) for path in check_paths]

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("PostgreSQL consistency requires psycopg") from exc

    results: list[ConsistencyResult] = []
    with psycopg.connect(dsn) as connection:
        connection.read_only = True
        with connection.cursor() as cursor:
            for path in validated_paths:
                sql_text = path.read_text(encoding="utf-8")
                cursor.execute(sql_text)
                if cursor.description is None:
                    raise ValueError(
                        f"PostgreSQL consistency query returned no result set: {path.name}"
                    )
                columns = tuple(column.name for column in cursor.description)
                results.extend(
                    normalise_check_rows(
                        source=path.name,
                        columns=columns,
                        rows=cursor.fetchall(),
                    )
                )
    return tuple(results)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute repository reconciliation SQL against PostgreSQL and fail "
            "when any returned status is not pass."
        )
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument(
        "--check",
        type=Path,
        action="append",
        dest="checks",
        help="SQL reconciliation file under sql/. May be supplied repeatedly.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    check_paths = tuple(args.checks or DEFAULT_CHECK_PATHS)
    try:
        results = run_consistency_checks(dsn=args.dsn, check_paths=check_paths)
    except Exception as exc:
        print(f"PostgreSQL consistency execution failed: {exc}", file=sys.stderr)
        return 1

    failures = [result for result in results if result.status != "pass"]
    for result in results:
        print(
            f"{result.status.upper()} {result.source}:{result.check_name} "
            f"expected={result.expected!r} actual={result.actual!r}"
        )
    if failures:
        print(
            f"PostgreSQL consistency failed: {len(failures)} of "
            f"{len(results)} checks failed",
            file=sys.stderr,
        )
        return 1
    print(f"PostgreSQL consistency passed: {len(results)} checks")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
