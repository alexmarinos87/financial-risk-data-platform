from __future__ import annotations

import argparse
import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from ..common.exceptions import DataQualityError
from .run_pipeline import run_pipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic day-by-day pipeline backfills.")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD format.",
    )
    parser.add_argument("--input", type=Path, help="Optional path to a JSON list of market events.")
    parser.add_argument(
        "--thresholds",
        type=Path,
        default=Path("config/risk_thresholds.yaml"),
        help="Path to risk thresholds YAML.",
    )
    parser.add_argument(
        "--contracts",
        type=Path,
        default=Path("config/data_contracts.yaml"),
        help="Path to data contracts YAML.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data_lake"),
        help="Root path for local bronze/silver/gold datasets.",
    )
    parser.add_argument(
        "--allow-dq-breach",
        action="store_true",
        help="Continue and exit successfully even when DQ thresholds are breached.",
    )
    parser.add_argument(
        "--summary-json",
        type=Path,
        help="Optional path to write the backfill summary as JSON.",
    )
    return parser.parse_args()


def _date_range(start: date, end: date) -> list[date]:
    current = start
    days: list[date] = []
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def run_backfill(
    start_date: str,
    end_date: str,
    *,
    input_path: Path | None = None,
    thresholds_path: Path = Path("config/risk_thresholds.yaml"),
    contracts_path: Path = Path("config/data_contracts.yaml"),
    output_root: Path = Path("data_lake"),
    fail_on_dq: bool = True,
) -> dict[str, Any]:
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    if start > end:
        raise ValueError("start_date must be less than or equal to end_date")

    summaries: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    for day in _date_range(start, end):
        run_id = f"backfill-{day.isoformat()}"
        try:
            summary = run_pipeline(
                input_path=input_path,
                thresholds_path=thresholds_path,
                contracts_path=contracts_path,
                output_root=output_root,
                run_id=run_id,
                fail_on_dq=fail_on_dq,
                late_seconds=300,
                window_minutes=5,
                vol_window=5,
            )
            summary["backfill_date"] = day.isoformat()
            summaries.append(summary)
        except DataQualityError as exc:
            failures.append({"backfill_date": day.isoformat(), "error": str(exc)})
            if fail_on_dq:
                break

    manifest = {
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "runs_requested": len(_date_range(start, end)),
        "runs_completed": len(summaries),
        "runs_failed": len(failures),
        "run_ids": [summary["run_id"] for summary in summaries],
        "failures": failures,
    }

    manifest_path = output_root / "_runs" / f"backfill-{start.isoformat()}-{end.isoformat()}.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    return manifest


def main() -> None:
    args = _parse_args()
    manifest = run_backfill(
        start_date=args.start_date,
        end_date=args.end_date,
        input_path=args.input,
        thresholds_path=args.thresholds,
        contracts_path=args.contracts,
        output_root=args.output_root,
        fail_on_dq=not args.allow_dq_breach,
    )

    if args.summary_json is not None:
        args.summary_json.write_text(
            json.dumps(manifest, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    print("Backfill summary")
    print(f"Date range: {manifest['start_date']} -> {manifest['end_date']}")
    print(f"Runs requested: {manifest['runs_requested']}")
    print(f"Runs completed: {manifest['runs_completed']}")
    print(f"Runs failed: {manifest['runs_failed']}")


if __name__ == "__main__":
    main()
