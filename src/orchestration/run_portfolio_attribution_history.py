from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_attribution_history import (
    MAX_HISTORY_SNAPSHOTS,
    PortfolioAttributionHistoryOutput,
    build_portfolio_attribution_history,
)
from ..analytics.portfolio_risk import (
    PortfolioDefinition,
    load_portfolio_definition,
)
from ..common.exceptions import StorageError, ValidationError
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config
from .run_portfolio_attribution import (
    OUTPUT_DATASET,
    _require_datasets,
    load_portfolio_return_records,
)

Reader = Callable[..., list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
DefinitionLoader = Callable[[Path, str], PortfolioDefinition]


def _calendar_date(value: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        ) from exc
    if value != parsed.isoformat():
        raise argparse.ArgumentTypeError(
            "must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _integer_at_least(value: str, minimum: int, label: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{label} must be an integer of at least {minimum}"
        ) from exc
    if parsed < minimum:
        raise argparse.ArgumentTypeError(
            f"{label} must be an integer of at least {minimum}"
        )
    return parsed


def _covariance_window(value: str) -> int:
    return _integer_at_least(value, 2, "covariance_window")


def _max_snapshots(value: str) -> int:
    parsed = _integer_at_least(value, 1, "max_snapshots")
    if parsed > MAX_HISTORY_SNAPSHOTS:
        raise argparse.ArgumentTypeError(
            f"max_snapshots must not exceed {MAX_HISTORY_SNAPSHOTS}"
        )
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build rolling covariance, correlation and component-risk history "
            "from local portfolio-return parquet."
        )
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--covariance-window",
        type=_covariance_window,
        default=20,
    )
    parser.add_argument(
        "--max-snapshots",
        type=_max_snapshots,
        default=MAX_HISTORY_SNAPSHOTS,
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _publish_snapshots(
    snapshots: tuple[dict[str, Any], ...],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for snapshot in snapshots:
        try:
            result = writer(
                [snapshot],
                kind="curated",
                dataset=OUTPUT_DATASET,
                storage_config=storage_config,
            )
        except Exception:
            raise StorageError(
                "Portfolio attribution history publication failed; rerun is safe"
            ) from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError(
                "Portfolio attribution history publication returned an invalid result"
            )
        written += result
    return written


def run_portfolio_attribution_history(
    *,
    portfolio_id: str,
    portfolio_config_path: Path,
    end_date: date,
    covariance_window: int,
    storage_config_path: Path,
    start_date: date | None = None,
    max_snapshots: int = MAX_HISTORY_SNAPSHOTS,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    definition_loader: DefinitionLoader | None = None,
) -> dict[str, Any]:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")

    selected_storage_loader = storage_config_loader or load_storage_config
    try:
        storage_config = selected_storage_loader(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None
    if not isinstance(storage_config, dict):
        raise StorageError("Storage configuration is invalid")
    _require_datasets(storage_config)

    selected_definition_loader = definition_loader or load_portfolio_definition
    try:
        definition = selected_definition_loader(
            portfolio_config_path,
            portfolio_id,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Portfolio configuration is invalid") from None

    selected_reader = reader or load_portfolio_return_records
    try:
        records = selected_reader(
            storage_config=storage_config,
            portfolio_id=definition.portfolio_id,
            definition_fingerprint=definition.fingerprint,
            end_date=end_date,
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local portfolio returns") from None

    output: PortfolioAttributionHistoryOutput = (
        build_portfolio_attribution_history(
            records,
            definition=definition,
            covariance_window=covariance_window,
            start_date=start_date,
            end_date=end_date,
            max_snapshots=max_snapshots,
        )
    )
    selected_writer = writer or write_records
    written = _publish_snapshots(
        output.snapshots,
        storage_config=storage_config,
        writer=selected_writer,
    )

    latest = output.snapshots[-1]
    components = json.loads(
        latest["component_volatility_contribution_json"]
    )
    largest_component = max(
        sorted(components),
        key=lambda key: abs(float(components[key])),
    )
    selected = len(output.snapshots)
    return {
        "run_id": str(uuid4()),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "selection": {
            "start_date": (
                start_date.isoformat() if start_date is not None else None
            ),
            "end_date": end_date.isoformat(),
            **dict(output.diagnostics),
        },
        "parameters": {
            "covariance_window": covariance_window,
            "max_snapshots": max_snapshots,
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": selected,
                "records_written": written,
                "records_already_present": selected - written,
            }
        },
        "latest_metrics": {
            "ts_event": latest["ts_event"].isoformat(),
            "portfolio_variance_annualized": latest[
                "portfolio_variance_annualized"
            ],
            "portfolio_volatility_annualized": latest[
                "portfolio_volatility_annualized"
            ],
            "volatility_status": latest["volatility_status"],
            "correlation_status": latest["correlation_status"],
            "largest_absolute_component": largest_component,
            "largest_absolute_component_contribution": components[
                largest_component
            ],
            "calculation_id": latest["calculation_id"],
        },
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary_path.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)
    except OSError:
        temporary_path.unlink(missing_ok=True)
        raise StorageError(
            "Unable to write the portfolio attribution history summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_attribution_history(
            portfolio_id=args.portfolio_id,
            portfolio_config_path=args.portfolio_config,
            start_date=args.start_date,
            end_date=args.end_date,
            covariance_window=args.covariance_window,
            max_snapshots=args.max_snapshots,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Portfolio attribution history failed: configuration, input data "
            "or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio attribution history failed: local storage operation "
            "failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio attribution history failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
