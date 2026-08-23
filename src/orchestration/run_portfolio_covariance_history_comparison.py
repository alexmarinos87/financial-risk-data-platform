from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_attribution_history import (
    MAX_HISTORY_SNAPSHOTS,
    PortfolioAttributionHistoryOutput,
    build_portfolio_attribution_history,
)
from ..analytics.portfolio_attribution_ewma_history import (
    build_portfolio_ewma_attribution_history,
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
    _calendar_date,
    _positive_integer,
    _require_datasets,
    load_portfolio_return_records,
)
from .run_portfolio_attribution_history import _max_snapshots

Reader = Callable[..., list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
DefinitionLoader = Callable[[Path, str], PortfolioDefinition]

ALIGNMENT_FIELDS = (
    "portfolio_id",
    "base_currency",
    "definition_fingerprint",
    "weighting_method",
    "covariance_window",
    "window_start",
    "window_end",
    "window_observations",
    "annualization_days",
    "ts_event",
    "input_calculation_ids_json",
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build aligned rolling sample and EWMA covariance-attribution history "
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
    parser.add_argument("--covariance-window", type=_positive_integer, default=20)
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


def _aligned_pairs(
    sample_history: PortfolioAttributionHistoryOutput,
    ewma_history: PortfolioAttributionHistoryOutput,
) -> tuple[tuple[dict[str, Any], dict[str, Any]], ...]:
    if len(sample_history.snapshots) != len(ewma_history.snapshots):
        raise ValidationError(
            "sample and EWMA attribution histories contain different row counts"
        )

    pairs: list[tuple[dict[str, Any], dict[str, Any]]] = []
    for sample, ewma in zip(
        sample_history.snapshots,
        ewma_history.snapshots,
        strict=True,
    ):
        if any(sample[field] != ewma[field] for field in ALIGNMENT_FIELDS):
            raise ValidationError(
                "sample and EWMA attribution histories are not input-aligned"
            )
        if sample["calculation_id"] == ewma["calculation_id"]:
            raise ValidationError(
                "sample and EWMA attribution calculations must have distinct identities"
            )
        pairs.append((sample, ewma))
    if not pairs:
        raise ValidationError("portfolio covariance history comparison is empty")
    return tuple(pairs)


def _publish_pairs(
    pairs: Sequence[tuple[dict[str, Any], dict[str, Any]]],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for sample, ewma in pairs:
        for snapshot in (sample, ewma):
            try:
                result = writer(
                    [snapshot],
                    kind="curated",
                    dataset=OUTPUT_DATASET,
                    storage_config=storage_config,
                )
            except Exception:
                raise StorageError(
                    "Portfolio covariance history publication failed; rerun is safe"
                ) from None
            if type(result) is not int or result not in {0, 1}:
                raise StorageError(
                    "Portfolio covariance history publication returned an invalid result"
                )
            written += result
    return written


def _comparison_evidence(
    sample: Mapping[str, Any],
    ewma: Mapping[str, Any],
) -> dict[str, Any]:
    sample_volatility = float(sample["portfolio_volatility_annualized"])
    ewma_volatility = float(ewma["portfolio_volatility_annualized"])
    if not math.isfinite(sample_volatility) or not math.isfinite(ewma_volatility):
        raise ValidationError("portfolio covariance history contains non-finite risk")
    difference = ewma_volatility - sample_volatility
    ratio = ewma_volatility / sample_volatility if sample_volatility > 0 else None
    if ratio is not None and not math.isfinite(ratio):
        raise ValidationError("portfolio covariance history ratio is non-finite")
    higher = (
        "equal"
        if math.isclose(difference, 0.0, rel_tol=0.0, abs_tol=1e-15)
        else "ewma"
        if difference > 0
        else "sample"
    )
    return {
        "date": sample["ts_event"].date().isoformat(),
        "sample_calculation_id": sample["calculation_id"],
        "ewma_calculation_id": ewma["calculation_id"],
        "sample_volatility_annualized": sample_volatility,
        "ewma_volatility_annualized": ewma_volatility,
        "ewma_minus_sample_volatility": difference,
        "ewma_to_sample_volatility_ratio": ratio,
        "higher_volatility_model": higher,
    }


def _aggregate_comparisons(
    pairs: Sequence[tuple[dict[str, Any], dict[str, Any]]],
) -> dict[str, Any]:
    comparisons = [
        _comparison_evidence(sample, ewma) for sample, ewma in pairs
    ]
    latest = comparisons[-1]
    maximum = max(
        comparisons,
        key=lambda comparison: (
            abs(float(comparison["ewma_minus_sample_volatility"])),
            str(comparison["date"]),
        ),
    )
    return {
        "paired_snapshot_dates": len(comparisons),
        "ewma_higher_dates": sum(
            comparison["higher_volatility_model"] == "ewma"
            for comparison in comparisons
        ),
        "sample_higher_dates": sum(
            comparison["higher_volatility_model"] == "sample"
            for comparison in comparisons
        ),
        "equal_dates": sum(
            comparison["higher_volatility_model"] == "equal"
            for comparison in comparisons
        ),
        "latest": latest,
        "maximum_absolute_difference": {
            **maximum,
            "absolute_volatility_difference": abs(
                float(maximum["ewma_minus_sample_volatility"])
            ),
        },
    }


def run_portfolio_covariance_history_comparison(
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
        definition = selected_definition_loader(portfolio_config_path, portfolio_id)
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Portfolio configuration is invalid") from None

    selected_reader = reader or load_portfolio_return_records
    try:
        records = tuple(
            selected_reader(
                storage_config=storage_config,
                portfolio_id=definition.portfolio_id,
                definition_fingerprint=definition.fingerprint,
                end_date=end_date,
            )
        )
    except (StorageError, ValidationError):
        raise
    except Exception:
        raise StorageError("Unable to read local portfolio returns") from None

    sample_history = build_portfolio_attribution_history(
        records,
        definition=definition,
        covariance_window=covariance_window,
        start_date=start_date,
        end_date=end_date,
        max_snapshots=max_snapshots,
    )
    ewma_history = build_portfolio_ewma_attribution_history(
        records,
        definition=definition,
        covariance_window=covariance_window,
        start_date=start_date,
        end_date=end_date,
        max_snapshots=max_snapshots,
    )
    pairs = _aligned_pairs(sample_history, ewma_history)

    selected_writer = writer or write_records
    written = _publish_pairs(
        pairs,
        storage_config=storage_config,
        writer=selected_writer,
    )
    selected = 2 * len(pairs)
    return {
        "run_id": str(uuid4()),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "selection": {
            "start_date": start_date.isoformat() if start_date is not None else None,
            "end_date": end_date.isoformat(),
            "covariance_window": covariance_window,
            "max_snapshots_per_model": max_snapshots,
            "first_snapshot_date": pairs[0][0]["ts_event"].date().isoformat(),
            "last_snapshot_date": pairs[-1][0]["ts_event"].date().isoformat(),
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": selected,
                "records_written": written,
                "records_already_present": selected - written,
                "sample_records_selected": len(pairs),
                "ewma_records_selected": len(pairs),
            }
        },
        "sample_diagnostics": dict(sample_history.diagnostics),
        "ewma_diagnostics": dict(ewma_history.diagnostics),
        "comparison": _aggregate_comparisons(pairs),
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError(
            "Unable to write the portfolio covariance history summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_covariance_history_comparison(
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
            "Portfolio covariance history comparison failed: configuration, "
            "input data, or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio covariance history comparison failed: local storage "
            "operation failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio covariance history comparison failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
