from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_attribution import (
    COVARIANCE_METHOD as SAMPLE_COVARIANCE_METHOD,
    MODEL_VERSION as SAMPLE_MODEL_VERSION,
    PortfolioAttributionOutput,
    build_portfolio_attribution,
)
from ..analytics.portfolio_attribution_ewma import (
    COVARIANCE_METHOD as EWMA_COVARIANCE_METHOD,
    EWMA_DECAY,
    MODEL_VERSION as EWMA_MODEL_VERSION,
    build_portfolio_ewma_attribution,
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

Reader = Callable[..., list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
DefinitionLoader = Callable[[Path, str], PortfolioDefinition]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Calculate and persist aligned sample and EWMA portfolio "
            "covariance attribution for one latest complete window."
        )
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument("--covariance-window", type=_positive_integer, default=20)
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _publish_snapshots(
    outputs: Sequence[PortfolioAttributionOutput],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for output in outputs:
        try:
            result = writer(
                [output.snapshot],
                kind="curated",
                dataset=OUTPUT_DATASET,
                storage_config=storage_config,
            )
        except Exception:
            raise StorageError(
                "Portfolio covariance comparison publication failed; rerun is safe"
            ) from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError(
                "Portfolio covariance comparison publication returned an invalid result"
            )
        written += result
    return written


def _model_summary(output: PortfolioAttributionOutput) -> dict[str, Any]:
    snapshot = output.snapshot
    components = json.loads(snapshot["component_volatility_contribution_json"])
    if not isinstance(components, dict) or not components:
        raise ValidationError("portfolio attribution components are invalid")
    largest_component = max(
        sorted(components),
        key=lambda key: abs(float(components[key])),
    )
    return {
        "model_version": snapshot["model_version"],
        "covariance_method": snapshot["covariance_method"],
        "correlation_method": snapshot["correlation_method"],
        "calculation_id": snapshot["calculation_id"],
        "portfolio_variance_annualized": snapshot[
            "portfolio_variance_annualized"
        ],
        "portfolio_volatility_annualized": snapshot[
            "portfolio_volatility_annualized"
        ],
        "volatility_status": snapshot["volatility_status"],
        "correlation_status": snapshot["correlation_status"],
        "largest_absolute_component": largest_component,
        "largest_absolute_component_contribution": components[largest_component],
        "diagnostics": dict(output.diagnostics),
    }


def run_portfolio_covariance_comparison(
    *,
    portfolio_id: str,
    portfolio_config_path: Path,
    end_date: date,
    covariance_window: int,
    storage_config_path: Path,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    definition_loader: DefinitionLoader | None = None,
) -> dict[str, Any]:
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

    sample = build_portfolio_attribution(
        records,
        definition=definition,
        covariance_window=covariance_window,
        end_date=end_date,
    )
    ewma = build_portfolio_ewma_attribution(
        records,
        definition=definition,
        covariance_window=covariance_window,
        end_date=end_date,
    )

    alignment_fields = (
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
    if any(
        sample.snapshot[field] != ewma.snapshot[field]
        for field in alignment_fields
    ):
        raise ValidationError(
            "sample and EWMA attribution calculations are not input-aligned"
        )

    selected_writer = writer or write_records
    outputs = (sample, ewma)
    written = _publish_snapshots(
        outputs,
        storage_config=storage_config,
        writer=selected_writer,
    )

    sample_summary = _model_summary(sample)
    ewma_summary = _model_summary(ewma)
    sample_volatility = float(
        sample.snapshot["portfolio_volatility_annualized"]
    )
    ewma_volatility = float(ewma.snapshot["portfolio_volatility_annualized"])
    difference = ewma_volatility - sample_volatility
    ratio = (
        ewma_volatility / sample_volatility
        if sample_volatility > 0
        else None
    )
    if ratio is not None and not math.isfinite(ratio):
        raise ValidationError("portfolio covariance comparison ratio is invalid")

    return {
        "run_id": str(uuid4()),
        "portfolio_id": definition.portfolio_id,
        "base_currency": definition.base_currency,
        "definition_fingerprint": definition.fingerprint,
        "selection": {
            "end_date": end_date.isoformat(),
            "covariance_window": covariance_window,
            "window_start": sample.snapshot["window_start"].date().isoformat(),
            "window_end": sample.snapshot["window_end"].date().isoformat(),
            "input_calculation_ids": json.loads(
                sample.snapshot["input_calculation_ids_json"]
            ),
        },
        "parameters": {
            "sample_covariance_method": SAMPLE_COVARIANCE_METHOD,
            "sample_model_version": SAMPLE_MODEL_VERSION,
            "ewma_covariance_method": EWMA_COVARIANCE_METHOD,
            "ewma_model_version": EWMA_MODEL_VERSION,
            "ewma_decay": EWMA_DECAY,
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": 2,
                "records_written": written,
                "records_already_present": 2 - written,
            }
        },
        "models": {
            "sample": sample_summary,
            "ewma": ewma_summary,
        },
        "comparison": {
            "ewma_minus_sample_volatility": difference,
            "ewma_to_sample_volatility_ratio": ratio,
            "higher_volatility_model": (
                "ewma"
                if difference > 0
                else "sample"
                if difference < 0
                else "equal"
            ),
        },
    }


def _write_summary(path: Path, summary: dict[str, Any]) -> None:
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
            "Unable to write the portfolio covariance comparison summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_portfolio_covariance_comparison(
            portfolio_id=args.portfolio_id,
            portfolio_config_path=args.portfolio_config,
            end_date=args.end_date,
            covariance_window=args.covariance_window,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Portfolio covariance comparison failed: configuration, input data, "
            "or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Portfolio covariance comparison failed: local storage operation "
            "failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio covariance comparison failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
