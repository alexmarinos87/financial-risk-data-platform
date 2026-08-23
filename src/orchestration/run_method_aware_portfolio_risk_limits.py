from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.portfolio_risk import PortfolioDefinition, load_portfolio_definition
from ..analytics.portfolio_risk_limit_method_evaluations import (
    evaluate_method_aware_portfolio_risk_limits,
)
from ..analytics.portfolio_risk_limit_method_policies import (
    MethodAwarePortfolioRiskLimitPolicy,
    load_method_aware_portfolio_risk_limit_policy,
    method_policy_metadata,
    validate_method_policy_range,
)
from ..analytics.portfolio_risk_limits import MAX_LIMIT_EVALUATIONS
from ..common.exceptions import StorageError, ValidationError
from ..storage.s3_writer import write_records
from ..storage.storage_config import load_storage_config, validate_storage_config
from ..warehouse.portfolio_attribution_loader import collect_attribution_records

INPUT_DATASET = "portfolio_risk_attribution"
OUTPUT_DATASET = "portfolio_risk_limit_evaluations"

Reader = Callable[[Path], list[dict[str, Any]]]
Writer = Callable[..., int]
StorageConfigLoader = Callable[[Path], dict[str, Any]]
DefinitionLoader = Callable[[Path, str], PortfolioDefinition]
MethodPolicyLoader = Callable[..., MethodAwarePortfolioRiskLimitPolicy]


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


def _max_evaluations(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "max_evaluations must be a positive integer"
        ) from exc
    if not 1 <= parsed <= MAX_LIMIT_EVALUATIONS:
        raise argparse.ArgumentTypeError(
            f"max_evaluations must be between 1 and {MAX_LIMIT_EVALUATIONS}"
        )
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate one explicitly bound sample or EWMA attribution history "
            "against an effective-dated local risk-limit policy."
        )
    )
    parser.add_argument("--method-policy-id", required=True)
    parser.add_argument(
        "--method-policies-config",
        type=Path,
        default=Path("config/portfolio_risk_limit_methods.yaml"),
    )
    parser.add_argument(
        "--limits-config",
        type=Path,
        default=Path("config/portfolio_risk_limits.yaml"),
    )
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--max-evaluations",
        type=_max_evaluations,
        default=MAX_LIMIT_EVALUATIONS,
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _require_datasets(storage_config: dict[str, Any]) -> None:
    validate_storage_config(storage_config)
    datasets = storage_config["storage"]["curated"]["datasets"]
    missing = sorted(
        dataset
        for dataset in (INPUT_DATASET, OUTPUT_DATASET)
        if dataset not in datasets
    )
    if missing:
        raise StorageError(
            "Storage configuration is missing method-aware risk-limit datasets: "
            + ", ".join(missing)
        )


def _publish(
    evaluations: tuple[dict[str, Any], ...],
    *,
    storage_config: dict[str, Any],
    writer: Writer,
) -> int:
    written = 0
    for evaluation in evaluations:
        try:
            result = writer(
                [evaluation],
                kind="curated",
                dataset=OUTPUT_DATASET,
                storage_config=storage_config,
            )
        except Exception:
            raise StorageError(
                "Method-aware risk-limit publication failed; rerun is safe"
            ) from None
        if type(result) is not int or result not in {0, 1}:
            raise StorageError(
                "Method-aware risk-limit publication returned an invalid result"
            )
        written += result
    return written


def run_method_aware_portfolio_risk_limits(
    *,
    method_policy_id: str,
    method_policies_config_path: Path,
    limits_config_path: Path,
    portfolio_config_path: Path,
    end_date: date,
    storage_config_path: Path,
    start_date: date | None = None,
    max_evaluations: int = MAX_LIMIT_EVALUATIONS,
    reader: Reader | None = None,
    writer: Writer | None = None,
    storage_config_loader: StorageConfigLoader | None = None,
    definition_loader: DefinitionLoader | None = None,
    method_policy_loader: MethodPolicyLoader | None = None,
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

    selected_policy_loader = (
        method_policy_loader or load_method_aware_portfolio_risk_limit_policy
    )
    try:
        policy = selected_policy_loader(
            method_policy_config_path=method_policies_config_path,
            limits_config_path=limits_config_path,
            method_policy_id=method_policy_id,
            as_of_date=end_date,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "Method-aware portfolio risk-limit policy is invalid"
        ) from None
    if not isinstance(policy, MethodAwarePortfolioRiskLimitPolicy):
        raise ValidationError(
            "method policy loader returned an invalid policy binding"
        )
    validate_method_policy_range(
        policy,
        start_date=start_date,
        end_date=end_date,
    )

    selected_definition_loader = definition_loader or load_portfolio_definition
    try:
        definition = selected_definition_loader(
            portfolio_config_path,
            policy.portfolio_id,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Portfolio configuration is invalid") from None

    selected_reader = reader or collect_attribution_records
    try:
        records = selected_reader(storage_config_path)
    except StorageError:
        raise
    except Exception:
        raise StorageError("Unable to read local portfolio attribution") from None

    output = evaluate_method_aware_portfolio_risk_limits(
        records,
        policy=policy,
        definition_fingerprint=definition.fingerprint,
        start_date=start_date,
        end_date=end_date,
        max_evaluations=max_evaluations,
    )
    selected_writer = writer or write_records
    written = _publish(
        output.evaluations,
        storage_config=storage_config,
        writer=selected_writer,
    )

    latest_date = max(item["ts_event"] for item in output.evaluations)
    latest = [
        item for item in output.evaluations if item["ts_event"] == latest_date
    ]
    severity = {"ok": 0, "warning": 1, "critical": 2}
    latest_status = max(latest, key=lambda item: severity[item["status"]])[
        "status"
    ]
    selected = len(output.evaluations)
    return {
        "run_id": str(uuid4()),
        "method_policy": method_policy_metadata(policy),
        "portfolio_id": policy.portfolio_id,
        "definition_fingerprint": definition.fingerprint,
        "selection": dict(output.diagnostics),
        "parameters": {
            "start_date": start_date.isoformat() if start_date else None,
            "end_date": end_date.isoformat(),
            "max_evaluations": max_evaluations,
        },
        "curated_output": {
            OUTPUT_DATASET: {
                "records_selected": selected,
                "records_written": written,
                "records_already_present": selected - written,
            }
        },
        "latest_status": {
            "ts_event": latest_date.isoformat(),
            "status": latest_status,
            "metrics": [
                {
                    "metric_name": item["metric_name"],
                    "subject_key": item["subject_key"],
                    "observed_value": item["observed_value"],
                    "status": item["status"],
                    "is_breach": item["is_breach"],
                    "calculation_id": item["calculation_id"],
                }
                for item in sorted(latest, key=lambda item: item["metric_name"])
            ],
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
            "Unable to write the method-aware risk-limit summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_method_aware_portfolio_risk_limits(
            method_policy_id=args.method_policy_id,
            method_policies_config_path=args.method_policies_config,
            limits_config_path=args.limits_config,
            portfolio_config_path=args.portfolio_config,
            start_date=args.start_date,
            end_date=args.end_date,
            max_evaluations=args.max_evaluations,
            storage_config_path=args.storage_config,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Method-aware risk-limit evaluation failed: configuration, input "
            "data or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Method-aware risk-limit evaluation failed: local storage operation "
            "failed; rerun is safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Method-aware risk-limit evaluation failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
