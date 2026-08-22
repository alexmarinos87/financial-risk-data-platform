from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from uuid import uuid4

from ..analytics.governed_portfolio_segments import (
    MAX_GOVERNED_SEGMENTS,
    GovernedPortfolioSegmentPlan,
    plan_governed_portfolio_segments,
)
from ..analytics.portfolio_mandates import (
    mandate_metadata,
    parse_portfolio_mandates,
)
from ..analytics.portfolio_risk import TRADING_DAYS_PER_YEAR
from ..analytics.portfolio_risk_limit_policies import (
    parse_portfolio_risk_limit_policies,
    policy_metadata,
)
from ..common.config import load_yaml
from ..common.exceptions import StorageError, ValidationError
from .run_governed_portfolio_cycle import STAGES

ConfigLoader = Callable[[Path], Any]


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


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plan exact mandate-policy segments for a governed historical "
            "portfolio range without running analytical stages."
        )
    )
    parser.add_argument("--portfolio-id", required=True)
    parser.add_argument("--policy-id", required=True)
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument(
        "--risk-limit-config",
        type=Path,
        default=Path("config/portfolio_risk_limits.yaml"),
    )
    parser.add_argument("--start-date", required=True, type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--covariance-window",
        type=_positive_integer,
        default=20,
    )
    parser.add_argument(
        "--max-segments",
        type=_positive_integer,
        default=MAX_GOVERNED_SEGMENTS,
    )
    parser.add_argument("--summary-json", type=Path)
    return parser


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    return value


def _segment_records(plan: GovernedPortfolioSegmentPlan) -> list[dict[str, Any]]:
    return [
        {
            "calendar_days": segment.calendar_days,
            "end_date": segment.end_date.isoformat(),
            "mandate": mandate_metadata(segment.mandate),
            "policy_version": policy_metadata(segment.policy),
            "segment_id": segment.segment_id,
            "start_date": segment.start_date.isoformat(),
        }
        for segment in plan.segments
    ]


def plan_governed_portfolio_history(
    *,
    portfolio_id: str,
    policy_id: str,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    start_date: date,
    end_date: date,
    covariance_window: int,
    max_segments: int = MAX_GOVERNED_SEGMENTS,
    config_loader: ConfigLoader | None = None,
) -> dict[str, Any]:
    selected_loader = config_loader or load_yaml
    try:
        portfolio_payload = _mapping(
            selected_loader(portfolio_config_path),
            "portfolio configuration",
        )
        policy_payload = _mapping(
            selected_loader(risk_limit_config_path),
            "risk-limit configuration",
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "governed portfolio planning configuration could not be loaded"
        ) from None

    mandates = parse_portfolio_mandates(portfolio_payload, portfolio_id)
    policies = parse_portfolio_risk_limit_policies(policy_payload, policy_id)
    plan = plan_governed_portfolio_segments(
        mandates,
        policies,
        portfolio_id=portfolio_id,
        policy_id=policy_id,
        start_date=start_date,
        end_date=end_date,
        covariance_window=covariance_window,
        annualization_days=TRADING_DAYS_PER_YEAR,
        max_segments=max_segments,
    )
    segments = _segment_records(plan)
    return {
        "execution": {
            "performed": False,
            "planned_segment_runs": len(segments),
            "planned_stage_invocations": len(segments) * len(STAGES),
            "planned_stages": list(STAGES),
            "reason": "plan_only",
        },
        "parameters": {
            "annualization_days": TRADING_DAYS_PER_YEAR,
            "covariance_window": covariance_window,
            "max_segments": max_segments,
        },
        "plan_id": plan.diagnostics["plan_id"],
        "policy_id": policy_id,
        "portfolio_id": portfolio_id,
        "run_id": str(uuid4()),
        "segments": segments,
        "selection": dict(plan.diagnostics),
    }


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
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
            "Unable to write the governed portfolio history plan"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = plan_governed_portfolio_history(
            portfolio_id=args.portfolio_id,
            policy_id=args.policy_id,
            portfolio_config_path=args.portfolio_config,
            risk_limit_config_path=args.risk_limit_config,
            start_date=args.start_date,
            end_date=args.end_date,
            covariance_window=args.covariance_window,
            max_segments=args.max_segments,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Governed portfolio history planning failed: configuration, "
            "coverage, compatibility, or options were invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Governed portfolio history planning failed: summary publication "
            "failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Governed portfolio history planning failed: unexpected local "
            "failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
