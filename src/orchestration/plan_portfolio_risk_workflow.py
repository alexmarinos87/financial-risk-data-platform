from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from ..common.exceptions import StorageError, ValidationError
from .portfolio_risk_workflow_plan import (
    DEFAULT_PYTHON_COMMAND,
    MAX_PLAN_EVALUATIONS,
    MAX_PLAN_SNAPSHOTS,
    build_portfolio_risk_workflow_plan,
    write_portfolio_risk_workflow_plan,
)

DEFAULT_OUTPUT = Path(".demo/portfolio-risk-workflow-plan.json")


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


def _bounded_integer(value: str, label: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"{label} must be an integer between {minimum} and {maximum}"
        ) from exc
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return parsed


def _volatility_window(value: str) -> int:
    return _bounded_integer(value, "volatility_window", 2, 2_520)


def _var_window(value: str) -> int:
    return _bounded_integer(value, "var_window", 2, 2_520)


def _max_snapshots(value: str) -> int:
    return _bounded_integer(
        value,
        "max_snapshots",
        1,
        MAX_PLAN_SNAPSHOTS,
    )


def _max_evaluations(value: str) -> int:
    return _bounded_integer(
        value,
        "max_evaluations",
        1,
        MAX_PLAN_EVALUATIONS,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a deterministic, non-authorizing local portfolio-risk "
            "workflow plan. This command never executes a planned step."
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
        "--limits-config",
        type=Path,
        default=Path("config/portfolio_risk_limits.yaml"),
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument("--start-date", required=True, type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument(
        "--vol-window",
        type=_volatility_window,
        default=20,
    )
    parser.add_argument(
        "--var-window",
        type=_var_window,
        default=60,
    )
    parser.add_argument("--var-confidence", type=float, default=0.95)
    parser.add_argument(
        "--max-snapshots",
        type=_max_snapshots,
        default=MAX_PLAN_SNAPSHOTS,
    )
    parser.add_argument(
        "--max-evaluations",
        type=_max_evaluations,
        default=MAX_PLAN_EVALUATIONS,
    )
    parser.add_argument(
        "--python-command",
        default=DEFAULT_PYTHON_COMMAND,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        plan = build_portfolio_risk_workflow_plan(
            portfolio_id=args.portfolio_id,
            policy_id=args.policy_id,
            start_date=args.start_date,
            end_date=args.end_date,
            portfolio_config_path=args.portfolio_config,
            limits_config_path=args.limits_config,
            storage_config_path=args.storage_config,
            volatility_window=args.vol_window,
            var_window=args.var_window,
            var_confidence=args.var_confidence,
            max_snapshots=args.max_snapshots,
            max_evaluations=args.max_evaluations,
            python_command=args.python_command,
        )
        written = write_portfolio_risk_workflow_plan(args.output, plan)
    except ValidationError as exc:
        print(
            f"Portfolio risk workflow planning failed: {exc}",
            file=sys.stderr,
        )
        return 1
    except StorageError as exc:
        print(
            f"Portfolio risk workflow planning failed: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio risk workflow planning failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            {
                "plan_id": plan["plan_id"],
                "output_path": args.output.as_posix(),
                "records_written": written,
                "execution_authorized": plan["execution_authorized"],
                "requires_human_review": plan["requires_human_review"],
                "step_count": len(plan["steps"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
