from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from ..common.exceptions import StorageError, ValidationError
from .portfolio_risk_workflow_verification import (
    load_and_verify_portfolio_risk_workflow_plan,
    write_portfolio_risk_workflow_verification,
)

DEFAULT_OUTPUT = Path(".demo/portfolio-risk-workflow-verification.json")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a controlled portfolio-risk workflow plan and its current "
            "configuration evidence without executing any planned command."
        )
    )
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        report = load_and_verify_portfolio_risk_workflow_plan(args.plan)
        written = write_portfolio_risk_workflow_verification(
            args.output,
            report,
        )
    except ValidationError as exc:
        print(
            f"Portfolio risk workflow verification failed: {exc}",
            file=sys.stderr,
        )
        return 1
    except StorageError as exc:
        print(
            f"Portfolio risk workflow verification failed: {exc}",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Portfolio risk workflow verification failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            {
                "verification_id": report["verification_id"],
                "plan_id": report["plan_id"],
                "verified": report["verified"],
                "execution_authorized": report["execution_authorized"],
                "verified_check_count": report["verified_check_count"],
                "records_written": written,
                "output_path": args.output.as_posix(),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
