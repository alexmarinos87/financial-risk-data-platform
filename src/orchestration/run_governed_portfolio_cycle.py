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

from ..analytics.portfolio_attribution import MAX_COVARIANCE_WINDOW
from ..analytics.portfolio_attribution_history import MAX_HISTORY_SNAPSHOTS
from ..analytics.portfolio_mandates import (
    PortfolioMandate,
    filter_records_to_mandate,
    load_portfolio_mandate,
    mandate_metadata,
    validate_mandate_range,
)
from ..analytics.portfolio_risk import TRADING_DAYS_PER_YEAR
from ..analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
    load_effective_portfolio_risk_limit_policy,
    policy_metadata,
    validate_policy_range,
)
from ..analytics.portfolio_risk_limits import MAX_LIMIT_EVALUATIONS
from ..common.exceptions import OverlapError, StorageError, ValidationError
from ..warehouse.portfolio_attribution_loader import collect_attribution_records
from .locks import acquire_partition_locks, release_partition_locks
from .run_portfolio_attribution import load_portfolio_return_records
from .run_portfolio_attribution_history import run_portfolio_attribution_history
from .run_portfolio_risk import load_daily_return_records, run_portfolio_risk
from .run_portfolio_risk_limits import run_portfolio_risk_limits

DEFAULT_STALE_LOCK_SECONDS = 21_600
STAGES = (
    "portfolio_risk",
    "portfolio_attribution_history",
    "portfolio_risk_limits",
)

Stage = Callable[..., dict[str, Any]]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
PolicyLoader = Callable[
    [Path, str, date],
    EffectiveDatedPortfolioRiskLimitPolicy,
]
DailyReader = Callable[..., list[dict[str, Any]]]
PortfolioReader = Callable[..., list[dict[str, Any]]]
AttributionReader = Callable[[Path], list[dict[str, Any]]]
LockAcquirer = Callable[..., list[Path]]
LockReleaser = Callable[[list[Path]], None]


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


def _confidence(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "must be a number between 0 and 1"
        ) from exc
    if not math.isfinite(parsed) or not 0 < parsed < 1:
        raise argparse.ArgumentTypeError("must be a number between 0 and 1")
    return parsed


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run portfolio risk, rolling attribution and risk-limit evaluation "
            "under one effective-dated portfolio mandate and policy version."
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
    parser.add_argument("--start-date", type=_calendar_date)
    parser.add_argument("--end-date", required=True, type=_calendar_date)
    parser.add_argument("--vol-window", type=_positive_integer, default=20)
    parser.add_argument("--var-window", type=_positive_integer, default=60)
    parser.add_argument("--var-confidence", type=_confidence, default=0.95)
    parser.add_argument(
        "--covariance-window",
        type=_positive_integer,
        default=20,
    )
    parser.add_argument(
        "--max-snapshots",
        type=_positive_integer,
        default=MAX_HISTORY_SNAPSHOTS,
    )
    parser.add_argument(
        "--max-evaluations",
        type=_positive_integer,
        default=MAX_LIMIT_EVALUATIONS,
    )
    parser.add_argument(
        "--storage-config",
        type=Path,
        default=Path("config/storage.yaml"),
    )
    parser.add_argument(
        "--lock-base-dir",
        type=Path,
        default=Path("."),
    )
    parser.add_argument(
        "--stale-lock-seconds",
        type=_positive_integer,
        default=DEFAULT_STALE_LOCK_SECONDS,
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--summary-json", type=Path)
    return parser


def _require_stage_summary(value: Any, stage: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise StorageError(f"{stage} returned an invalid run summary")
    return value


def _validate_parameters(
    *,
    volatility_window: int,
    var_window: int,
    var_confidence: float,
    covariance_window: int,
    max_snapshots: int,
    max_evaluations: int,
) -> None:
    for label, value in (
        ("volatility_window", volatility_window),
        ("var_window", var_window),
    ):
        if type(value) is not int or value < 2:
            raise ValidationError(f"{label} must be an integer of at least 2")
    if (
        isinstance(var_confidence, bool)
        or not isinstance(var_confidence, (int, float))
        or not math.isfinite(float(var_confidence))
        or not 0 < float(var_confidence) < 1
    ):
        raise ValidationError("var_confidence must be between 0 and 1")
    if (
        type(covariance_window) is not int
        or not 2 <= covariance_window <= MAX_COVARIANCE_WINDOW
    ):
        raise ValidationError(
            "covariance_window must be an integer between 2 and "
            f"{MAX_COVARIANCE_WINDOW}"
        )
    if (
        type(max_snapshots) is not int
        or not 1 <= max_snapshots <= MAX_HISTORY_SNAPSHOTS
    ):
        raise ValidationError(
            "max_snapshots must be an integer between 1 and "
            f"{MAX_HISTORY_SNAPSHOTS}"
        )
    if (
        type(max_evaluations) is not int
        or not 1 <= max_evaluations <= MAX_LIMIT_EVALUATIONS
    ):
        raise ValidationError(
            "max_evaluations must be an integer between 1 and "
            f"{MAX_LIMIT_EVALUATIONS}"
        )


def run_governed_portfolio_cycle(
    *,
    portfolio_id: str,
    policy_id: str,
    portfolio_config_path: Path,
    risk_limit_config_path: Path,
    start_date: date | None,
    end_date: date,
    volatility_window: int,
    var_window: int,
    var_confidence: float,
    covariance_window: int,
    max_snapshots: int,
    max_evaluations: int,
    storage_config_path: Path,
    dry_run: bool = False,
    lock_base_dir: Path = Path("."),
    stale_lock_seconds: int = DEFAULT_STALE_LOCK_SECONDS,
    mandate_loader: MandateLoader | None = None,
    policy_loader: PolicyLoader | None = None,
    portfolio_stage: Stage | None = None,
    attribution_stage: Stage | None = None,
    risk_limit_stage: Stage | None = None,
    daily_reader: DailyReader | None = None,
    portfolio_reader: PortfolioReader | None = None,
    attribution_reader: AttributionReader | None = None,
    lock_acquirer: LockAcquirer | None = None,
    lock_releaser: LockReleaser | None = None,
) -> dict[str, Any]:
    if start_date is not None and start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    _validate_parameters(
        volatility_window=volatility_window,
        var_window=var_window,
        var_confidence=var_confidence,
        covariance_window=covariance_window,
        max_snapshots=max_snapshots,
        max_evaluations=max_evaluations,
    )
    if type(stale_lock_seconds) is not int or stale_lock_seconds <= 0:
        raise ValidationError("stale_lock_seconds must be a positive integer")

    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    try:
        mandate = selected_mandate_loader(
            portfolio_config_path,
            portfolio_id,
            end_date,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "portfolio mandate configuration is invalid"
        ) from None

    effective_start_date = start_date or mandate.effective_from
    validate_mandate_range(
        mandate,
        start_date=effective_start_date,
        end_date=end_date,
    )

    selected_policy_loader = (
        policy_loader or load_effective_portfolio_risk_limit_policy
    )
    try:
        policy = selected_policy_loader(
            risk_limit_config_path,
            policy_id,
            end_date,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError(
            "risk-limit policy configuration is invalid"
        ) from None
    if not isinstance(policy, EffectiveDatedPortfolioRiskLimitPolicy):
        raise ValidationError(
            "risk-limit policy loader returned an invalid policy version"
        )
    validate_policy_range(
        policy,
        start_date=effective_start_date,
        end_date=end_date,
    )

    if policy.portfolio_id != mandate.portfolio_id:
        raise ValidationError(
            "risk-limit policy portfolio does not match the selected mandate"
        )
    if policy.covariance_window != covariance_window:
        raise ValidationError(
            "covariance_window must match the selected risk-limit policy"
        )
    if policy.annualization_days != TRADING_DAYS_PER_YEAR:
        raise ValidationError(
            "risk-limit policy annualization_days is unsupported by "
            "portfolio attribution"
        )

    run_id = str(uuid4())
    base_summary: dict[str, Any] = {
        "run_id": run_id,
        "portfolio_id": mandate.portfolio_id,
        "policy_id": policy.policy_id,
        "policy_fingerprint": policy.fingerprint,
        "policy_version": policy_metadata(policy),
        "mandate": mandate_metadata(mandate),
        "selection": {
            "requested_start_date": (
                start_date.isoformat() if start_date is not None else None
            ),
            "effective_start_date": effective_start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
        "parameters": {
            "volatility_window": volatility_window,
            "var_window": var_window,
            "var_confidence": var_confidence,
            "covariance_window": covariance_window,
            "max_snapshots": max_snapshots,
            "max_evaluations": max_evaluations,
        },
        "delivery": {
            "performed": False,
            "reason": "analytics_and_evidence_only",
        },
    }
    if dry_run:
        return {
            **base_summary,
            "execution": {
                "performed": False,
                "lock_acquired": False,
                "planned_stages": list(STAGES),
            },
            "stages": {},
        }

    selected_daily_reader = daily_reader or load_daily_return_records
    selected_portfolio_reader = portfolio_reader or load_portfolio_return_records
    selected_attribution_reader = attribution_reader or collect_attribution_records

    def definition_loader(
        _path: Path,
        requested_portfolio_id: str,
    ) -> PortfolioMandate:
        if requested_portfolio_id != mandate.portfolio_id:
            raise ValidationError(
                "stage requested a portfolio outside the selected mandate"
            )
        return mandate

    def selected_policy(
        _path: Path,
        requested_policy_id: str,
        as_of_date: date,
    ) -> EffectiveDatedPortfolioRiskLimitPolicy:
        if requested_policy_id != policy.policy_id:
            raise ValidationError(
                "stage requested a policy outside the governed cycle"
            )
        if not policy.contains(as_of_date):
            raise ValidationError(
                "stage requested a date outside the governed policy version"
            )
        return policy

    def mandate_daily_reader(
        *,
        storage_config: dict[str, Any],
        end_date: date,
    ) -> list[dict[str, Any]]:
        records = selected_daily_reader(
            storage_config=storage_config,
            end_date=end_date,
        )
        return [
            dict(record)
            for record in filter_records_to_mandate(records, mandate)
        ]

    def mandate_portfolio_reader(
        *,
        storage_config: dict[str, Any],
        portfolio_id: str,
        definition_fingerprint: str,
        end_date: date,
    ) -> list[dict[str, Any]]:
        if portfolio_id != mandate.portfolio_id:
            raise ValidationError(
                "portfolio attribution requested an unexpected portfolio"
            )
        if definition_fingerprint != mandate.fingerprint:
            raise ValidationError(
                "portfolio attribution requested an unexpected mandate"
            )
        records = selected_portfolio_reader(
            storage_config=storage_config,
            portfolio_id=portfolio_id,
            definition_fingerprint=definition_fingerprint,
            end_date=end_date,
        )
        return [
            dict(record)
            for record in filter_records_to_mandate(records, mandate)
        ]

    def mandate_attribution_reader(path: Path) -> list[dict[str, Any]]:
        records = selected_attribution_reader(path)
        return [
            dict(record)
            for record in filter_records_to_mandate(records, mandate)
        ]

    selected_portfolio_stage = portfolio_stage or run_portfolio_risk
    selected_attribution_stage = (
        attribution_stage or run_portfolio_attribution_history
    )
    selected_risk_limit_stage = risk_limit_stage or run_portfolio_risk_limits
    selected_lock_acquirer = lock_acquirer or acquire_partition_locks
    selected_lock_releaser = lock_releaser or release_partition_locks

    lock_paths: list[Path] = []
    try:
        lock_paths = selected_lock_acquirer(
            lock_base_dir,
            [f"governed-portfolio/{mandate.portfolio_id}"],
            run_id,
            stale_after_seconds=stale_lock_seconds,
        )
        if len(lock_paths) != 1:
            raise StorageError(
                "governed portfolio cycle did not acquire exactly one lock"
            )
        portfolio_summary = _require_stage_summary(
            selected_portfolio_stage(
                portfolio_id=mandate.portfolio_id,
                portfolio_config_path=portfolio_config_path,
                start_date=effective_start_date,
                end_date=end_date,
                volatility_window=volatility_window,
                var_window=var_window,
                var_confidence=var_confidence,
                storage_config_path=storage_config_path,
                reader=mandate_daily_reader,
                definition_loader=definition_loader,
            ),
            "portfolio_risk",
        )
        attribution_summary = _require_stage_summary(
            selected_attribution_stage(
                portfolio_id=mandate.portfolio_id,
                portfolio_config_path=portfolio_config_path,
                start_date=effective_start_date,
                end_date=end_date,
                covariance_window=covariance_window,
                max_snapshots=max_snapshots,
                storage_config_path=storage_config_path,
                reader=mandate_portfolio_reader,
                definition_loader=definition_loader,
            ),
            "portfolio_attribution_history",
        )
        limit_summary = _require_stage_summary(
            selected_risk_limit_stage(
                policy_id=policy.policy_id,
                limits_config_path=risk_limit_config_path,
                portfolio_config_path=portfolio_config_path,
                start_date=effective_start_date,
                end_date=end_date,
                max_evaluations=max_evaluations,
                storage_config_path=storage_config_path,
                reader=mandate_attribution_reader,
                policy_loader=selected_policy,
                definition_loader=definition_loader,
            ),
            "portfolio_risk_limits",
        )
    finally:
        if lock_paths:
            selected_lock_releaser(lock_paths)

    return {
        **base_summary,
        "execution": {
            "performed": True,
            "lock_acquired": True,
            "completed_stages": list(STAGES),
        },
        "stages": {
            "portfolio_risk": portfolio_summary,
            "portfolio_attribution_history": attribution_summary,
            "portfolio_risk_limits": limit_summary,
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
            "Unable to write the governed portfolio cycle summary"
        ) from None


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = run_governed_portfolio_cycle(
            portfolio_id=args.portfolio_id,
            policy_id=args.policy_id,
            portfolio_config_path=args.portfolio_config,
            risk_limit_config_path=args.risk_limit_config,
            start_date=args.start_date,
            end_date=args.end_date,
            volatility_window=args.vol_window,
            var_window=args.var_window,
            var_confidence=args.var_confidence,
            covariance_window=args.covariance_window,
            max_snapshots=args.max_snapshots,
            max_evaluations=args.max_evaluations,
            storage_config_path=args.storage_config,
            dry_run=args.dry_run,
            lock_base_dir=args.lock_base_dir,
            stale_lock_seconds=args.stale_lock_seconds,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, summary)
    except ValidationError:
        print(
            "Governed portfolio cycle failed: mandate, policy, data or options "
            "were invalid",
            file=sys.stderr,
        )
        return 1
    except OverlapError:
        print(
            "Governed portfolio cycle failed: another cycle already owns the "
            "portfolio lock",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Governed portfolio cycle failed: local storage operation failed; "
            "completed stages are replay-safe",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Governed portfolio cycle failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1

    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
