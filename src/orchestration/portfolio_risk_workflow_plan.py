from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

from ..analytics.portfolio_mandates import (
    PortfolioMandate,
    load_portfolio_mandate,
    mandate_metadata,
    validate_mandate_range,
)
from ..analytics.portfolio_risk_limit_policies import (
    EffectiveDatedPortfolioRiskLimitPolicy,
    load_effective_portfolio_risk_limit_policy,
    policy_metadata,
    validate_policy_range,
)
from ..common.exceptions import StorageError, ValidationError
from ..storage.storage_config import load_storage_config

PLAN_SCHEMA_VERSION = "portfolio-risk-workflow-plan-v1"
MAX_CONFIG_BYTES = 1_000_000
MAX_PLAN_SNAPSHOTS = 2_500
MAX_PLAN_EVALUATIONS = 10_000
DEFAULT_PYTHON_COMMAND = ".venv/bin/python"
DEFAULT_CYCLE_SUMMARY = ".demo/governed-portfolio-cycle-summary.json"


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _bounded_integer(
    value: int,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def _safe_text(value: str, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def _config_evidence(path: Path, label: str) -> dict[str, Any]:
    if path.is_symlink():
        raise StorageError(f"{label} must not be a symbolic link")
    if not path.exists() or not path.is_file():
        raise StorageError(f"{label} must be a regular file")
    try:
        payload = path.read_bytes()
    except OSError:
        raise StorageError(f"{label} could not be read") from None
    if len(payload) > MAX_CONFIG_BYTES:
        raise StorageError(
            f"{label} exceeds the {MAX_CONFIG_BYTES}-byte plan limit"
        )
    return {
        "path": path.as_posix(),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }


def _python_command(
    module: str,
    arguments: list[str],
    *,
    python_command: str,
) -> dict[str, Any]:
    return {
        "program": python_command,
        "argv": ["-m", module, *arguments],
    }


def _make_command(target: str) -> dict[str, Any]:
    return {
        "program": "make",
        "argv": [target],
    }


def _step(
    *,
    step_id: str,
    description: str,
    depends_on: list[str],
    commands: list[dict[str, Any]],
    reads: list[str],
    writes: list[str],
    preconditions: list[str],
) -> dict[str, Any]:
    return {
        "step_id": step_id,
        "description": description,
        "depends_on": depends_on,
        "commands": commands,
        "declared_effects": {
            "reads": reads,
            "writes": writes,
        },
        "preconditions": preconditions,
    }


def build_portfolio_risk_workflow_plan(
    *,
    portfolio_id: str,
    policy_id: str,
    start_date: date,
    end_date: date,
    portfolio_config_path: Path,
    limits_config_path: Path,
    storage_config_path: Path,
    volatility_window: int = 20,
    var_window: int = 60,
    var_confidence: float = 0.95,
    max_snapshots: int = MAX_PLAN_SNAPSHOTS,
    max_evaluations: int = MAX_PLAN_EVALUATIONS,
    python_command: str = DEFAULT_PYTHON_COMMAND,
) -> dict[str, Any]:
    if isinstance(start_date, datetime) or not isinstance(start_date, date):
        raise ValidationError("start_date must be a calendar date")
    if isinstance(end_date, datetime) or not isinstance(end_date, date):
        raise ValidationError("end_date must be a calendar date")
    if start_date > end_date:
        raise ValidationError("start_date must be on or before end_date")
    portfolio_id = _safe_text(portfolio_id, "portfolio_id")
    policy_id = _safe_text(policy_id, "policy_id")
    python_command = _safe_text(python_command, "python_command")
    volatility_window = _bounded_integer(
        volatility_window,
        "volatility_window",
        minimum=2,
        maximum=2_520,
    )
    var_window = _bounded_integer(
        var_window,
        "var_window",
        minimum=2,
        maximum=2_520,
    )
    max_snapshots = _bounded_integer(
        max_snapshots,
        "max_snapshots",
        minimum=1,
        maximum=MAX_PLAN_SNAPSHOTS,
    )
    max_evaluations = _bounded_integer(
        max_evaluations,
        "max_evaluations",
        minimum=1,
        maximum=MAX_PLAN_EVALUATIONS,
    )
    if not isinstance(var_confidence, (int, float)) or isinstance(
        var_confidence,
        bool,
    ):
        raise ValidationError("var_confidence must be numeric")
    var_confidence = float(var_confidence)
    if not 0.0 < var_confidence < 1.0:
        raise ValidationError("var_confidence must be between zero and one")

    portfolio_config = _config_evidence(
        portfolio_config_path,
        "portfolio configuration",
    )
    limits_config = _config_evidence(
        limits_config_path,
        "risk-limit configuration",
    )
    storage_config = _config_evidence(
        storage_config_path,
        "storage configuration",
    )
    try:
        load_storage_config(storage_config_path)
    except Exception:
        raise StorageError("Storage configuration is invalid") from None

    try:
        mandate = load_portfolio_mandate(
            portfolio_config_path,
            portfolio_id,
            end_date,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Portfolio mandate is invalid") from None
    if not isinstance(mandate, PortfolioMandate):
        raise ValidationError("Portfolio mandate loader returned an invalid mandate")
    validate_mandate_range(
        mandate,
        start_date=start_date,
        end_date=end_date,
    )

    try:
        policy = load_effective_portfolio_risk_limit_policy(
            limits_config_path,
            policy_id,
            end_date,
        )
    except ValidationError:
        raise
    except Exception:
        raise ValidationError("Risk-limit policy is invalid") from None
    if not isinstance(policy, EffectiveDatedPortfolioRiskLimitPolicy):
        raise ValidationError("Risk-limit policy loader returned an invalid version")
    validate_policy_range(
        policy,
        start_date=start_date,
        end_date=end_date,
    )
    if policy.portfolio_id != mandate.portfolio_id:
        raise ValidationError(
            "Risk-limit policy portfolio does not match the selected mandate"
        )

    common_cycle_arguments = [
        "--portfolio-id",
        mandate.portfolio_id,
        "--policy-id",
        policy.policy_id,
        "--portfolio-config",
        portfolio_config_path.as_posix(),
        "--limits-config",
        limits_config_path.as_posix(),
        "--storage-config",
        storage_config_path.as_posix(),
        "--start-date",
        start_date.isoformat(),
        "--end-date",
        end_date.isoformat(),
        "--covariance-window",
        str(policy.covariance_window),
        "--vol-window",
        str(volatility_window),
        "--var-window",
        str(var_window),
        "--var-confidence",
        str(var_confidence),
        "--max-snapshots",
        str(max_snapshots),
        "--max-evaluations",
        str(max_evaluations),
        "--summary-json",
        DEFAULT_CYCLE_SUMMARY,
    ]
    dry_run_arguments = [*common_cycle_arguments, "--dry-run"]

    steps = [
        _step(
            step_id="01-review-dry-run",
            description=(
                "Validate mandate and policy governance and generate the stage "
                "plan without creating a lease or invoking analytics."
            ),
            depends_on=[],
            commands=[
                _python_command(
                    "src.orchestration.run_governed_portfolio_cycle",
                    dry_run_arguments,
                    python_command=python_command,
                )
            ],
            reads=[
                "portfolio_configuration",
                "risk_limit_configuration",
                "storage_configuration",
            ],
            writes=["local_governed_cycle_dry_run_summary"],
            preconditions=[
                "human_reviews_this_plan",
                "configuration_hashes_match",
            ],
        ),
        _step(
            step_id="02-run-governed-cycle",
            description=(
                "Run portfolio risk, rolling attribution, and risk-limit "
                "evaluation under one local outer lease."
            ),
            depends_on=["01-review-dry-run"],
            commands=[
                _python_command(
                    "src.orchestration.run_governed_portfolio_cycle",
                    common_cycle_arguments,
                    python_command=python_command,
                )
            ],
            reads=[
                "local_daily_returns_parquet",
                "portfolio_configuration",
                "risk_limit_configuration",
                "storage_configuration",
            ],
            writes=[
                "local_portfolio_risk_parquet",
                "local_portfolio_attribution_parquet",
                "local_risk_limit_evaluation_parquet",
                "local_governed_cycle_summary",
            ],
            preconditions=[
                "execution_is_explicitly_authorized_outside_this_plan",
                "required_local_daily_returns_exist",
                "no_overlapping_governed_cycle_lease",
            ],
        ),
        _step(
            step_id="03-load-local-warehouse",
            description=(
                "Apply local schemas and load versioned portfolio, attribution, "
                "and risk-limit evidence into local PostgreSQL."
            ),
            depends_on=["02-run-governed-cycle"],
            commands=[_make_command("portfolio-risk-limits-warehouse-load")],
            reads=["local_curated_parquet"],
            writes=["local_postgresql_risk_platform"],
            preconditions=[
                "local_postgresql_is_running",
                "warehouse_dsn_is_operator_controlled",
            ],
        ),
        _step(
            step_id="04-reconcile-local-warehouse",
            description=(
                "Run focused local PostgreSQL consistency checks for risk-limit "
                "evidence and current serving views."
            ),
            depends_on=["03-load-local-warehouse"],
            commands=[_make_command("check-portfolio-risk-limits-consistency")],
            reads=["local_postgresql_risk_platform"],
            writes=["operator_visible_reconciliation_output"],
            preconditions=["local_postgresql_load_completed"],
        ),
    ]

    unsigned_plan: dict[str, Any] = {
        "schema_version": PLAN_SCHEMA_VERSION,
        "plan_type": "governed-local-portfolio-risk-workflow",
        "execution_authorized": False,
        "requires_human_review": True,
        "selection": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "mandate": mandate_metadata(mandate),
            "policy_version": policy_metadata(policy),
        },
        "parameters": {
            "volatility_window": volatility_window,
            "var_window": var_window,
            "var_confidence": var_confidence,
            "covariance_window": policy.covariance_window,
            "max_snapshots": max_snapshots,
            "max_evaluations": max_evaluations,
        },
        "configuration_evidence": {
            "portfolio": portfolio_config,
            "risk_limits": limits_config,
            "storage": storage_config,
        },
        "controls": {
            "provider_requests": 0,
            "external_delivery_attempts": 0,
            "cloud_mutations": 0,
            "terraform_apply": False,
            "deployment": False,
            "trading_mutation": False,
            "acknowledgement_mutation": False,
            "executor_included": False,
        },
        "declared_preconditions": [
            "daily_return_history_is_already_available_locally",
            "operator_reviews_configuration_hashes_and_step_effects",
            "operator_separately_authorizes_any_mutating_step",
        ],
        "steps": steps,
    }
    digest = hashlib.sha256(
        _canonical_json(unsigned_plan).encode("utf-8")
    ).hexdigest()[:24]
    return {
        "plan_id": f"portfolio-risk-workflow-plan-{digest}",
        **unsigned_plan,
    }


def _reject_symlink_ancestors(path: Path) -> None:
    current = path
    while True:
        if current.exists() and current.is_symlink():
            raise StorageError("Workflow plan output path must not use symbolic links")
        if current == current.parent:
            break
        current = current.parent


def write_portfolio_risk_workflow_plan(
    path: Path,
    plan: Mapping[str, Any],
) -> int:
    if path.suffix.lower() != ".json":
        raise StorageError("Workflow plan output must use a .json suffix")
    if not isinstance(plan, Mapping) or not isinstance(plan.get("plan_id"), str):
        raise ValidationError("Workflow plan must contain a plan_id")
    _reject_symlink_ancestors(path.parent)
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise StorageError("Workflow plan output must be a regular file")

    payload = json.dumps(
        dict(plan),
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == payload:
                return 0
        except OSError:
            raise StorageError("Existing workflow plan could not be read") from None

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _reject_symlink_ancestors(path.parent)
        handle = tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        )
        temporary_path = Path(handle.name)
        try:
            with handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary_path, 0o600)
            os.replace(temporary_path, path)
        finally:
            temporary_path.unlink(missing_ok=True)
    except OSError:
        raise StorageError("Workflow plan could not be written atomically") from None
    return 1
