from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from datetime import date
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
from .portfolio_risk_workflow_plan import (
    DEFAULT_CYCLE_DRY_RUN_SUMMARY,
    DEFAULT_CYCLE_SUMMARY,
    MAX_CONFIG_BYTES,
    PLAN_SCHEMA_VERSION,
)

VERIFICATION_SCHEMA_VERSION = "portfolio-risk-workflow-verification-v1"
MAX_PLAN_BYTES = 2_000_000
EXPECTED_STEP_IDS = (
    "01-review-dry-run",
    "02-run-governed-cycle",
    "03-load-local-warehouse",
    "04-reconcile-local-warehouse",
)
CYCLE_MODULE = "src.orchestration.run_governed_portfolio_cycle"
ALLOWED_CYCLE_OPTIONS = frozenset(
    {
        "--portfolio-id",
        "--policy-id",
        "--portfolio-config",
        "--limits-config",
        "--storage-config",
        "--start-date",
        "--end-date",
        "--covariance-window",
        "--vol-window",
        "--var-window",
        "--var-confidence",
        "--max-snapshots",
        "--max-evaluations",
        "--summary-json",
    }
)


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _derived_id(prefix: str, payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()[:24]
    return f"{prefix}-{digest}"


def _reject_symlink_ancestors(path: Path, label: str) -> None:
    current = path
    while True:
        if current.exists() and current.is_symlink():
            raise StorageError(f"{label} must not use symbolic links")
        if current == current.parent:
            break
        current = current.parent


def _regular_file_bytes(path: Path, label: str, maximum: int) -> bytes:
    _reject_symlink_ancestors(path.parent, label)
    if path.is_symlink() or not path.exists() or not path.is_file():
        raise StorageError(f"{label} must be a regular file")
    try:
        size = path.stat().st_size
        if size > maximum:
            raise StorageError(f"{label} exceeds the {maximum}-byte limit")
        return path.read_bytes()
    except StorageError:
        raise
    except OSError:
        raise StorageError(f"{label} could not be read") from None


def _json_without_duplicate_keys(payload: bytes, label: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValidationError(f"{label} contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        return json.loads(payload, object_pairs_hook=reject_duplicates)
    except ValidationError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValidationError(f"{label} must contain valid UTF-8 JSON") from None


def _required_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    if not all(isinstance(key, str) for key in value):
        raise ValidationError(f"{label} must use text keys")
    return dict(value)


def _required_list(value: Any, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be a list")
    return value


def _required_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} must not contain control characters")
    return parsed


def _strict_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be a calendar date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        ) from None
    if value != parsed.isoformat():
        raise ValidationError(
            f"{label} must be a calendar date in YYYY-MM-DD format"
        )
    return parsed


def _verify_plan_identity(plan: dict[str, Any]) -> None:
    supplied_id = plan.get("plan_id")
    unsigned = dict(plan)
    unsigned.pop("plan_id", None)
    expected_id = _derived_id("portfolio-risk-workflow-plan", unsigned)
    if not isinstance(supplied_id, str) or supplied_id != expected_id:
        raise ValidationError("Workflow plan identity does not match its body")


def _verify_controls(plan: dict[str, Any]) -> None:
    if plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise ValidationError("Workflow plan schema version is unsupported")
    if plan.get("plan_type") != "governed-local-portfolio-risk-workflow":
        raise ValidationError("Workflow plan type is unsupported")
    if plan.get("execution_authorized") is not False:
        raise ValidationError("Workflow plan must not authorize execution")
    if plan.get("requires_human_review") is not True:
        raise ValidationError("Workflow plan must require human review")
    controls = _required_mapping(plan.get("controls"), "controls")
    expected = {
        "provider_requests": 0,
        "external_delivery_attempts": 0,
        "cloud_mutations": 0,
        "terraform_apply": False,
        "deployment": False,
        "trading_mutation": False,
        "acknowledgement_mutation": False,
        "executor_included": False,
    }
    if controls != expected:
        raise ValidationError("Workflow plan controls are not fail-closed")


def _parse_cycle_command(command: Any, *, dry_run: bool) -> dict[str, str]:
    candidate = _required_mapping(command, "cycle command")
    _required_text(candidate.get("program"), "cycle command program")
    argv = _required_list(candidate.get("argv"), "cycle command argv")
    if any(not isinstance(item, str) for item in argv):
        raise ValidationError("cycle command argv must contain only text")
    if len(argv) < 2 or argv[:2] != ["-m", CYCLE_MODULE]:
        raise ValidationError("cycle command module is not allowed")

    parsed: dict[str, str] = {}
    index = 2
    saw_dry_run = False
    while index < len(argv):
        option = argv[index]
        if option == "--dry-run":
            if saw_dry_run:
                raise ValidationError("cycle command repeats --dry-run")
            saw_dry_run = True
            index += 1
            continue
        if option not in ALLOWED_CYCLE_OPTIONS:
            raise ValidationError(f"cycle command option '{option}' is not allowed")
        if option in parsed or index + 1 >= len(argv):
            raise ValidationError("cycle command options must be unique key/value pairs")
        value = argv[index + 1]
        if not isinstance(value, str) or not value:
            raise ValidationError("cycle command option values must be non-empty text")
        parsed[option] = value
        index += 2

    if saw_dry_run is not dry_run:
        raise ValidationError("cycle command dry-run mode does not match its step")
    if set(parsed) != ALLOWED_CYCLE_OPTIONS:
        missing = sorted(ALLOWED_CYCLE_OPTIONS.difference(parsed))
        raise ValidationError(
            "cycle command is missing required options: " + ", ".join(missing)
        )
    return parsed


def _verify_effects(step: dict[str, Any], expected: dict[str, list[str]]) -> None:
    effects = _required_mapping(step.get("declared_effects"), "declared effects")
    if effects != expected:
        raise ValidationError(
            f"Workflow step '{step.get('step_id')}' declares unexpected effects"
        )


def _verify_steps(plan: dict[str, Any]) -> dict[str, str]:
    steps = _required_list(plan.get("steps"), "steps")
    if len(steps) != len(EXPECTED_STEP_IDS):
        raise ValidationError("Workflow plan must contain exactly four steps")
    mapped = [_required_mapping(step, "workflow step") for step in steps]
    if tuple(step.get("step_id") for step in mapped) != EXPECTED_STEP_IDS:
        raise ValidationError("Workflow plan step order is not allowed")

    expected_dependencies = (
        [],
        ["01-review-dry-run"],
        ["02-run-governed-cycle"],
        ["03-load-local-warehouse"],
    )
    for step, expected in zip(mapped, expected_dependencies, strict=True):
        if step.get("depends_on") != expected:
            raise ValidationError(
                f"Workflow step '{step['step_id']}' has invalid dependencies"
            )

    _verify_effects(
        mapped[0],
        {
            "reads": [
                "portfolio_configuration",
                "risk_limit_configuration",
                "storage_configuration",
            ],
            "writes": ["local_governed_cycle_dry_run_summary"],
        },
    )
    _verify_effects(
        mapped[1],
        {
            "reads": [
                "local_daily_returns_parquet",
                "portfolio_configuration",
                "risk_limit_configuration",
                "storage_configuration",
            ],
            "writes": [
                "local_portfolio_risk_parquet",
                "local_portfolio_attribution_parquet",
                "local_risk_limit_evaluation_parquet",
                "local_governed_cycle_summary",
            ],
        },
    )
    _verify_effects(
        mapped[2],
        {
            "reads": ["local_curated_parquet"],
            "writes": ["local_postgresql_risk_platform"],
        },
    )
    _verify_effects(
        mapped[3],
        {
            "reads": ["local_postgresql_risk_platform"],
            "writes": ["operator_visible_reconciliation_output"],
        },
    )

    expected_preconditions = (
        ["human_reviews_this_plan", "configuration_hashes_match"],
        [
            "execution_is_explicitly_authorized_outside_this_plan",
            "required_local_daily_returns_exist",
            "no_overlapping_governed_cycle_lease",
        ],
        ["local_postgresql_is_running", "warehouse_dsn_is_operator_controlled"],
        ["local_postgresql_load_completed"],
    )
    for step, expected in zip(mapped, expected_preconditions, strict=True):
        if step.get("preconditions") != expected:
            raise ValidationError(
                f"Workflow step '{step['step_id']}' has invalid preconditions"
            )

    dry_commands = _required_list(mapped[0].get("commands"), "dry-run commands")
    cycle_commands = _required_list(mapped[1].get("commands"), "cycle commands")
    if len(dry_commands) != 1 or len(cycle_commands) != 1:
        raise ValidationError("Workflow cycle steps must contain one command each")
    dry_options = _parse_cycle_command(dry_commands[0], dry_run=True)
    cycle_options = _parse_cycle_command(cycle_commands[0], dry_run=False)
    if dry_options["--summary-json"] != DEFAULT_CYCLE_DRY_RUN_SUMMARY:
        raise ValidationError("Dry-run summary path is not allowed")
    if cycle_options["--summary-json"] != DEFAULT_CYCLE_SUMMARY:
        raise ValidationError("Cycle summary path is not allowed")
    for option in ALLOWED_CYCLE_OPTIONS.difference({"--summary-json"}):
        if dry_options[option] != cycle_options[option]:
            raise ValidationError("Dry-run and cycle commands must use identical inputs")

    expected_make_commands = (
        (mapped[2], "portfolio-risk-limits-warehouse-load"),
        (mapped[3], "check-portfolio-risk-limits-consistency"),
    )
    for step, target in expected_make_commands:
        commands = _required_list(step.get("commands"), "make commands")
        if commands != [{"program": "make", "argv": [target]}]:
            raise ValidationError(
                f"Workflow step '{step['step_id']}' command is not allowed"
            )
    return cycle_options


def _verify_configuration_evidence(plan: dict[str, Any]) -> dict[str, Path]:
    evidence = _required_mapping(
        plan.get("configuration_evidence"),
        "configuration evidence",
    )
    if set(evidence) != {"portfolio", "risk_limits", "storage"}:
        raise ValidationError("Workflow plan configuration evidence is incomplete")

    paths: dict[str, Path] = {}
    for key in ("portfolio", "risk_limits", "storage"):
        item = _required_mapping(evidence[key], f"{key} configuration evidence")
        if set(item) != {"path", "sha256", "bytes"}:
            raise ValidationError(f"{key} configuration evidence has unexpected fields")
        path = Path(_required_text(item.get("path"), f"{key} configuration path"))
        payload = _regular_file_bytes(path, f"{key} configuration", MAX_CONFIG_BYTES)
        if item.get("bytes") != len(payload):
            raise ValidationError(f"{key} configuration byte count has changed")
        digest = hashlib.sha256(payload).hexdigest()
        if item.get("sha256") != digest:
            raise ValidationError(f"{key} configuration hash has changed")
        paths[key] = path
    return paths


def _verify_governance(
    plan: dict[str, Any],
    paths: dict[str, Path],
    cycle_options: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    selection = _required_mapping(plan.get("selection"), "selection")
    parameters = _required_mapping(plan.get("parameters"), "parameters")
    start_date = _strict_date(selection.get("start_date"), "selection start_date")
    end_date = _strict_date(selection.get("end_date"), "selection end_date")
    if start_date > end_date:
        raise ValidationError("Workflow plan date range is reversed")

    planned_mandate = _required_mapping(selection.get("mandate"), "mandate metadata")
    planned_policy = _required_mapping(
        selection.get("policy_version"),
        "policy metadata",
    )
    portfolio_id = _required_text(
        planned_mandate.get("portfolio_id"),
        "mandate portfolio_id",
    )
    policy_id = _required_text(planned_policy.get("policy_id"), "policy_id")

    mandate = load_portfolio_mandate(paths["portfolio"], portfolio_id, end_date)
    if not isinstance(mandate, PortfolioMandate):
        raise ValidationError("Portfolio mandate loader returned an invalid mandate")
    validate_mandate_range(mandate, start_date=start_date, end_date=end_date)
    current_mandate = mandate_metadata(mandate)
    if current_mandate != planned_mandate:
        raise ValidationError("Selected portfolio mandate no longer matches the plan")

    policy = load_effective_portfolio_risk_limit_policy(
        paths["risk_limits"],
        policy_id,
        end_date,
    )
    if not isinstance(policy, EffectiveDatedPortfolioRiskLimitPolicy):
        raise ValidationError("Risk-limit policy loader returned an invalid version")
    validate_policy_range(policy, start_date=start_date, end_date=end_date)
    current_policy = policy_metadata(policy)
    if current_policy != planned_policy:
        raise ValidationError("Selected risk-limit policy no longer matches the plan")
    if policy.portfolio_id != mandate.portfolio_id:
        raise ValidationError("Risk-limit policy no longer matches the mandate")

    expected_options = {
        "--portfolio-id": mandate.portfolio_id,
        "--policy-id": policy.policy_id,
        "--portfolio-config": paths["portfolio"].as_posix(),
        "--limits-config": paths["risk_limits"].as_posix(),
        "--storage-config": paths["storage"].as_posix(),
        "--start-date": start_date.isoformat(),
        "--end-date": end_date.isoformat(),
        "--covariance-window": str(policy.covariance_window),
        "--vol-window": str(parameters.get("volatility_window")),
        "--var-window": str(parameters.get("var_window")),
        "--var-confidence": str(parameters.get("var_confidence")),
        "--max-snapshots": str(parameters.get("max_snapshots")),
        "--max-evaluations": str(parameters.get("max_evaluations")),
    }
    for option, expected in expected_options.items():
        if cycle_options.get(option) != expected:
            raise ValidationError(
                f"Cycle command option '{option}' does not match plan metadata"
            )
    if parameters.get("covariance_window") != policy.covariance_window:
        raise ValidationError("Plan covariance window does not match the policy")
    return current_mandate, current_policy


def load_and_verify_portfolio_risk_workflow_plan(path: Path) -> dict[str, Any]:
    payload = _regular_file_bytes(path, "workflow plan", MAX_PLAN_BYTES)
    plan = _required_mapping(
        _json_without_duplicate_keys(payload, "workflow plan"),
        "workflow plan",
    )
    _verify_plan_identity(plan)
    _verify_controls(plan)
    cycle_options = _verify_steps(plan)
    paths = _verify_configuration_evidence(plan)
    mandate, policy = _verify_governance(plan, paths, cycle_options)

    checks = [
        {"check": "plan_identity", "status": "pass"},
        {"check": "fail_closed_controls", "status": "pass"},
        {"check": "command_allowlist", "status": "pass"},
        {"check": "step_dependencies_and_effects", "status": "pass"},
        {"check": "configuration_hashes", "status": "pass"},
        {"check": "temporal_mandate", "status": "pass"},
        {"check": "temporal_risk_limit_policy", "status": "pass"},
    ]
    unsigned_report: dict[str, Any] = {
        "schema_version": VERIFICATION_SCHEMA_VERSION,
        "plan_id": plan["plan_id"],
        "verified": True,
        "execution_authorized": False,
        "requires_human_review": True,
        "configuration_evidence": plan["configuration_evidence"],
        "selection": {
            "start_date": plan["selection"]["start_date"],
            "end_date": plan["selection"]["end_date"],
            "mandate": mandate,
            "policy_version": policy,
        },
        "checks": checks,
        "verified_check_count": len(checks),
        "executor_included": False,
    }
    return {
        "verification_id": _derived_id(
            "portfolio-risk-workflow-verification",
            unsigned_report,
        ),
        **unsigned_report,
    }


def _validated_report(report: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(report, Mapping):
        raise ValidationError("Verification report must be a mapping")
    candidate = dict(report)
    supplied_id = candidate.pop("verification_id", None)
    expected_id = _derived_id(
        "portfolio-risk-workflow-verification",
        candidate,
    )
    if not isinstance(supplied_id, str) or supplied_id != expected_id:
        raise ValidationError("Verification report identity does not match its body")
    if candidate.get("verified") is not True:
        raise ValidationError("Verification report must represent a verified plan")
    if candidate.get("execution_authorized") is not False:
        raise ValidationError("Verification report must not authorize execution")
    if candidate.get("executor_included") is not False:
        raise ValidationError("Verification report must not include an executor")
    return {"verification_id": supplied_id, **candidate}


def write_portfolio_risk_workflow_verification(
    path: Path,
    report: Mapping[str, Any],
) -> int:
    if path.suffix.lower() != ".json":
        raise StorageError("Verification output must use a .json suffix")
    validated = _validated_report(report)
    _reject_symlink_ancestors(path.parent, "verification output path")
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise StorageError("Verification output must be a regular file")

    payload = json.dumps(
        validated,
        indent=2,
        sort_keys=True,
        allow_nan=False,
    ) + "\n"
    if path.exists():
        try:
            if path.read_text(encoding="utf-8") == payload:
                return 0
        except OSError:
            raise StorageError("Existing verification output could not be read") from None

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _reject_symlink_ancestors(path.parent, "verification output path")
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
        raise StorageError("Verification output could not be written atomically") from None
    return 1
