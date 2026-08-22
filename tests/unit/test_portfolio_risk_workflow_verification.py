from __future__ import annotations

import copy
import hashlib
import json
import stat
from datetime import date
from pathlib import Path

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.plan_portfolio_risk_workflow import main as plan_main
from src.orchestration.portfolio_risk_workflow_plan import (
    build_portfolio_risk_workflow_plan,
    write_portfolio_risk_workflow_plan,
)
from src.orchestration.portfolio_risk_workflow_verification import (
    VERIFICATION_SCHEMA_VERSION,
    load_and_verify_portfolio_risk_workflow_plan,
    write_portfolio_risk_workflow_verification,
)
from src.orchestration.verify_portfolio_risk_workflow import main as verify_main


def _write_configs(tmp_path: Path) -> tuple[Path, Path, Path]:
    portfolio = tmp_path / "portfolios.yaml"
    portfolio.write_text(
        yaml.safe_dump(
            {
                "portfolios": {
                    "us-tech-equal": {
                        "mandate_id": "us-tech-equal-v1",
                        "effective_from": "2026-01-01",
                        "effective_to": None,
                        "base_currency": "USD",
                        "constituents": [
                            {
                                "source": "alpha_vantage",
                                "symbol": "AAPL",
                                "weight": 0.5,
                            },
                            {
                                "source": "alpha_vantage",
                                "symbol": "MSFT",
                                "weight": 0.5,
                            },
                        ],
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    limits = tmp_path / "limits.yaml"
    limits.write_text(
        yaml.safe_dump(
            {
                "policies": {
                    "us-tech-standard": {
                        "policy_version_id": "us-tech-standard-v1",
                        "effective_from": "2026-01-01",
                        "effective_to": None,
                        "portfolio_id": "us-tech-equal",
                        "covariance_window": 20,
                        "annualization_days": 252,
                        "limits": {
                            "portfolio_volatility_annualized": {
                                "warning": 0.30,
                                "critical": 0.45,
                            },
                            (
                                "largest_absolute_component_contribution_share"
                            ): {
                                "warning": 0.65,
                                "critical": 0.80,
                            },
                        },
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    storage = tmp_path / "storage.yaml"
    storage.write_text(
        yaml.safe_dump(
            {
                "storage": {
                    "base_dir": str(tmp_path / "data"),
                    "raw": {
                        "base_path": str(tmp_path / "data/raw"),
                        "dataset": "market_events",
                    },
                    "curated": {
                        "base_path": str(tmp_path / "data/curated"),
                        "datasets": {
                            "daily_returns": "daily_returns",
                            "portfolio_daily_returns": "portfolio_daily_returns",
                            "portfolio_daily_risk_summary": (
                                "portfolio_daily_risk_summary"
                            ),
                            "portfolio_risk_attribution": (
                                "portfolio_risk_attribution"
                            ),
                            "portfolio_risk_limit_evaluations": (
                                "portfolio_risk_limit_evaluations"
                            ),
                        },
                    },
                    "format": "parquet",
                    "partitioning": {"granularity": "hourly"},
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return portfolio, limits, storage


def _build_plan(tmp_path: Path) -> tuple[dict[str, object], Path, tuple[Path, ...]]:
    portfolio, limits, storage = _write_configs(tmp_path)
    plan = build_portfolio_risk_workflow_plan(
        portfolio_id="us-tech-equal",
        policy_id="us-tech-standard",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        portfolio_config_path=portfolio,
        limits_config_path=limits,
        storage_config_path=storage,
        volatility_window=20,
        var_window=60,
        var_confidence=0.95,
        max_snapshots=100,
        max_evaluations=200,
    )
    path = tmp_path / "plan.json"
    assert write_portfolio_risk_workflow_plan(path, plan) == 1
    return plan, path, (portfolio, limits, storage)


def _retag_plan(plan: dict[str, object]) -> dict[str, object]:
    candidate = copy.deepcopy(plan)
    unsigned = dict(candidate)
    unsigned.pop("plan_id", None)
    digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    candidate["plan_id"] = f"portfolio-risk-workflow-plan-{digest}"
    return candidate


def _write_untrusted(path: Path, plan: dict[str, object]) -> None:
    path.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def test_verification_is_deterministic_non_authorizing_and_complete(
    tmp_path: Path,
) -> None:
    plan, path, _ = _build_plan(tmp_path)

    first = load_and_verify_portfolio_risk_workflow_plan(path)
    second = load_and_verify_portfolio_risk_workflow_plan(path)

    assert first == second
    assert first["schema_version"] == VERIFICATION_SCHEMA_VERSION
    assert first["plan_id"] == plan["plan_id"]
    assert first["verified"] is True
    assert first["execution_authorized"] is False
    assert first["requires_human_review"] is True
    assert first["executor_included"] is False
    assert first["verified_check_count"] == 7
    assert {item["check"] for item in first["checks"]} == {
        "plan_identity",
        "fail_closed_controls",
        "command_allowlist",
        "step_dependencies_and_effects",
        "configuration_hashes",
        "temporal_mandate",
        "temporal_risk_limit_policy",
    }
    assert all(item["status"] == "pass" for item in first["checks"])
    assert first["verification_id"].startswith(
        "portfolio-risk-workflow-verification-"
    )


def test_configuration_drift_and_temporal_change_fail_verification(
    tmp_path: Path,
) -> None:
    _, path, (_, limits, _) = _build_plan(tmp_path)
    limits.write_text(
        limits.read_text(encoding="utf-8") + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValidationError, match="byte count|hash"):
        load_and_verify_portfolio_risk_workflow_plan(path)


def test_retagged_shell_command_is_rejected_by_allowlist(tmp_path: Path) -> None:
    plan, _, _ = _build_plan(tmp_path)
    forged = copy.deepcopy(plan)
    for index in (0, 1):
        forged["steps"][index]["commands"][0]["program"] = "bash"
    forged = _retag_plan(forged)
    path = tmp_path / "forged-shell.json"
    _write_untrusted(path, forged)

    with pytest.raises(ValidationError, match="Python executable"):
        load_and_verify_portfolio_risk_workflow_plan(path)


def test_retagged_unknown_option_and_dependency_are_rejected(tmp_path: Path) -> None:
    plan, _, _ = _build_plan(tmp_path)
    forged = copy.deepcopy(plan)
    forged["steps"][1]["commands"][0]["argv"].extend(
        ["--api-key", "not-allowed"]
    )
    forged = _retag_plan(forged)
    option_path = tmp_path / "forged-option.json"
    _write_untrusted(option_path, forged)
    with pytest.raises(ValidationError, match="not allowed"):
        load_and_verify_portfolio_risk_workflow_plan(option_path)

    forged = copy.deepcopy(plan)
    forged["steps"][2]["depends_on"] = []
    forged = _retag_plan(forged)
    dependency_path = tmp_path / "forged-dependency.json"
    _write_untrusted(dependency_path, forged)
    with pytest.raises(ValidationError, match="dependencies"):
        load_and_verify_portfolio_risk_workflow_plan(dependency_path)


def test_retagged_controls_and_effects_are_rejected(tmp_path: Path) -> None:
    plan, _, _ = _build_plan(tmp_path)
    forged = copy.deepcopy(plan)
    forged["controls"]["external_delivery_attempts"] = 1
    forged = _retag_plan(forged)
    controls_path = tmp_path / "forged-controls.json"
    _write_untrusted(controls_path, forged)
    with pytest.raises(ValidationError, match="controls"):
        load_and_verify_portfolio_risk_workflow_plan(controls_path)

    forged = copy.deepcopy(plan)
    forged["steps"][1]["declared_effects"]["writes"].append(
        "external_notification_delivery"
    )
    forged = _retag_plan(forged)
    effects_path = tmp_path / "forged-effects.json"
    _write_untrusted(effects_path, forged)
    with pytest.raises(ValidationError, match="unexpected effects"):
        load_and_verify_portfolio_risk_workflow_plan(effects_path)


def test_duplicate_json_keys_and_symlink_plan_are_rejected(tmp_path: Path) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"plan_id":"first","plan_id":"second"}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="duplicate JSON keys"):
        load_and_verify_portfolio_risk_workflow_plan(duplicate)

    _, plan_path, _ = _build_plan(tmp_path)
    link = tmp_path / "plan-link.json"
    link.symlink_to(plan_path)
    with pytest.raises(StorageError, match="regular file"):
        load_and_verify_portfolio_risk_workflow_plan(link)


def test_verification_writer_is_atomic_private_and_replay_safe(
    tmp_path: Path,
) -> None:
    _, plan_path, _ = _build_plan(tmp_path)
    report = load_and_verify_portfolio_risk_workflow_plan(plan_path)
    output = tmp_path / "evidence" / "verification.json"

    assert write_portfolio_risk_workflow_verification(output, report) == 1
    assert write_portfolio_risk_workflow_verification(output, report) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == report
    assert stat.S_IMODE(output.stat().st_mode) == 0o600

    tampered = dict(report)
    tampered["execution_authorized"] = True
    with pytest.raises(ValidationError, match="identity"):
        write_portfolio_risk_workflow_verification(output, tampered)


def test_verification_cli_writes_and_replays_report(tmp_path: Path, capsys) -> None:
    portfolio, limits, storage = _write_configs(tmp_path)
    plan_path = tmp_path / "plan.json"
    plan_args = [
        "--portfolio-id",
        "us-tech-equal",
        "--policy-id",
        "us-tech-standard",
        "--portfolio-config",
        str(portfolio),
        "--limits-config",
        str(limits),
        "--storage-config",
        str(storage),
        "--start-date",
        "2026-01-01",
        "--end-date",
        "2026-03-31",
        "--max-snapshots",
        "100",
        "--max-evaluations",
        "200",
        "--output",
        str(plan_path),
    ]
    assert plan_main(plan_args) == 0
    capsys.readouterr()

    output = tmp_path / "verification.json"
    verify_args = ["--plan", str(plan_path), "--output", str(output)]
    assert verify_main(verify_args) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["verified"] is True
    assert first["execution_authorized"] is False
    assert first["records_written"] == 1

    assert verify_main(verify_args) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["records_written"] == 0
    assert second["verification_id"] == first["verification_id"]


def test_verification_cli_reports_drift_without_writing(
    tmp_path: Path,
    capsys,
) -> None:
    plan, plan_path, (_, limits, _) = _build_plan(tmp_path)
    assert plan["execution_authorized"] is False
    limits.write_text("policies: {}\n", encoding="utf-8")
    output = tmp_path / "verification.json"

    assert verify_main(["--plan", str(plan_path), "--output", str(output)]) == 1
    captured = capsys.readouterr()
    assert "verification failed" in captured.err.lower()
    assert not output.exists()
