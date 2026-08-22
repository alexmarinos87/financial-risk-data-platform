from __future__ import annotations

import hashlib
import json
import stat
from datetime import date
from pathlib import Path

import pytest
import yaml

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.portfolio_risk_workflow_plan import (
    PLAN_SCHEMA_VERSION,
    build_portfolio_risk_workflow_plan,
    write_portfolio_risk_workflow_plan,
)


def _write_portfolio_config(
    tmp_path: Path,
    *,
    effective_from: str = "2026-01-01",
    effective_to: str | None = None,
) -> Path:
    path = tmp_path / "portfolios.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "portfolios": {
                    "us-tech-equal": {
                        "mandate_id": "us-tech-equal-v1",
                        "effective_from": effective_from,
                        "effective_to": effective_to,
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
    return path


def _write_limits_config(
    tmp_path: Path,
    *,
    portfolio_id: str = "us-tech-equal",
    effective_from: str = "2026-01-01",
    effective_to: str | None = None,
) -> Path:
    path = tmp_path / "limits.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "policies": {
                    "us-tech-standard": {
                        "policy_version_id": "us-tech-standard-v1",
                        "effective_from": effective_from,
                        "effective_to": effective_to,
                        "portfolio_id": portfolio_id,
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
    return path


def _write_storage_config(tmp_path: Path) -> Path:
    path = tmp_path / "storage.yaml"
    path.write_text(
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
    return path


def _build(tmp_path: Path, **overrides: object) -> dict[str, object]:
    arguments: dict[str, object] = {
        "portfolio_id": "us-tech-equal",
        "policy_id": "us-tech-standard",
        "start_date": date(2026, 1, 1),
        "end_date": date(2026, 3, 31),
        "portfolio_config_path": _write_portfolio_config(tmp_path),
        "limits_config_path": _write_limits_config(tmp_path),
        "storage_config_path": _write_storage_config(tmp_path),
        "volatility_window": 20,
        "var_window": 60,
        "var_confidence": 0.95,
        "max_snapshots": 100,
        "max_evaluations": 200,
        "python_command": ".venv/bin/python",
    }
    arguments.update(overrides)
    return build_portfolio_risk_workflow_plan(**arguments)  # type: ignore[arg-type]


def test_plan_is_deterministic_non_authorizing_and_self_identifying(
    tmp_path: Path,
) -> None:
    first = _build(tmp_path)
    second = _build(tmp_path)

    assert first == second
    assert first["schema_version"] == PLAN_SCHEMA_VERSION
    assert first["execution_authorized"] is False
    assert first["requires_human_review"] is True
    controls = first["controls"]
    assert controls == {
        "provider_requests": 0,
        "external_delivery_attempts": 0,
        "cloud_mutations": 0,
        "terraform_apply": False,
        "deployment": False,
        "trading_mutation": False,
        "acknowledgement_mutation": False,
        "executor_included": False,
    }

    unsigned = dict(first)
    plan_id = unsigned.pop("plan_id")
    expected = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    assert plan_id == f"portfolio-risk-workflow-plan-{expected}"


def test_plan_declares_ordered_steps_commands_and_effects(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    steps = plan["steps"]

    assert [step["step_id"] for step in steps] == [
        "01-review-dry-run",
        "02-run-governed-cycle",
        "03-load-local-warehouse",
        "04-reconcile-local-warehouse",
    ]
    assert [step["depends_on"] for step in steps] == [
        [],
        ["01-review-dry-run"],
        ["02-run-governed-cycle"],
        ["03-load-local-warehouse"],
    ]

    dry_run_command = steps[0]["commands"][0]
    cycle_command = steps[1]["commands"][0]
    assert dry_run_command["program"] == ".venv/bin/python"
    assert dry_run_command["argv"][:2] == [
        "-m",
        "src.orchestration.run_governed_portfolio_cycle",
    ]
    assert "--dry-run" in dry_run_command["argv"]
    assert "--dry-run" not in cycle_command["argv"]
    assert steps[2]["commands"] == [
        {
            "program": "make",
            "argv": ["portfolio-risk-limits-warehouse-load"],
        }
    ]
    assert steps[3]["commands"] == [
        {
            "program": "make",
            "argv": ["check-portfolio-risk-limits-consistency"],
        }
    ]
    assert "local_postgresql_risk_platform" in steps[2]["declared_effects"][
        "writes"
    ]
    assert "operator_visible_reconciliation_output" in steps[3][
        "declared_effects"
    ]["writes"]


def test_plan_records_temporal_governance_and_config_hashes(tmp_path: Path) -> None:
    portfolio = _write_portfolio_config(tmp_path)
    limits = _write_limits_config(tmp_path)
    storage = _write_storage_config(tmp_path)
    plan = build_portfolio_risk_workflow_plan(
        portfolio_id="us-tech-equal",
        policy_id="us-tech-standard",
        start_date=date(2026, 1, 1),
        end_date=date(2026, 3, 31),
        portfolio_config_path=portfolio,
        limits_config_path=limits,
        storage_config_path=storage,
    )

    selection = plan["selection"]
    assert selection["mandate"]["mandate_id"] == "us-tech-equal-v1"
    assert selection["policy_version"]["policy_version_id"] == (
        "us-tech-standard-v1"
    )
    evidence = plan["configuration_evidence"]
    for key, path in {
        "portfolio": portfolio,
        "risk_limits": limits,
        "storage": storage,
    }.items():
        payload = path.read_bytes()
        assert evidence[key] == {
            "path": path.as_posix(),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }


def test_temporal_boundaries_and_policy_mismatch_fail_closed(
    tmp_path: Path,
) -> None:
    portfolio = _write_portfolio_config(
        tmp_path,
        effective_from="2026-02-01",
    )
    with pytest.raises(ValidationError, match="crosses"):
        _build(tmp_path, portfolio_config_path=portfolio)

    limits = _write_limits_config(
        tmp_path,
        effective_from="2026-02-01",
    )
    with pytest.raises(ValidationError, match="crosses"):
        _build(tmp_path, limits_config_path=limits)

    mismatch = _write_limits_config(tmp_path, portfolio_id="other-portfolio")
    with pytest.raises(ValidationError, match="does not match"):
        _build(tmp_path, limits_config_path=mismatch)


def test_invalid_bounds_and_configuration_paths_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="volatility_window"):
        _build(tmp_path, volatility_window=1)
    with pytest.raises(ValidationError, match="var_confidence"):
        _build(tmp_path, var_confidence=1.0)
    with pytest.raises(ValidationError, match="max_snapshots"):
        _build(tmp_path, max_snapshots=2_501)
    with pytest.raises(StorageError, match="regular file"):
        _build(tmp_path, storage_config_path=tmp_path / "missing.yaml")


def test_atomic_writer_is_replay_safe_and_private(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    output = tmp_path / "evidence" / "plan.json"

    assert write_portfolio_risk_workflow_plan(output, plan) == 1
    assert write_portfolio_risk_workflow_plan(output, plan) == 0
    assert json.loads(output.read_text(encoding="utf-8")) == plan
    assert stat.S_IMODE(output.stat().st_mode) == 0o600

    changed = dict(plan)
    changed["requires_human_review"] = False
    assert write_portfolio_risk_workflow_plan(output, changed) == 1


def test_writer_rejects_non_json_and_symbolic_link_paths(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    with pytest.raises(StorageError, match=".json suffix"):
        write_portfolio_risk_workflow_plan(tmp_path / "plan.txt", plan)

    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "linked"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(StorageError, match="symbolic links"):
        write_portfolio_risk_workflow_plan(link / "plan.json", plan)


def test_plan_does_not_embed_secrets_or_authorize_execution(tmp_path: Path) -> None:
    plan = _build(tmp_path)
    serialized = json.dumps(plan, sort_keys=True).lower()

    for prohibited in (
        "api_key",
        "password",
        "bearer ",
        "access_token",
        "secret_key",
    ):
        assert prohibited not in serialized
    assert "operator_separately_authorizes_any_mutating_step" in serialized
    assert plan["execution_authorized"] is False
