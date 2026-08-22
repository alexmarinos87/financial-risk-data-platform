from __future__ import annotations

import json
from pathlib import Path

import yaml

from src.orchestration.plan_portfolio_risk_workflow import main


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


def _arguments(
    portfolio: Path,
    limits: Path,
    storage: Path,
    output: Path,
) -> list[str]:
    return [
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
        str(output),
    ]


def test_cli_writes_plan_and_reports_replay(tmp_path: Path, capsys) -> None:
    portfolio, limits, storage = _write_configs(tmp_path)
    output = tmp_path / "plan.json"
    arguments = _arguments(portfolio, limits, storage, output)

    assert main(arguments) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["records_written"] == 1
    assert first["execution_authorized"] is False
    assert first["requires_human_review"] is True
    assert first["step_count"] == 4

    assert main(arguments) == 0
    second = json.loads(capsys.readouterr().out)
    assert second["records_written"] == 0
    assert second["plan_id"] == first["plan_id"]
    plan = json.loads(output.read_text(encoding="utf-8"))
    assert plan["plan_id"] == first["plan_id"]


def test_cli_reports_governance_failure_without_writing(
    tmp_path: Path,
    capsys,
) -> None:
    portfolio, limits, storage = _write_configs(tmp_path)
    payload = yaml.safe_load(portfolio.read_text(encoding="utf-8"))
    payload["portfolios"]["us-tech-equal"]["effective_from"] = "2026-02-01"
    portfolio.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    output = tmp_path / "plan.json"

    assert main(_arguments(portfolio, limits, storage, output)) == 1
    captured = capsys.readouterr()
    assert "planning failed" in captured.err.lower()
    assert not output.exists()


def test_cli_rejects_invalid_storage_file(tmp_path: Path, capsys) -> None:
    portfolio, limits, storage = _write_configs(tmp_path)
    storage.unlink()
    output = tmp_path / "plan.json"

    assert main(_arguments(portfolio, limits, storage, output)) == 1
    captured = capsys.readouterr()
    assert "regular file" in captured.err
    assert not output.exists()
