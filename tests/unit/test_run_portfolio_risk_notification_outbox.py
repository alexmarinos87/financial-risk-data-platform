from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from src.analytics.portfolio_risk_limits import (
    MODEL_VERSION as RISK_LIMIT_MODEL_VERSION,
    VOLATILITY_METRIC,
)
from src.common.exceptions import StorageError
from src.orchestration.run_portfolio_risk_notification_outbox import (
    OUTPUT_DATASET,
    run_portfolio_risk_notification_outbox,
)


def _transition(
    day: int,
    transition_type: str,
    previous: str | None,
    status: str,
) -> dict[str, object]:
    ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
    observed = 0.2 if status == "ok" else 0.35 if status == "warning" else 0.5
    return {
        "calculation_id": f"evaluation-{day}",
        "model_version": RISK_LIMIT_MODEL_VERSION,
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "risk-limit-policy-a",
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": "definition-a",
        "attribution_calculation_id": f"attribution-{day}",
        "attribution_model_version": "portfolio-attribution-v1",
        "weighting_method": "constant_weight_daily_rebalanced",
        "covariance_method": "sample_annualized",
        "correlation_method": "pearson",
        "covariance_window": 20,
        "annualization_days": 252,
        "ts_event": ts_event,
        "ts_ingest": ts_event + timedelta(hours=1),
        "metric_name": VOLATILITY_METRIC,
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "unit": "annualized_decimal",
        "observed_value": observed,
        "observed_signed_value": observed,
        "warning_threshold": 0.3,
        "critical_threshold": 0.45,
        "status": status,
        "is_breach": status != "ok",
        "breach_threshold": (
            None
            if status == "ok"
            else 0.3 if status == "warning" else 0.45
        ),
        "breach_excess": (
            0.0
            if status == "ok"
            else observed - (0.3 if status == "warning" else 0.45)
        ),
        "previous_status": previous,
        "previous_calculation_id": (
            None if previous is None else f"evaluation-{day - 1}"
        ),
        "previous_subject_key": (
            None if previous is None else "us-tech-equal"
        ),
        "transition_type": transition_type,
        "severity_rank": {"ok": 0, "warning": 1, "critical": 2}[status],
        "subject_changed": False,
    }


def _storage_config() -> dict[str, Any]:
    return {
        "storage": {
            "base_dir": "unused",
            "raw": {
                "base_path": "unused/raw",
                "dataset": "market_events",
            },
            "curated": {
                "base_path": "unused/curated",
                "datasets": {
                    "portfolio_risk_limit_evaluations": (
                        "portfolio_risk_limit_evaluations"
                    ),
                    OUTPUT_DATASET: OUTPUT_DATASET,
                },
            },
            "format": "parquet",
            "partitioning": {"granularity": "hourly"},
        }
    }


def test_runner_publishes_candidates_without_delivery() -> None:
    writes: list[dict[str, Any]] = []
    records = [
        _transition(1, "opened", None, "warning"),
        _transition(2, "escalated", "warning", "critical"),
        _transition(3, "resolved", "critical", "ok"),
    ]

    def reader(**kwargs: Any) -> list[dict[str, Any]]:
        assert kwargs["policy_id"] == "us-tech-standard"
        assert kwargs["end_date"] == date(2026, 1, 3)
        assert kwargs["dsn"] == "postgresql://not-printed"
        return records

    def writer(
        events: list[dict[str, Any]],
        *,
        dataset: str,
        **_: Any,
    ) -> int:
        assert dataset == OUTPUT_DATASET
        writes.extend(events)
        return 1

    summary = run_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=None,
        end_date=date(2026, 1, 3),
        max_events=10,
        dsn="postgresql://not-printed",
        storage_config_path=Path("unused.yaml"),
        reader=reader,
        writer=writer,
        storage_config_loader=lambda _: _storage_config(),
    )

    assert summary["curated_output"][OUTPUT_DATASET]["records_written"] == 3
    assert summary["curated_output"][OUTPUT_DATASET][
        "pending_delivery_candidates"
    ] == 3
    assert summary["delivery"] == {
        "performed": False,
        "external_destinations": 0,
        "reason": "delivery_not_implemented",
    }
    assert "postgresql" not in str(summary).lower()
    assert len(writes) == 3


def test_runner_reports_replay_and_suppressed_candidates() -> None:
    records = [
        _transition(1, "opened", None, "critical"),
        _transition(2, "deescalated", "critical", "warning"),
    ]

    summary = run_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=None,
        end_date=date(2026, 1, 2),
        max_events=10,
        dsn="postgresql://unused",
        storage_config_path=Path("unused.yaml"),
        reader=lambda **_: records,
        writer=lambda *_args, **_kwargs: 0,
        storage_config_loader=lambda _: _storage_config(),
    )

    output = summary["curated_output"][OUTPUT_DATASET]
    assert output["records_already_present"] == 2
    assert output["pending_delivery_candidates"] == 1
    assert output["suppressed_candidates"] == 1


def test_runner_requires_output_dataset() -> None:
    config = _storage_config()
    del config["storage"]["curated"]["datasets"][OUTPUT_DATASET]

    with pytest.raises(StorageError, match=OUTPUT_DATASET):
        run_portfolio_risk_notification_outbox(
            policy_id="us-tech-standard",
            start_date=None,
            end_date=date(2026, 1, 1),
            max_events=10,
            dsn="postgresql://unused",
            storage_config_path=Path("unused.yaml"),
            reader=lambda **_: [
                _transition(1, "opened", None, "warning")
            ],
            writer=lambda *_args, **_kwargs: 1,
            storage_config_loader=lambda _: config,
        )


def test_runner_rejects_invalid_writer_result() -> None:
    with pytest.raises(StorageError, match="invalid result"):
        run_portfolio_risk_notification_outbox(
            policy_id="us-tech-standard",
            start_date=None,
            end_date=date(2026, 1, 1),
            max_events=10,
            dsn="postgresql://unused",
            storage_config_path=Path("unused.yaml"),
            reader=lambda **_: [
                _transition(1, "opened", None, "warning")
            ],
            writer=lambda *_args, **_kwargs: 2,
            storage_config_loader=lambda _: _storage_config(),
        )
