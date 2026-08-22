from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from src.analytics.portfolio_risk_limits import (
    MODEL_VERSION as RISK_LIMIT_MODEL_VERSION,
    VOLATILITY_METRIC,
)
from src.orchestration.run_portfolio_risk_notification_outbox import (
    OUTPUT_DATASET,
    run_portfolio_risk_notification_outbox,
)
from src.warehouse.portfolio_risk_notification_outbox_loader import (
    collect_notification_events,
)
from tests.storage_config_helpers import (
    build_storage_config,
    write_storage_config,
)


def _transitions() -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for day, transition, previous, status, observed in (
        (1, "opened", None, "warning", 0.35),
        (2, "escalated", "warning", "critical", 0.50),
        (3, "resolved", "critical", "ok", 0.20),
    ):
        ts_event = datetime(2026, 1, day, tzinfo=timezone.utc)
        records.append(
            {
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
                "warning_threshold": 0.30,
                "critical_threshold": 0.45,
                "status": status,
                "is_breach": status != "ok",
                "breach_threshold": (
                    None
                    if status == "ok"
                    else 0.30 if status == "warning"
                    else 0.45
                ),
                "breach_excess": (
                    0.0
                    if status == "ok"
                    else observed - (0.30 if status == "warning" else 0.45)
                ),
                "previous_status": previous,
                "previous_calculation_id": (
                    None if previous is None else f"evaluation-{day - 1}"
                ),
                "previous_subject_key": (
                    None if previous is None else "us-tech-equal"
                ),
                "transition_type": transition,
                "severity_rank": {
                    "ok": 0,
                    "warning": 1,
                    "critical": 2,
                }[status],
                "subject_changed": False,
            }
        )
    return records


def test_notification_outbox_parquet_replays_and_loads(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)

    first = run_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=None,
        end_date=date(2026, 1, 3),
        max_events=10,
        dsn="postgresql://unused",
        storage_config_path=storage_config_path,
        reader=lambda **_: _transitions(),
        storage_config_loader=lambda _: storage_config,
    )
    second = run_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=None,
        end_date=date(2026, 1, 3),
        max_events=10,
        dsn="postgresql://unused",
        storage_config_path=storage_config_path,
        reader=lambda **_: _transitions(),
        storage_config_loader=lambda _: storage_config,
    )

    first_output = first["curated_output"][OUTPUT_DATASET]
    second_output = second["curated_output"][OUTPUT_DATASET]
    assert first_output["records_written"] == 3
    assert second_output["records_written"] == 0
    assert second_output["records_already_present"] == 3
    assert second["delivery"]["performed"] is False

    records = collect_notification_events(storage_config_path)
    assert len(records) == 3
    assert len({record["event_id"] for record in records}) == 3
    assert {record["event_type"] for record in records} == {
        "breach_opened",
        "breach_escalated",
        "breach_resolved",
    }
    assert all(
        record["delivery_disposition"] == "pending"
        for record in records
    )
