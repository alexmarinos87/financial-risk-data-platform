from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from src.analytics.portfolio_risk_limits import (
    MODEL_VERSION as RISK_LIMIT_MODEL_VERSION,
    VOLATILITY_METRIC,
)
from src.analytics.portfolio_risk_notification_outbox import (
    build_portfolio_risk_notification_outbox,
)
from src.common.exceptions import StorageError
from src.storage.s3_writer import write_records
from src.warehouse.portfolio_risk_notification_outbox_loader import (
    DATASET_KEY,
    build_insert_sql,
    collect_notification_events,
    load_notification_events_to_postgres,
)
from tests.storage_config_helpers import (
    build_storage_config,
    write_storage_config,
)


def _transition() -> dict[str, object]:
    ts_event = datetime(2026, 1, 2, tzinfo=timezone.utc)
    return {
        "calculation_id": "evaluation-2",
        "model_version": RISK_LIMIT_MODEL_VERSION,
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "risk-limit-policy-a",
        "portfolio_id": "us-tech-equal",
        "base_currency": "USD",
        "definition_fingerprint": "definition-a",
        "attribution_calculation_id": "attribution-2",
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
        "observed_value": 0.35,
        "observed_signed_value": 0.35,
        "warning_threshold": 0.30,
        "critical_threshold": 0.45,
        "status": "warning",
        "is_breach": True,
        "breach_threshold": 0.30,
        "breach_excess": 0.05,
        "previous_status": "ok",
        "previous_calculation_id": "evaluation-1",
        "previous_subject_key": "us-tech-equal",
        "transition_type": "opened",
        "severity_rank": 1,
        "subject_changed": False,
    }


def test_loader_collects_outbox_payload_and_uses_immutable_insert(
    tmp_path: Path,
) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    event = build_portfolio_risk_notification_outbox([_transition()]).events[0]

    assert write_records(
        [event],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=storage_config,
    ) == 1

    records = collect_notification_events(storage_config_path)

    assert len(records) == 1
    assert records[0]["event_id"] == event["event_id"]
    assert records[0]["payload_json"] == event["payload_json"]
    statement = build_insert_sql()
    assert (
        'INSERT INTO "risk_platform"."portfolio_risk_notification_outbox"'
        in statement
    )
    assert 'ON CONFLICT ("event_id") DO NOTHING' in statement
    assert "DO UPDATE" not in statement

    assert load_notification_events_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=storage_config_path,
        dry_run=True,
    ) == 1


def test_loader_is_safe_when_dataset_is_absent(tmp_path: Path) -> None:
    storage_config_path = write_storage_config(tmp_path)

    assert collect_notification_events(storage_config_path) == []
    assert load_notification_events_to_postgres(
        dsn="postgresql://unused",
        storage_config_path=storage_config_path,
        dry_run=True,
    ) == 0


def test_loader_rejects_invalid_payload_json(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    event = dict(
        build_portfolio_risk_notification_outbox([_transition()]).events[0]
    )
    event["payload_json"] = "not-json"

    assert write_records(
        [event],
        kind="curated",
        dataset=DATASET_KEY,
        storage_config=storage_config,
    ) == 1

    with pytest.raises(StorageError, match="invalid JSON"):
        collect_notification_events(storage_config_path)


def test_loader_rejects_symbolic_link_dataset_path(tmp_path: Path) -> None:
    storage_config = build_storage_config(tmp_path)
    storage_config_path = write_storage_config(tmp_path)
    curated_base = Path(storage_config["storage"]["curated"]["base_path"])
    curated_base.mkdir(parents=True)
    target = tmp_path / "external"
    target.mkdir()
    (curated_base / DATASET_KEY).symlink_to(
        target,
        target_is_directory=True,
    )

    with pytest.raises(StorageError, match="symbolic link"):
        collect_notification_events(storage_config_path)
