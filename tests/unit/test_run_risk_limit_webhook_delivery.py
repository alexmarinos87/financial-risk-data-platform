from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.delivery.risk_limit_webhook import WebhookDeliveryConfig
from src.orchestration.run_risk_limit_webhook_delivery import (
    run_risk_limit_webhook_delivery,
)


def _config(*, enabled: bool) -> WebhookDeliveryConfig:
    return WebhookDeliveryConfig(
        adapter_id="risk-webhook",
        enabled=enabled,
        endpoint_env="ENDPOINT",
        authorization_env="AUTH",
        timeout_seconds=5,
        max_attempts=3,
        initial_backoff_seconds=1,
        max_backoff_seconds=4,
        max_notifications_per_run=10,
    )


def test_missing_explicit_flag_returns_without_reading_notifications() -> None:
    def fail(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("notification reader must not be called")

    summary = run_risk_limit_webhook_delivery(
        adapter_id="risk-webhook",
        delivery_config_path=Path("delivery.yaml"),
        dsn="unused",
        attempts_dir=Path("attempts"),
        config_loader=lambda *_: _config(enabled=True),
        notification_reader=fail,
    )

    assert summary["delivery"] == {
        "performed": False,
        "reason": "explicit_enable_flag_required",
    }
    assert summary["secrets_recorded"] is False


def test_disabled_configuration_cannot_be_overridden_by_flag() -> None:
    summary = run_risk_limit_webhook_delivery(
        adapter_id="risk-webhook",
        delivery_config_path=Path("delivery.yaml"),
        dsn="unused",
        attempts_dir=Path("attempts"),
        enable_external_delivery=True,
        config_loader=lambda *_: _config(enabled=False),
    )

    assert summary["delivery"]["reason"] == (
        "adapter_disabled_in_configuration"
    )


def test_enabled_run_resolves_secret_environment_without_recording_values() -> None:
    calls: list[dict[str, Any]] = []

    def deliverer(notifications: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append({"notifications": notifications, **kwargs})
        return {
            "delivery_performed": True,
            "notifications_selected": 1,
        }

    summary = run_risk_limit_webhook_delivery(
        adapter_id="risk-webhook",
        delivery_config_path=Path("delivery.yaml"),
        dsn="postgresql://example",
        attempts_dir=Path("attempts"),
        enable_external_delivery=True,
        environment={
            "ENDPOINT": "https://example.invalid/hook",
            "AUTH": "Bearer secret-value",
        },
        config_loader=lambda *_: _config(enabled=True),
        notification_reader=lambda **kwargs: [
            {"notification_id": "notification-1"}
        ],
        deliverer=deliverer,
    )

    assert calls[0]["endpoint"] == "https://example.invalid/hook"
    assert calls[0]["authorization"] == "Bearer secret-value"
    assert "secret-value" not in str(summary)
    assert "example.invalid" not in str(summary)
    assert summary["delivery"]["delivery_performed"] is True


def test_missing_required_environment_fails_before_database_read() -> None:
    reader_called = False

    def reader(**_kwargs: Any) -> list[dict[str, Any]]:
        nonlocal reader_called
        reader_called = True
        return []

    with pytest.raises(ValidationError, match="ENDPOINT"):
        run_risk_limit_webhook_delivery(
            adapter_id="risk-webhook",
            delivery_config_path=Path("delivery.yaml"),
            dsn="unused",
            attempts_dir=Path("attempts"),
            enable_external_delivery=True,
            environment={"AUTH": "Bearer secret"},
            config_loader=lambda *_: _config(enabled=True),
            notification_reader=reader,
        )

    assert reader_called is False


def test_nothing_pending_performs_no_delivery() -> None:
    summary = run_risk_limit_webhook_delivery(
        adapter_id="risk-webhook",
        delivery_config_path=Path("delivery.yaml"),
        dsn="unused",
        attempts_dir=Path("attempts"),
        enable_external_delivery=True,
        environment={
            "ENDPOINT": "https://example.invalid/hook",
            "AUTH": "Bearer secret",
        },
        config_loader=lambda *_: _config(enabled=True),
        notification_reader=lambda **_: [],
        deliverer=lambda *_args, **_kwargs: pytest.fail(
            "deliverer must not be called"
        ),
    )

    assert summary["delivery"] == {
        "performed": False,
        "reason": "nothing_pending",
        "notifications_selected": 0,
    }
