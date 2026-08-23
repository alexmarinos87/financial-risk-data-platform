from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.delivery.risk_limit_webhook import (
    WebhookDeliveryConfig,
    WebhookResponse,
    deliver_risk_limit_notifications,
    parse_webhook_delivery_config,
)


def _config(**overrides: Any) -> WebhookDeliveryConfig:
    values: dict[str, Any] = {
        "adapter_id": "risk-webhook",
        "enabled": True,
        "endpoint_env": "ENDPOINT",
        "authorization_env": "AUTH",
        "timeout_seconds": 5,
        "max_attempts": 3,
        "initial_backoff_seconds": 2,
        "max_backoff_seconds": 10,
        "max_notifications_per_run": 10,
    }
    values.update(overrides)
    return WebhookDeliveryConfig(**values)


def _notification() -> dict[str, object]:
    return {
        "notification_id": "notification-1",
        "deduplication_id": "dedupe-1",
        "payload_json": {
            "policy_id": "us-tech-standard",
            "status": "critical",
        },
    }


def test_config_is_disabled_by_default_and_secret_referenced() -> None:
    parsed = parse_webhook_delivery_config(
        {
            "adapters": {
                "risk-webhook": {
                    "enabled": False,
                    "adapter_type": "webhook",
                    "endpoint_env": "ENDPOINT",
                    "authorization_env": "AUTH",
                    "timeout_seconds": 10,
                    "max_attempts": 3,
                    "initial_backoff_seconds": 2,
                    "max_backoff_seconds": 30,
                    "max_notifications_per_run": 100,
                }
            }
        },
        "risk-webhook",
    )

    assert parsed.enabled is False
    assert parsed.endpoint_env == "ENDPOINT"
    assert parsed.authorization_env == "AUTH"


def test_success_writes_one_secret_free_attempt(tmp_path: Path) -> None:
    calls: list[dict[str, Any]] = []

    def transport(**kwargs: Any) -> WebhookResponse:
        calls.append(kwargs)
        return WebhookResponse(status_code=202)

    result = deliver_risk_limit_notifications(
        [_notification()],
        config=_config(),
        attempts_dir=tmp_path,
        endpoint="https://example.invalid/risk",
        authorization="Bearer top-secret",
        transport=transport,
        sleeper=lambda _: None,
        clock=lambda: datetime(2026, 8, 22, 12, tzinfo=timezone.utc),
    )

    assert result["counts"]["delivered"] == 1
    assert result["counts"]["attempts_written"] == 1
    assert calls[0]["headers"]["Idempotency-Key"] == "dedupe-1"
    assert calls[0]["headers"]["Authorization"] == "Bearer top-secret"
    attempt_files = list(tmp_path.rglob("attempt-*.json"))
    assert len(attempt_files) == 1
    attempt_text = attempt_files[0].read_text(encoding="utf-8")
    assert "top-secret" not in attempt_text
    assert "example.invalid" not in attempt_text


def test_retryable_failure_uses_same_idempotency_key_then_succeeds(
    tmp_path: Path,
) -> None:
    statuses = iter([500, 429, 204])
    keys: list[str] = []
    sleeps: list[float] = []

    def transport(**kwargs: Any) -> WebhookResponse:
        keys.append(kwargs["headers"]["Idempotency-Key"])
        return WebhookResponse(status_code=next(statuses))

    result = deliver_risk_limit_notifications(
        [_notification()],
        config=_config(),
        attempts_dir=tmp_path,
        endpoint="https://example.invalid/risk",
        authorization=None,
        transport=transport,
        sleeper=sleeps.append,
        clock=lambda: datetime(2026, 8, 22, 12, tzinfo=timezone.utc),
    )

    assert result["counts"]["delivered"] == 1
    assert result["counts"]["attempts_written"] == 3
    assert keys == ["dedupe-1", "dedupe-1", "dedupe-1"]
    assert sleeps == [2.0, 4.0]


def test_permanent_failure_is_not_retried(tmp_path: Path) -> None:
    calls = 0

    def transport(**_kwargs: Any) -> WebhookResponse:
        nonlocal calls
        calls += 1
        return WebhookResponse(status_code=400)

    result = deliver_risk_limit_notifications(
        [_notification()],
        config=_config(),
        attempts_dir=tmp_path,
        endpoint="https://example.invalid/risk",
        authorization=None,
        transport=transport,
        sleeper=lambda _: pytest.fail("permanent failure must not sleep"),
        clock=lambda: datetime(2026, 8, 22, 12, tzinfo=timezone.utc),
    )

    assert calls == 1
    assert result["counts"]["permanent_failure"] == 1


def test_identical_rerun_skips_existing_terminal_attempt(tmp_path: Path) -> None:
    kwargs = {
        "config": _config(),
        "attempts_dir": tmp_path,
        "endpoint": "https://example.invalid/risk",
        "authorization": None,
        "transport": lambda **_: WebhookResponse(status_code=200),
        "sleeper": lambda _: None,
        "clock": lambda: datetime(2026, 8, 22, 12, tzinfo=timezone.utc),
    }
    first = deliver_risk_limit_notifications([_notification()], **kwargs)
    second = deliver_risk_limit_notifications([_notification()], **kwargs)

    assert first["counts"]["delivered"] == 1
    assert second["counts"]["skipped_terminal"] == 1
    assert second["counts"]["attempts_written"] == 0
    assert len(list(tmp_path.rglob("attempt-*.json"))) == 1


def test_disabled_adapter_and_oversized_selection_fail_closed(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValidationError, match="disabled"):
        deliver_risk_limit_notifications(
            [_notification()],
            config=_config(enabled=False),
            attempts_dir=tmp_path,
            endpoint="https://example.invalid/risk",
            authorization=None,
        )

    with pytest.raises(ValidationError, match="configured bound"):
        deliver_risk_limit_notifications(
            [_notification(), {**_notification(), "notification_id": "n-2"}],
            config=_config(max_notifications_per_run=1),
            attempts_dir=tmp_path,
            endpoint="https://example.invalid/risk",
            authorization=None,
        )


def test_attempt_json_is_valid_and_contains_no_payload(tmp_path: Path) -> None:
    deliver_risk_limit_notifications(
        [_notification()],
        config=_config(),
        attempts_dir=tmp_path,
        endpoint="https://example.invalid/risk",
        authorization=None,
        transport=lambda **_: WebhookResponse(status_code=200),
        sleeper=lambda _: None,
        clock=lambda: datetime(2026, 8, 22, 12, tzinfo=timezone.utc),
    )
    attempt = json.loads(
        next(tmp_path.rglob("attempt-*.json")).read_text(encoding="utf-8")
    )

    assert attempt["notification_id"] == "notification-1"
    assert attempt["status"] == "delivered"
    assert "payload" not in attempt
