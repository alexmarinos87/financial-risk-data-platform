from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    DeliveryTransportError,
    deliver_portfolio_risk_notifications,
    parse_webhook_delivery_config,
)


def _config_payload(*, enabled: bool = False, attempts: int = 3):
    return {
        "delivery": {
            "webhook": {
                "enabled": enabled,
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                "timeout_seconds": 5,
                "max_batch_events": 25,
                "max_attempts_per_event": attempts,
                "initial_backoff_seconds": 1,
            }
        }
    }


def _write_config(tmp_path: Path, *, enabled: bool, attempts: int = 3) -> Path:
    path = tmp_path / "delivery.yaml"
    path.write_text(
        yaml.safe_dump(
            _config_payload(enabled=enabled, attempts=attempts),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return path


def _candidate(*, attempts_so_far: int = 0) -> dict[str, Any]:
    return {
        "event_id": "event-1",
        "event_type": "breach_opened",
        "transition_type": "opened",
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "policy-1",
        "portfolio_id": "us-tech-equal",
        "definition_fingerprint": "definition-1",
        "metric_name": "portfolio_volatility_annualized",
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "current_status": "critical",
        "ts_event": datetime(2026, 1, 9, tzinfo=timezone.utc),
        "payload_json": {"event_id": "event-1", "severity": "critical"},
        "attempts_so_far": attempts_so_far,
    }


def test_delivery_config_is_disabled_and_fingerprinted_deterministically() -> None:
    first = parse_webhook_delivery_config(_config_payload())
    second = parse_webhook_delivery_config(_config_payload())

    assert first.enabled is False
    assert first.fingerprint == second.fingerprint


def test_plan_only_reads_candidates_without_transport_or_attempt_writes(
    tmp_path: Path,
) -> None:
    transports: list[bytes] = []
    attempts: list[dict[str, Any]] = []
    summary = deliver_portfolio_risk_notifications(
        config_path=_write_config(tmp_path, enabled=False),
        dsn="dsn",
        execute=False,
        environment={},
        reader=lambda **_: [_candidate()],
        transport=lambda endpoint, payload, headers, timeout: (
            transports.append(payload) or 204
        ),
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
    )

    assert transports == []
    assert attempts == []
    assert summary["execution"]["performed"] is False
    assert summary["endpoint"] == {"configured": False, "host": None}
    assert "dsn" not in json.dumps(summary)


def test_execute_requires_reviewed_enablement_before_reading_candidates(
    tmp_path: Path,
) -> None:
    reads = 0

    def reader(**_: Any) -> list[dict[str, Any]]:
        nonlocal reads
        reads += 1
        return [_candidate()]

    with pytest.raises(ValidationError, match="disabled"):
        deliver_portfolio_risk_notifications(
            config_path=_write_config(tmp_path, enabled=False),
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
            },
            reader=reader,
        )
    assert reads == 0


def test_failed_attempt_retries_then_succeeds_with_same_idempotency_key(
    tmp_path: Path,
) -> None:
    statuses = iter([503, 204])
    calls: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []
    sleeps: list[float] = []

    def transport(
        endpoint: str,
        payload: bytes,
        headers: Any,
        timeout: float,
    ) -> int:
        calls.append(
            {
                "endpoint": endpoint,
                "payload": json.loads(payload),
                "headers": dict(headers),
                "timeout": timeout,
            }
        )
        return next(statuses)

    summary = deliver_portfolio_risk_notifications(
        config_path=_write_config(tmp_path, enabled=True),
        dsn="dsn",
        execute=True,
        environment={
            "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
        },
        reader=lambda **_: [_candidate()],
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=transport,
        sleeper=sleeps.append,
    )

    assert [attempt["outcome"] for attempt in attempts] == [
        "failed",
        "succeeded",
    ]
    assert [attempt["attempt_number"] for attempt in attempts] == [1, 2]
    assert all(
        call["headers"]["Idempotency-Key"] == "event-1" for call in calls
    )
    assert calls[0]["payload"]["event_id"] == "event-1"
    assert sleeps == [1.0]
    assert summary["execution"] == {
        "requested": True,
        "performed": True,
        "succeeded": 1,
        "failed": 0,
        "exhausted": 0,
        "attempts_recorded": 2,
    }
    assert "https://alerts.example.test/risk" not in json.dumps(summary)
    assert summary["endpoint"]["host"] == "alerts.example.test"


def test_network_failure_is_recorded_without_error_message_or_response_body(
    tmp_path: Path,
) -> None:
    attempts: list[dict[str, Any]] = []

    def transport(*_: Any) -> int:
        raise DeliveryTransportError("network_error")

    summary = deliver_portfolio_risk_notifications(
        config_path=_write_config(tmp_path, enabled=True, attempts=1),
        dsn="dsn",
        execute=True,
        environment={
            "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
        },
        reader=lambda **_: [_candidate()],
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=transport,
        sleeper=lambda _: None,
    )

    assert attempts[0]["error_code"] == "network_error"
    assert attempts[0]["http_status"] is None
    assert summary["response_bodies_recorded"] is False
    assert summary["execution"]["failed"] == 1


def test_exhausted_candidate_is_not_sent_again(tmp_path: Path) -> None:
    calls = 0

    def transport(*_: Any) -> int:
        nonlocal calls
        calls += 1
        return 204

    summary = deliver_portfolio_risk_notifications(
        config_path=_write_config(tmp_path, enabled=True, attempts=3),
        dsn="dsn",
        execute=True,
        environment={
            "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
        },
        reader=lambda **_: [_candidate(attempts_so_far=3)],
        attempt_writer=lambda _: None,
        transport=transport,
    )

    assert calls == 0
    assert summary["execution"]["exhausted"] == 1


def test_endpoint_must_be_https_without_embedded_credentials(
    tmp_path: Path,
) -> None:
    for endpoint in (
        "http://alerts.example.test/risk",
        "https://user:password@alerts.example.test/risk",
        "https://alerts.example.test/risk#secret",
    ):
        with pytest.raises(ValidationError, match="HTTPS"):
            deliver_portfolio_risk_notifications(
                config_path=_write_config(tmp_path, enabled=True),
                dsn="dsn",
                execute=True,
                environment={"RISK_NOTIFICATION_WEBHOOK_URL": endpoint},
                reader=lambda **_: [_candidate()],
            )
