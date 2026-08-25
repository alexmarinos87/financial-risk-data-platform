from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import OverlapError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    DeliveryTransportError,
    deliver_portfolio_risk_notifications,
    parse_webhook_delivery_config,
)
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
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


@contextmanager
def _delivery_lock(**_: Any) -> Iterator[Mapping[str, Any]]:
    yield {
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
        "acquired": True,
    }


def test_delivery_config_is_disabled_and_fingerprinted_deterministically() -> None:
    first = parse_webhook_delivery_config(_config_payload())
    second = parse_webhook_delivery_config(_config_payload())

    assert first.enabled is False
    assert first.fingerprint == second.fingerprint


def test_plan_only_reads_initial_candidates_without_transport_attempt_or_lock(
    tmp_path: Path,
) -> None:
    transports: list[bytes] = []
    attempts: list[dict[str, Any]] = []
    locks: list[bool] = []

    @contextmanager
    def lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        locks.append(True)
        yield {}

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
        lock_factory=lock,
    )

    assert transports == []
    assert attempts == []
    assert locks == []
    assert summary["execution"]["performed"] is False
    assert summary["selection"]["initial_attempts_only"] is True
    assert summary["endpoint"] == {"configured": False, "host": None}
    assert summary["concurrency_control"]["performed"] is False
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
            lock_factory=_delivery_lock,
        )
    assert reads == 0


def test_held_lock_rejects_before_candidate_read_or_transport(tmp_path: Path) -> None:
    reads: list[bool] = []
    sends: list[bool] = []

    @contextmanager
    def held_lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        raise OverlapError("already holds")
        yield {}

    with pytest.raises(OverlapError, match="already holds"):
        deliver_portfolio_risk_notifications(
            config_path=_write_config(tmp_path, enabled=True),
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
            },
            reader=lambda **_: reads.append(True) or [_candidate()],
            attempt_writer=lambda _: None,
            transport=lambda *_: sends.append(True) or 204,
            lock_factory=held_lock,
        )

    assert reads == []
    assert sends == []


def test_initial_delivery_failure_records_one_attempt_without_hidden_retry(
    tmp_path: Path,
) -> None:
    calls: list[dict[str, Any]] = []
    attempts: list[dict[str, Any]] = []

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
        return 503

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
        lock_factory=_delivery_lock,
    )

    assert len(calls) == 1
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["attempt_number"] == 1
    assert attempts[0]["error_code"] == "http_503"
    assert calls[0]["headers"]["Idempotency-Key"] == "event-1"
    assert calls[0]["payload"]["event_id"] == "event-1"
    assert summary["execution"] == {
        "requested": True,
        "performed": True,
        "succeeded": 0,
        "failed": 1,
        "exhausted": 0,
        "attempts_recorded": 1,
    }
    assert summary["concurrency_control"] == {
        "performed": True,
        "acquired": True,
        "released": True,
        "held_through_attempt_persistence": True,
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
    }
    assert "https://alerts.example.test/risk" not in json.dumps(summary)
    assert summary["endpoint"]["host"] == "alerts.example.test"


def test_network_failure_is_recorded_without_error_message_or_response_body(
    tmp_path: Path,
) -> None:
    attempts: list[dict[str, Any]] = []

    def transport(*_: Any) -> int:
        raise DeliveryTransportError("arbitrary transport text")

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
        lock_factory=_delivery_lock,
    )

    assert attempts[0]["error_code"] == "network_error"
    assert attempts[0]["http_status"] is None
    assert summary["response_bodies_recorded"] is False
    assert summary["execution"]["failed"] == 1
    assert "arbitrary transport text" not in json.dumps(summary)


def test_candidate_with_prior_attempt_requires_exact_retry_plan_before_transport(
    tmp_path: Path,
) -> None:
    calls = 0
    attempts: list[dict[str, Any]] = []

    def transport(*_: Any) -> int:
        nonlocal calls
        calls += 1
        return 204

    with pytest.raises(ValidationError, match="exact retry plan"):
        deliver_portfolio_risk_notifications(
            config_path=_write_config(tmp_path, enabled=True, attempts=3),
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
            },
            reader=lambda **_: [_candidate(attempts_so_far=1)],
            attempt_writer=lambda attempt: attempts.append(dict(attempt)),
            transport=transport,
            lock_factory=_delivery_lock,
        )

    assert calls == 0
    assert attempts == []


def test_invalid_transport_status_fails_before_attempt_write(tmp_path: Path) -> None:
    attempts: list[dict[str, Any]] = []

    with pytest.raises(ValidationError, match="invalid HTTP status"):
        deliver_portfolio_risk_notifications(
            config_path=_write_config(tmp_path, enabled=True),
            dsn="dsn",
            execute=True,
            environment={
                "RISK_NOTIFICATION_WEBHOOK_URL": "https://alerts.example.test/risk"
            },
            reader=lambda **_: [_candidate()],
            attempt_writer=lambda attempt: attempts.append(dict(attempt)),
            transport=lambda *_: 999,
            lock_factory=_delivery_lock,
        )
    assert attempts == []


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
                lock_factory=_delivery_lock,
            )
