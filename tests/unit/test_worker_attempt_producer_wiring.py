from __future__ import annotations

import copy
import hashlib
import socket
from datetime import datetime, timedelta
from typing import Any

import pytest

from src.common.exceptions import StorageError, ValidationError
from src.orchestration import deliver_portfolio_risk_notifications as delivery
from src.orchestration.notification_worker_attempt_observer import WorkerAttemptObserver
from src.orchestration.notification_worker_authority_contract import canonical_bytes
from test_notification_worker_authority_contract import grant
from test_worker_execution_context import context


@pytest.mark.parametrize("scenario", ["succeeded", "failed", "network_error", "writer_error", "expired_attribution"])
def test_actual_initial_attempt_loop_preserves_write_order_and_observes_context(
    scenario: str, monkeypatch: Any,
) -> None:
    selected = context()
    start = datetime.fromisoformat(selected["started_at"])
    offset = 120 if scenario == "expired_attribution" else 0

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> FrozenDateTime:
            return cls.fromisoformat((start + timedelta(seconds=offset)).isoformat())

    monkeypatch.setattr(delivery, "datetime", FrozenDateTime)
    network_calls: list[bool] = []

    def forbidden_network(*args: Any, **kwargs: Any) -> Any:
        network_calls.append(True)
        raise AssertionError("network is forbidden in attribution fixture")

    monkeypatch.setattr(socket, "socket", forbidden_network)
    monkeypatch.setattr(socket, "create_connection", forbidden_network)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden_network)
    monkeypatch.setattr(delivery, "write_delivery_attempt", forbidden_network)
    monkeypatch.setattr(delivery, "_default_transport", forbidden_network)
    order: list[str] = []
    written: list[dict[str, Any]] = []

    def writer(row: Any) -> None:
        order.append("write")
        written.append(copy.deepcopy(row))
        if scenario == "writer_error":
            raise StorageError("synthetic writer outcome unknown")

    def transport(endpoint: str, payload: bytes, headers: Any, timeout: float) -> int:
        order.append("transport")
        assert headers["Idempotency-Key"] == "producer-event-1"
        assert timeout == 1.0 and payload
        if scenario == "network_error":
            raise delivery.DeliveryTransportError("network_error")
        return 503 if scenario == "failed" else 204

    watch = WorkerAttemptObserver(authority=grant(), context=selected, writer=writer)
    candidates = [{
        "event_id": f"producer-event-{number}", "event_type": "breach_opened",
        "metric_name": "portfolio_var", "payload_json": {"synthetic": True},
        "policy_id": "fixture-policy", "portfolio_id": "fixture-portfolio",
        "current_status": "critical", "subject_key": "fixture-subject",
        "ts_event": FrozenDateTime.fromisoformat(selected["started_at"]),
    } for number in (1, 2)]
    config = delivery.WebhookDeliveryConfig(
        enabled=True, endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL", timeout_seconds=1,
        max_batch_events=25, max_attempts_per_event=3, initial_backoff_seconds=1,
    )

    def execute() -> dict[str, Any]:
        # Only this producer loop is under test. No public execute path, readiness
        # check, database, shared lock or real transport is invoked by this test.
        return delivery._execute_initial_attempts(
            candidates=candidates if scenario in {"writer_error", "expired_attribution"} else candidates[:1],
            endpoint="https://receiver.example.invalid/fixture",
            endpoint_host="receiver.example.invalid", config=config, writer=watch, transport=transport,
        )

    if scenario == "writer_error":
        with pytest.raises(StorageError, match="unconfirmed"):
            execute()
    elif scenario == "expired_attribution":
        with pytest.raises(ValidationError, match="after writer returned"):
            execute()
    else:
        result = execute()
        assert result["attempts_recorded"] == 1
        assert result["succeeded"] == (1 if scenario == "succeeded" else 0)
        assert result["failed"] == (0 if scenario == "succeeded" else 1)
    assert network_calls == []
    assert order == ["transport", "write"]
    assert len(written) == 1
    report = watch.seal()
    assert report["writer_calls"] == 1
    assert report["writer_returns"] == (0 if scenario == "writer_error" else 1)
    assert report["writer_outcome_uncertain"] is (scenario == "writer_error")
    assert report["attributed_attempts"] == (0 if scenario in {"writer_error", "expired_attribution"} else 1)
    assert report["database_commit_verified"] is False
    assert report["failure_history_complete"] is False
    assert report["runtime_permission_granted"] is False
    if report["receipts"]:
        source = {**written[0], "attempted_at": written[0]["attempted_at"].isoformat()}
        assert report["receipts"][0]["source_attempt_sha256"] == hashlib.sha256(canonical_bytes(source)).hexdigest()
