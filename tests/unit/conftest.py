from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _retry_destination_authority_unit_test_seam(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    """Keep legacy retry tests focused on retry semantics, not destination parsing."""

    module = getattr(request.node, "module", None)
    if module is None or not module.__name__.endswith(
        "test_portfolio_risk_notification_retry_execution"
    ):
        return

    from src.orchestration import execute_portfolio_risk_notification_retries as target

    def resolver(**kwargs: Any) -> dict[str, Any]:
        evaluated_at = kwargs["evaluated_at"]
        if isinstance(evaluated_at, datetime):
            evaluated = evaluated_at.astimezone(timezone.utc).isoformat()
        else:
            evaluated = str(evaluated_at)
        event_types = sorted(set(kwargs["event_types"]))
        return {
            "authority_id": "portfolio-risk-notification-destination-authority-v1-authority-test",
            "destination_fingerprint": "portfolio-risk-notification-destination-v1-test",
            "destination_id": kwargs["destination_id"],
            "endpoint_environment_variable": kwargs["delivery_endpoint_env"],
            "evaluated_at": evaluated,
            "evaluated_event_types": event_types,
            "model_version": "portfolio-risk-notification-destination-authority-v1",
            "channel": "webhook",
            "activation": {
                "enabled": True,
                "status": "active",
                "change_request_id": "TEST-DESTINATION-ACTIVATION",
                "reviewed_at": "2026-01-01T00:00:00+00:00",
                "review_expires_at": "2026-12-31T00:00:00+00:00",
            },
            "allowed_event_types": event_types,
            "active": True,
            "endpoint_value_recorded": False,
            "external_request_performed": False,
            "delivery_attempt_written": False,
            "outbox_mutated": False,
            "acknowledgement_mutated": False,
        }

    monkeypatch.setattr(target, "resolve_notification_destination_authority", resolver)
