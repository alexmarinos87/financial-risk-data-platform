from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

import src.orchestration.run_optional_portfolio_risk_notification_outbox as optional
from src.common.exceptions import ValidationError


def test_optional_outbox_returns_successful_noop_when_no_transition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_records(**_: Any) -> list[dict[str, Any]]:
        raise ValidationError(optional.NO_TRANSITIONS_MESSAGE)

    monkeypatch.setattr(optional, "read_actionable_transitions", no_records)
    summary = optional.run_optional_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=date(2026, 1, 9),
        end_date=date(2026, 1, 9),
        max_events=100,
        dsn="dsn",
        storage_config_path=Path("storage.yaml"),
    )

    assert summary["selection"]["actionable_transitions"] == 0
    assert summary["delivery"] == {
        "performed": False,
        "external_destinations": 0,
        "reason": "no_actionable_transitions",
    }
    assert summary["latest_event"] is None


def test_optional_outbox_reuses_preloaded_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    records = [{"calculation_id": "transition-1"}]
    received: list[list[dict[str, Any]]] = []

    monkeypatch.setattr(
        optional,
        "read_actionable_transitions",
        lambda **_: records,
    )

    def run_outbox(**kwargs: Any) -> dict[str, Any]:
        received.append(kwargs["reader"]())
        return {"ok": True}

    monkeypatch.setattr(
        optional,
        "run_portfolio_risk_notification_outbox",
        run_outbox,
    )
    assert optional.run_optional_portfolio_risk_notification_outbox(
        policy_id="us-tech-standard",
        start_date=None,
        end_date=date(2026, 1, 9),
        max_events=100,
        dsn="dsn",
        storage_config_path=Path("storage.yaml"),
    ) == {"ok": True}
    assert received == [records]


def test_optional_outbox_does_not_suppress_other_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def invalid(**_: Any) -> list[dict[str, Any]]:
        raise ValidationError("different failure")

    monkeypatch.setattr(optional, "read_actionable_transitions", invalid)
    with pytest.raises(ValidationError, match="different failure"):
        optional.run_optional_portfolio_risk_notification_outbox(
            policy_id="us-tech-standard",
            start_date=None,
            end_date=date(2026, 1, 9),
            max_events=100,
            dsn="dsn",
            storage_config_path=Path("storage.yaml"),
        )
