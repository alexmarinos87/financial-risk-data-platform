from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import OverlapError, StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    DeliveryTransportError,
)
from src.orchestration.execute_portfolio_risk_notification_retries import (
    _write_summary,
    execute_portfolio_risk_notification_retries,
)
from src.orchestration.plan_portfolio_risk_notification_retries import (
    plan_portfolio_risk_notification_retries,
)
from src.orchestration.portfolio_risk_notification_delivery_lock import (
    LOCK_KEY_FINGERPRINT,
    LOCK_MODEL_VERSION,
    LOCK_SCOPE,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    load_retry_execution_contract,
)
from src.orchestration.portfolio_risk_notification_retry_plan_contract import (
    load_retry_plan,
)

PLANNED_AT = datetime(2026, 1, 10, 12, tzinfo=timezone.utc)
EXECUTED_AT = PLANNED_AT + timedelta(minutes=1)
ENDPOINT = "https://alerts.example.test/risk"


def _config_payload(
    *,
    delivery_enabled: bool = True,
    execution_enabled: bool = True,
    max_execution_events: int = 25,
    max_plan_age_seconds: int = 3600,
    timeout_seconds: int = 5,
) -> dict[str, Any]:
    return {
        "delivery": {
            "webhook": {
                "enabled": delivery_enabled,
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                "timeout_seconds": timeout_seconds,
                "max_batch_events": 25,
                "max_attempts_per_event": 3,
                "initial_backoff_seconds": 1,
            },
            "retry_planning": {
                "max_candidate_rows": 500,
                "max_plan_events": 25,
                "max_event_age_seconds": 7 * 24 * 60 * 60,
                "max_backoff_seconds": 3600,
                "retryable_http_statuses": [408, 425, 429, 500, 502, 503, 504],
                "retryable_error_codes": ["network_error"],
            },
            "retry_execution": {
                "enabled": execution_enabled,
                "max_plan_age_seconds": max_plan_age_seconds,
                "max_events": max_execution_events,
            },
        }
    }


def _write_config(tmp_path: Path, **kwargs: Any) -> Path:
    path = tmp_path / "notification-delivery.yaml"
    path.write_text(
        yaml.safe_dump(_config_payload(**kwargs), sort_keys=False),
        encoding="utf-8",
    )
    return path


def _candidate(
    event_id: str = "event-1",
    *,
    attempt_count: int = 1,
    last_attempted_at: datetime | None = None,
    acknowledgement: bool = False,
) -> dict[str, Any]:
    event_time = PLANNED_AT - timedelta(hours=2)
    attempt_time = last_attempted_at or PLANNED_AT - timedelta(minutes=10)
    candidate: dict[str, Any] = {
        "event_id": event_id,
        "outbox_model_version": "portfolio-risk-notification-outbox-v1",
        "event_type": "breach_opened",
        "transition_type": "opened",
        "delivery_disposition": "pending",
        "source_evaluation_calculation_id": f"evaluation-{event_id}",
        "policy_id": "us-tech-standard",
        "policy_fingerprint": "policy-fingerprint",
        "portfolio_id": "us-tech-equal",
        "definition_fingerprint": "definition-fingerprint",
        "metric_name": "portfolio_volatility_annualized",
        "subject_type": "portfolio",
        "subject_key": "us-tech-equal",
        "current_status": "critical",
        "ts_event": event_time,
        "ts_ingest": event_time + timedelta(seconds=1),
        "payload_json": {"event_id": event_id, "severity": "critical"},
        "attempt_count": attempt_count,
        "last_attempt_id": (
            f"attempt-{event_id}-{attempt_count}" if attempt_count else None
        ),
        "last_attempt_number": attempt_count if attempt_count else None,
        "last_attempted_at": attempt_time if attempt_count else None,
        "last_attempt_outcome": "failed" if attempt_count else None,
        "last_http_status": 503 if attempt_count else None,
        "last_error_code": "http_503" if attempt_count else None,
        "acknowledgement_id": None,
        "acknowledged_at": None,
        "acknowledgement_disposition": None,
    }
    if acknowledgement:
        candidate.update(
            {
                "acknowledgement_id": f"ack-{event_id}",
                "acknowledged_at": PLANNED_AT - timedelta(minutes=5),
                "acknowledgement_disposition": "investigating",
            }
        )
    return candidate


def _write_plan(
    tmp_path: Path,
    config_path: Path,
    candidates: list[dict[str, Any]],
) -> tuple[Path, dict[str, Any]]:
    plan = plan_portfolio_risk_notification_retries(
        config_path=config_path,
        dsn="not-used",
        planned_at=PLANNED_AT,
        policy_id="us-tech-standard",
        portfolio_id="us-tech-equal",
        reader=lambda **_: candidates,
    )
    path = tmp_path / "retry-plan.json"
    path.write_text(
        json.dumps(plan, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path, plan


@contextmanager
def _delivery_lock(**_: Any) -> Iterator[Mapping[str, Any]]:
    yield {
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
        "acquired": True,
    }


def _execute(
    *,
    plan_path: Path,
    plan: dict[str, Any],
    config_path: Path,
    candidates: list[dict[str, Any]],
    **kwargs: Any,
) -> dict[str, Any]:
    parameters: dict[str, Any] = {
        "plan_path": plan_path,
        "confirm_plan_id": plan["plan_id"],
        "request_id": "RETRY-2026-001",
        "config_path": config_path,
        "dsn": "postgresql://secret-value",
        "execute": True,
        "environment": {"RISK_NOTIFICATION_WEBHOOK_URL": ENDPOINT},
        "reader": lambda **_: candidates,
        "attempt_writer": lambda _: None,
        "transport": lambda *_: 204,
        "clock": lambda: EXECUTED_AT,
        "lock_factory": _delivery_lock,
    }
    parameters.update(kwargs)
    return execute_portfolio_risk_notification_retries(**parameters)


def test_retry_execution_policy_is_deterministic_and_bounded(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    delivery, retry, execution = load_retry_execution_contract(config_path)
    _, _, repeated = load_retry_execution_contract(config_path)

    assert execution.enabled is True
    assert execution.max_events == retry.max_plan_events
    assert execution.max_events == delivery.max_batch_events
    assert execution.fingerprint == repeated.fingerprint

    invalid_path = _write_config(tmp_path, max_execution_events=26)
    with pytest.raises(ValidationError, match="retry planning limit"):
        load_retry_execution_contract(invalid_path)


def test_explicit_gate_and_plan_confirmation_fail_before_reading(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    reads: list[bool] = []

    with pytest.raises(ValidationError, match="explicit --execute"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            execute=False,
            reader=lambda **_: reads.append(True) or candidates,
        )
    assert reads == []

    with pytest.raises(ValidationError, match="confirm_plan_id"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            confirm_plan_id="wrong-plan-id",
            reader=lambda **_: reads.append(True) or candidates,
        )
    assert reads == []


def test_disabled_execution_rejects_before_database_or_transport(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path, execution_enabled=False)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    reads: list[bool] = []
    sends: list[bool] = []

    with pytest.raises(ValidationError, match="disabled"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            reader=lambda **_: reads.append(True) or candidates,
            transport=lambda *_: sends.append(True) or 204,
        )
    assert reads == []
    assert sends == []


def test_execution_uses_clock_time_for_current_evidence_read(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    as_of_values: list[datetime] = []

    _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
        reader=lambda **kwargs: (
            as_of_values.append(kwargs["planned_at"]) or candidates
        ),
    )

    assert as_of_values == [EXECUTED_AT]


def test_held_lock_rejects_before_current_evidence_or_transport(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    reads: list[bool] = []
    sends: list[bool] = []

    @contextmanager
    def held_lock(**_: Any) -> Iterator[Mapping[str, Any]]:
        raise OverlapError("already holds")
        yield {}

    with pytest.raises(OverlapError, match="already holds"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            reader=lambda **_: reads.append(True) or candidates,
            transport=lambda *_: sends.append(True) or 204,
            lock_factory=held_lock,
        )

    assert reads == []
    assert sends == []


def test_plan_can_be_reviewed_before_enabling_separate_execution_gate(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path, execution_enabled=False)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)

    config_path.write_text(
        yaml.safe_dump(
            _config_payload(execution_enabled=True),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    summary = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
    )

    assert summary["execution"]["performed"] is True
    assert summary["plan_id"] == plan["plan_id"]


def test_exact_plan_executes_one_attempt_per_event_with_stable_identity(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate("event-b"), _candidate("event-a")]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    attempts: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []

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
        return 204

    summary = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=transport,
    )

    assert [attempt["event_id"] for attempt in attempts] == ["event-a", "event-b"]
    assert [attempt["attempt_number"] for attempt in attempts] == [2, 2]
    assert all(
        attempt["idempotency_key"] == attempt["event_id"] for attempt in attempts
    )
    assert [call["headers"]["Idempotency-Key"] for call in calls] == [
        "event-a",
        "event-b",
    ]
    assert summary["outcome_counts"] == {"succeeded": 2, "failed": 0}
    assert summary["execution"] == {
        "requested": True,
        "performed": True,
        "external_requests_performed": 2,
        "delivery_attempts_written": 2,
    }
    assert summary["revalidation"]["exact_event_evidence_unchanged"] is True
    assert summary["concurrency_control"] == {
        "performed": True,
        "acquired": True,
        "released": True,
        "held_through_revalidation": True,
        "held_through_attempt_persistence": True,
        "model_version": LOCK_MODEL_VERSION,
        "scope": LOCK_SCOPE,
        "key_fingerprint": LOCK_KEY_FINGERPRINT,
    }
    assert summary["plan_mutated"] is False
    assert summary["acknowledgement_mutated"] is False
    assert summary["dead_letter_mutated"] is False
    assert summary["response_bodies_recorded"] is False

    rendered = json.dumps(summary, sort_keys=True)
    assert ENDPOINT not in rendered
    assert "postgresql://secret-value" not in rendered
    assert '"severity"' not in rendered
    assert summary["endpoint"] == {
        "host": "alerts.example.test",
        "full_url_recorded": False,
    }


def test_execution_identity_is_deterministic_for_exact_inputs(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)

    first = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
    )
    second = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
    )

    assert first["execution_id"] == second["execution_id"]
    assert first["outcomes"] == second["outcomes"]


def test_stale_or_reconfigured_plan_rejects_before_reading(tmp_path: Path) -> None:
    stale_config = _write_config(tmp_path, max_plan_age_seconds=30)
    candidates = [_candidate()]
    stale_path, stale_plan = _write_plan(tmp_path, stale_config, candidates)
    reads: list[bool] = []

    with pytest.raises(ValidationError, match="age limit"):
        _execute(
            plan_path=stale_path,
            plan=stale_plan,
            config_path=stale_config,
            candidates=candidates,
            reader=lambda **_: reads.append(True) or candidates,
        )
    assert reads == []

    config_path = _write_config(tmp_path)
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    config_path.write_text(
        yaml.safe_dump(
            _config_payload(timeout_seconds=6),
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValidationError, match="fingerprint changed"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            reader=lambda **_: reads.append(True) or candidates,
        )
    assert reads == []


def test_changed_attempt_acknowledgement_or_event_set_rejects_before_transport(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    sends: list[bool] = []

    changed_attempt = [_candidate(attempt_count=2)]
    with pytest.raises(ValidationError, match="changed"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=changed_attempt,
            transport=lambda *_: sends.append(True) or 204,
        )

    acknowledged = [_candidate(acknowledgement=True)]
    with pytest.raises(ValidationError, match="changed"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=acknowledged,
            transport=lambda *_: sends.append(True) or 204,
        )

    with pytest.raises(ValidationError, match="event set changed"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=[],
            transport=lambda *_: sends.append(True) or 204,
        )
    assert sends == []


def test_failed_retry_records_one_attempt_without_hidden_retry_loop(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    attempts: list[dict[str, Any]] = []
    calls: list[bool] = []

    summary = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=lambda *_: calls.append(True) or 503,
    )

    assert len(calls) == 1
    assert len(attempts) == 1
    assert attempts[0]["outcome"] == "failed"
    assert attempts[0]["error_code"] == "http_503"
    assert summary["outcome_counts"] == {"succeeded": 0, "failed": 1}


def test_transport_errors_are_bounded_and_invalid_status_fails_before_write(
    tmp_path: Path,
) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    attempts: list[dict[str, Any]] = []

    def network_failure(*_: Any) -> int:
        raise DeliveryTransportError("arbitrary exception text must not be retained")

    summary = _execute(
        plan_path=plan_path,
        plan=plan,
        config_path=config_path,
        candidates=candidates,
        attempt_writer=lambda attempt: attempts.append(dict(attempt)),
        transport=network_failure,
    )
    assert attempts[0]["error_code"] == "network_error"
    assert summary["outcomes"][0]["error_code"] == "network_error"
    assert "arbitrary exception" not in json.dumps(summary)

    attempts.clear()
    with pytest.raises(ValidationError, match="invalid HTTP status"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            attempt_writer=lambda attempt: attempts.append(dict(attempt)),
            transport=lambda *_: 999,
        )
    assert attempts == []


def test_attempt_persistence_failure_is_not_reported_as_success(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    calls: list[bool] = []

    def writer(_: Any) -> None:
        raise StorageError("write failed")

    with pytest.raises(StorageError, match="write failed"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            attempt_writer=writer,
            transport=lambda *_: calls.append(True) or 204,
        )
    assert calls == [True]


def test_execution_event_limit_fails_before_reader_or_transport(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, max_execution_events=1)
    candidates = [_candidate("event-a"), _candidate("event-b")]
    plan_path, plan = _write_plan(tmp_path, config_path, candidates)
    reads: list[bool] = []
    sends: list[bool] = []

    with pytest.raises(ValidationError, match="event limit"):
        _execute(
            plan_path=plan_path,
            plan=plan,
            config_path=config_path,
            candidates=candidates,
            reader=lambda **_: reads.append(True) or candidates,
            transport=lambda *_: sends.append(True) or 204,
        )
    assert reads == []
    assert sends == []


def test_tampered_symlink_and_oversized_plans_fail_closed(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path)
    candidates = [_candidate()]
    plan_path, _ = _write_plan(tmp_path, config_path, candidates)

    document = json.loads(plan_path.read_text(encoding="utf-8"))
    document["retryable_event_ids"] = []
    plan_path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ValidationError, match="retryable_event_ids"):
        load_retry_plan(plan_path)

    original = tmp_path / "original.json"
    original.write_text("{}", encoding="utf-8")
    link = tmp_path / "plan-link.json"
    link.symlink_to(original)
    with pytest.raises(ValidationError, match="symbolic link"):
        load_retry_plan(link)

    oversized = tmp_path / "oversized.json"
    oversized.write_text("x" * 1_048_577, encoding="utf-8")
    with pytest.raises(ValidationError, match="1 MB"):
        load_retry_plan(oversized)


def test_summary_writer_rejects_symbolic_links(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    link = tmp_path / "summary.json"
    link.symlink_to(target)

    with pytest.raises(StorageError, match="symbolic link"):
        _write_summary(link, {"safe": True})
