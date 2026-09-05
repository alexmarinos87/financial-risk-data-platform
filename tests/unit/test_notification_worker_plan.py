from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.plan_notification_worker import (
    PLAN_MODEL_VERSION,
    load_notification_workers,
    main,
    plan_notification_worker,
    validate_notification_worker_plan,
)

BASE_TIME = datetime(2026, 9, 5, 20, 0, tzinfo=timezone.utc)
WORKER_ID = "risk-operations-managed"


def _worker_payload(*, enabled: bool = False) -> dict[str, Any]:
    return {
        "model_version": "portfolio-risk-notification-worker-config-v1",
        "workers": {
            WORKER_ID: {
                "enabled": enabled,
                "destination_id": "risk-operations-webhook",
                "execution_kinds": ["initial", "retry"],
                "schedule": {
                    "mode": "fixed_interval",
                    "interval_seconds": 300,
                    "jitter_seconds": 30,
                    "timezone": "UTC",
                },
                "limits": {
                    "max_initial_events": 25,
                    "max_retry_events": 25,
                    "max_concurrency": 1,
                    "execution_timeout_seconds": 120,
                },
                "readiness": {
                    "required_status": "allowed",
                    "max_age_seconds": 300,
                },
                "suspension": {
                    "block_on_readiness_failure": True,
                    "block_on_persistence_ambiguity": True,
                    "block_on_expired_review": True,
                    "max_consecutive_failures": 3,
                    "cooldown_seconds": 900,
                },
            }
        },
    }


def _delivery_payload(*, enabled: bool, retry_enabled: bool) -> dict[str, Any]:
    return {
        "delivery": {
            "webhook": {
                "enabled": enabled,
                "endpoint_env": "RISK_NOTIFICATION_WEBHOOK_URL",
                "timeout_seconds": 5,
                "max_batch_events": 25,
                "max_attempts_per_event": 3,
                "initial_backoff_seconds": 1,
            },
            "retry_planning": {
                "max_candidate_rows": 500,
                "max_plan_events": 25,
                "max_event_age_seconds": 604800,
                "max_backoff_seconds": 3600,
                "retryable_http_statuses": [408, 425, 429, 500, 502, 503, 504],
                "retryable_error_codes": ["network_error"],
            },
            "retry_execution": {
                "enabled": retry_enabled,
                "max_plan_age_seconds": 3600,
                "max_events": 25,
            },
        }
    }


def _destination_payload(
    *,
    enabled: bool,
    endpoint_env: str = "RISK_NOTIFICATION_WEBHOOK_URL",
) -> dict[str, Any]:
    activation: dict[str, Any]
    if enabled:
        activation = {
            "enabled": True,
            "change_request_id": "CHG-WORKER-001",
            "reviewed_by": ["risk-control-reviewer"],
            "reviewed_at": "2026-09-01T00:00:00+00:00",
            "review_expires_at": "2026-10-01T00:00:00+00:00",
        }
    else:
        activation = {
            "enabled": False,
            "change_request_id": None,
            "reviewed_by": [],
            "reviewed_at": None,
            "review_expires_at": None,
        }
    return {
        "model_version": "portfolio-risk-notification-destination-v1",
        "destinations": {
            "risk-operations-webhook": {
                "channel": "webhook",
                "endpoint_env": endpoint_env,
                "owner": {
                    "team": "risk-operations",
                    "contact": "risk-operations-oncall",
                },
                "purpose": "portfolio-risk-breach-lifecycle",
                "recipient_scope": "risk-operations",
                "data_classification": "internal",
                "allowed_event_types": [
                    "breach_escalated",
                    "breach_opened",
                    "breach_resolved",
                ],
                "activation": activation,
            }
        },
    }


def _write_yaml(path: Path, payload: dict[str, Any]) -> Path:
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _paths(
    tmp_path: Path,
    *,
    worker_enabled: bool,
    delivery_enabled: bool,
    retry_enabled: bool,
    destination_enabled: bool,
    endpoint_env: str = "RISK_NOTIFICATION_WEBHOOK_URL",
) -> tuple[Path, Path, Path]:
    worker_path = _write_yaml(
        tmp_path / "workers.yaml",
        _worker_payload(enabled=worker_enabled),
    )
    delivery_path = _write_yaml(
        tmp_path / "delivery.yaml",
        _delivery_payload(
            enabled=delivery_enabled,
            retry_enabled=retry_enabled,
        ),
    )
    destination_path = _write_yaml(
        tmp_path / "destinations.yaml",
        _destination_payload(
            enabled=destination_enabled,
            endpoint_env=endpoint_env,
        ),
    )
    return worker_path, delivery_path, destination_path


def _plan(
    tmp_path: Path,
    *,
    worker_enabled: bool = True,
    delivery_enabled: bool = True,
    retry_enabled: bool = True,
    destination_enabled: bool = True,
    endpoint_env: str = "RISK_NOTIFICATION_WEBHOOK_URL",
) -> dict[str, Any]:
    worker_path, delivery_path, destination_path = _paths(
        tmp_path,
        worker_enabled=worker_enabled,
        delivery_enabled=delivery_enabled,
        retry_enabled=retry_enabled,
        destination_enabled=destination_enabled,
        endpoint_env=endpoint_env,
    )
    return plan_notification_worker(
        worker_id=WORKER_ID,
        planned_at=BASE_TIME,
        worker_config_path=worker_path,
        delivery_config_path=delivery_path,
        destination_config_path=destination_path,
    )


def test_committed_worker_is_disabled_and_plan_only() -> None:
    plan = plan_notification_worker(
        worker_id=WORKER_ID,
        planned_at=BASE_TIME,
    )

    assert plan["model_version"] == PLAN_MODEL_VERSION
    assert plan["status"] == "disabled"
    assert plan["blocking_reasons"] == [
        "worker_disabled",
        "delivery_disabled",
        "retry_execution_disabled",
        "destination_not_active",
    ]
    assert plan["schedule"]["activation_action"] == "none"
    assert plan["concurrency_control"]["max_concurrency"] == 1
    assert plan["concurrency_control"]["lock_acquired"] is False
    assert all(value is False for value in plan["side_effects"].values())
    assert validate_notification_worker_plan(plan) == plan


def test_enabled_reviewed_contract_would_schedule_deterministically(
    tmp_path: Path,
) -> None:
    first = _plan(tmp_path)
    second = _plan(tmp_path)

    assert first == second
    assert first["status"] == "would_schedule"
    assert first["blocking_reasons"] == []
    assert first["schedule"]["activation_action"] == "would_create"
    assert first["schedule"]["scheduled_for"] > first["planned_at"]
    assert first["schedule"]["deterministic_jitter_seconds"] <= 30
    assert [item["execution_kind"] for item in first["execution"]["work_items"]] == [
        "initial",
        "retry",
    ]
    assert validate_notification_worker_plan(first) == first


def test_changed_plan_time_changes_identity_but_remains_canonical(
    tmp_path: Path,
) -> None:
    first = _plan(tmp_path)
    worker_path, delivery_path, destination_path = _paths(
        tmp_path,
        worker_enabled=True,
        delivery_enabled=True,
        retry_enabled=True,
        destination_enabled=True,
    )
    later = plan_notification_worker(
        worker_id=WORKER_ID,
        planned_at="2026-09-05T20:01:00+00:00",
        worker_config_path=worker_path,
        delivery_config_path=delivery_path,
        destination_config_path=destination_path,
    )

    assert later["plan_id"] != first["plan_id"]
    assert validate_notification_worker_plan(later) == later


def test_endpoint_environment_mismatch_is_a_visible_blocker(
    tmp_path: Path,
) -> None:
    plan = _plan(
        tmp_path,
        endpoint_env="RISK_NOTIFICATION_WEBHOOK_URL_V2",
    )

    assert plan["status"] == "blocked"
    assert plan["blocking_reasons"] == ["endpoint_environment_mismatch"]
    assert plan["destination"]["endpoint_value_recorded"] is False


def test_disabled_delivery_and_retry_are_visible_for_enabled_worker(
    tmp_path: Path,
) -> None:
    plan = _plan(
        tmp_path,
        delivery_enabled=False,
        retry_enabled=False,
        destination_enabled=True,
    )

    assert plan["status"] == "blocked"
    assert plan["blocking_reasons"] == [
        "delivery_disabled",
        "retry_execution_disabled",
    ]


def test_unsafe_worker_configuration_fails_closed(tmp_path: Path) -> None:
    cases = []

    concurrency = _worker_payload(enabled=True)
    concurrency["workers"][WORKER_ID]["limits"]["max_concurrency"] = 2
    cases.append((concurrency, "max_concurrency"))

    duplicate_kinds = _worker_payload(enabled=True)
    duplicate_kinds["workers"][WORKER_ID]["execution_kinds"] = [
        "initial",
        "retry",
        "retry",
    ]
    cases.append((duplicate_kinds, "sorted and unique"))

    readiness = _worker_payload(enabled=True)
    readiness["workers"][WORKER_ID]["readiness"]["max_age_seconds"] = 301
    cases.append((readiness, "readiness max_age_seconds"))

    suspension = _worker_payload(enabled=True)
    suspension["workers"][WORKER_ID]["suspension"][
        "block_on_persistence_ambiguity"
    ] = False
    cases.append((suspension, "must block on readiness"))

    for index, (payload, message) in enumerate(cases):
        path = _write_yaml(tmp_path / f"invalid-{index}.yaml", payload)
        with pytest.raises(ValidationError, match=message):
            load_notification_workers(path)


def test_worker_limits_must_fit_delivery_and_retry_contract(
    tmp_path: Path,
) -> None:
    worker = _worker_payload(enabled=True)
    worker["workers"][WORKER_ID]["limits"]["max_retry_events"] = 26
    worker_path = _write_yaml(tmp_path / "workers.yaml", worker)
    delivery_path = _write_yaml(
        tmp_path / "delivery.yaml",
        _delivery_payload(enabled=True, retry_enabled=True),
    )
    destination_path = _write_yaml(
        tmp_path / "destinations.yaml",
        _destination_payload(enabled=True),
    )

    with pytest.raises(ValidationError, match="webhook batch limit"):
        plan_notification_worker(
            worker_id=WORKER_ID,
            planned_at=BASE_TIME,
            worker_config_path=worker_path,
            delivery_config_path=delivery_path,
            destination_config_path=destination_path,
        )


def test_plan_validation_rejects_identity_and_side_effect_tampering(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)

    changed_id = copy.deepcopy(plan)
    changed_id["plan_id"] = "portfolio-risk-notification-worker-plan-v1-plan-tampered"
    with pytest.raises(ValidationError, match="plan_id"):
        validate_notification_worker_plan(changed_id)

    changed_effect = copy.deepcopy(plan)
    changed_effect["side_effects"]["external_request_performed"] = True
    with pytest.raises(ValidationError, match="side effect"):
        validate_notification_worker_plan(changed_effect)


def test_symlinked_worker_configuration_is_rejected(tmp_path: Path) -> None:
    target = _write_yaml(tmp_path / "workers.yaml", _worker_payload())
    link = tmp_path / "workers-link.yaml"
    link.symlink_to(target)

    with pytest.raises(ValidationError, match="symbolic link"):
        load_notification_workers(link)


def test_cli_writes_credential_free_summary(tmp_path: Path, capsys: Any) -> None:
    worker_path, delivery_path, destination_path = _paths(
        tmp_path,
        worker_enabled=True,
        delivery_enabled=True,
        retry_enabled=True,
        destination_enabled=True,
    )
    summary_path = tmp_path / "summary.json"

    exit_code = main(
        [
            "--worker-id",
            WORKER_ID,
            "--planned-at",
            BASE_TIME.isoformat(),
            "--config",
            str(worker_path),
            "--delivery-config",
            str(delivery_path),
            "--destination-config",
            str(destination_path),
            "--summary-json",
            str(summary_path),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "would_schedule"
    rendered = json.dumps(summary, sort_keys=True)
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "RISK_NOTIFICATION_WEBHOOK_URL=" not in rendered
    assert json.loads(capsys.readouterr().out) == summary
