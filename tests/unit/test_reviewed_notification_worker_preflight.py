from __future__ import annotations

import copy
import hashlib
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition, canonical_bytes,
)
from src.orchestration import reviewed_notification_worker_preflight as reviewed
from src.warehouse.notification_worker_authority_snapshot import read_worker_authority_snapshot_with_cursor
from test_notification_worker_authority_snapshot import SnapshotCursor, snapshot_row
from test_reviewed_notification_worker_authority import (
    NOW, WORKER_ID, _grant, _plan, configurations as configurations,
)


def reviewed_snapshot(paths: dict[str, Path], *, observed: datetime | None = None) -> dict[str, Any]:
    plan = _plan(paths)
    current = _grant(plan, paths)
    when = datetime.fromisoformat(plan["schedule"]["scheduled_for"]) if observed is None else observed
    return read_worker_authority_snapshot_with_cursor(
        SnapshotCursor(snapshot_row(current, observed=when)), worker_id=WORKER_ID,
    )


def bundle(paths: dict[str, Path], snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    captured = reviewed_snapshot(paths) if snapshot is None else snapshot
    current = captured["transition"]
    return reviewed.build_reviewed_worker_preflight(
        snapshot=captured, selected_transition_id=current["transition_id"],
        scheduled_for=current["plan"]["schedule"]["scheduled_for"], **paths,
    )


def test_due_preflight_rebuilds_actual_reviewed_sources(configurations: dict[str, Path]) -> None:
    captured = reviewed_snapshot(configurations)
    before = copy.deepcopy(captured)
    result = bundle(configurations, captured)
    assert result["preflight"]["outcome"] == "eligible_for_health_review"
    assert result["preflight"]["readiness_evaluated"] is False
    assert result["runtime_permission_granted"] is False
    assert result["configuration_validated"] is True
    assert result["evidence"]["destination_review_expires_at"] == "2026-10-01T00:00:00+00:00"
    assert result["evidence"]["evaluated_at"] == captured["observed_at"]
    assert result["evaluation_scope"] == "captured_database_instant"
    assert reviewed.validate_reviewed_worker_preflight(result, **configurations) == result
    assert captured == before
    result["snapshot"]["transition"]["reason_codes"].append("operator_request")
    assert captured == before


@pytest.mark.parametrize("key", ["worker_config_path", "delivery_config_path", "destination_config_path"])
def test_changed_reviewed_configuration_rejects_retained_bundle(configurations: dict[str, Path], key: str) -> None:
    result = bundle(configurations)
    path = configurations[key]
    value = yaml.safe_load(path.read_text())
    if key == "worker_config_path":
        value["workers"][WORKER_ID]["enabled"] = False
    elif key == "delivery_config_path":
        value["delivery"]["webhook"]["enabled"] = False
    else:
        value["destinations"]["risk-operations-webhook"]["activation"]["review_expires_at"] = "2026-09-20T00:00:00+00:00"
    path.write_text(yaml.safe_dump(value), encoding="utf-8")
    with pytest.raises(ValidationError):
        reviewed.validate_reviewed_worker_preflight(result, **configurations)


def test_missing_authority_skips_configuration_io(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = read_worker_authority_snapshot_with_cursor(
        SnapshotCursor(snapshot_row(None)), worker_id=WORKER_ID,
    )

    def forbidden(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("unknown authority must not inspect configurations")

    monkeypatch.setattr(reviewed, "_bind_reviewed_plan", forbidden)
    result = reviewed.build_reviewed_worker_preflight(
        snapshot=captured, selected_transition_id="selected-old-grant", scheduled_for=NOW.isoformat(),
    )
    assert result["preflight"]["outcome"] == "blocked"
    assert "authority_missing" in result["preflight"]["reasons"]
    assert result["configuration_validated"] is False
    assert reviewed.validate_reviewed_worker_preflight(result) == result


@pytest.mark.parametrize("action", ["suspend", "disable"])
def test_newer_stop_blocks_an_old_selected_grant(configurations: dict[str, Path], action: str) -> None:
    captured = reviewed_snapshot(configurations)
    prior = captured["transition"]
    current = build_worker_authority_transition(
        plan=prior["plan"], action=action, request_id=f"reviewed-preflight-{action}",
        operator_id="operator", previous=prior, reason_codes=["operator_request"],
        requested_at=NOW + timedelta(seconds=2), effective_at=NOW + timedelta(seconds=2),
    )
    stopped = read_worker_authority_snapshot_with_cursor(
        SnapshotCursor(snapshot_row(current, observed=datetime.fromisoformat(captured["observed_at"]))),
        worker_id=WORKER_ID,
    )
    result = reviewed.build_reviewed_worker_preflight(
        snapshot=stopped, selected_transition_id=prior["transition_id"],
        scheduled_for=prior["plan"]["schedule"]["scheduled_for"], **configurations,
    )
    assert result["preflight"]["outcome"] == "blocked"
    assert "authority_superseded" in result["preflight"]["reasons"]
    assert f"authority_{current['to_state']}" in result["preflight"]["reasons"]


def test_slot_and_expiry_boundaries_reuse_existing_preflight(configurations: dict[str, Path]) -> None:
    captured = reviewed_snapshot(configurations)
    current = captured["transition"]
    slot = datetime.fromisoformat(current["plan"]["schedule"]["scheduled_for"])
    early = reviewed_snapshot(configurations, observed=slot - timedelta(seconds=1))
    assert bundle(configurations, early)["preflight"]["outcome"] == "wait"
    expired = reviewed_snapshot(configurations, observed=datetime.fromisoformat(current["expires_at"]))
    assert "authority_expired" in bundle(configurations, expired)["preflight"]["reasons"]
    wrong = reviewed.build_reviewed_worker_preflight(
        snapshot=captured, selected_transition_id=current["transition_id"],
        scheduled_for=(slot + timedelta(seconds=1)).isoformat(), **configurations,
    )
    assert "schedule_slot_mismatch" in wrong["preflight"]["reasons"]


@pytest.mark.parametrize("mutation", ["permission", "outcome", "expiry", "time", "scope", "extra"])
def test_rehashed_bundle_tampering_is_rejected(configurations: dict[str, Path], mutation: str) -> None:
    result = bundle(configurations)
    if mutation == "permission":
        result["runtime_permission_granted"] = True
    elif mutation == "outcome":
        result["preflight"]["outcome"] = "may_run"
    elif mutation == "expiry":
        result["evidence"]["destination_review_expires_at"] = "2099-01-01T00:00:00+00:00"
    elif mutation == "time":
        result["evidence"]["evaluated_at"] = NOW.isoformat()
    elif mutation == "scope":
        result["evaluation_scope"] = "current_runtime_permission"
    else:
        result["evidence"]["extra"] = True
    identity = {key: value for key, value in result.items() if key != "bundle_id"}
    result["bundle_id"] = f"{reviewed.MODEL_VERSION}-{hashlib.sha256(canonical_bytes(identity)).hexdigest()}"
    with pytest.raises(ValidationError):
        reviewed.validate_reviewed_worker_preflight(result, **configurations)


def test_config_symlink_and_missing_config_fail_closed(configurations: dict[str, Path], tmp_path: Path) -> None:
    captured = reviewed_snapshot(configurations)
    path = configurations["worker_config_path"]
    link = tmp_path / "linked-workers.yaml"
    link.symlink_to(path)
    with pytest.raises(ValidationError):
        bundle({**configurations, "worker_config_path": link}, captured)
    path.unlink()
    with pytest.raises(ValidationError):
        bundle(configurations, captured)
