from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from src.orchestration.notification_worker_readiness import assess_worker_readiness
from src.orchestration.plan_notification_worker import _plan_id
from test_notification_worker_readiness import evidence_fixture, retime
from test_reviewed_notification_worker_authority import configurations as configurations


@pytest.mark.parametrize("offset", [-301, 1])
def test_overall_snapshot_must_be_fresh_and_not_future(
    configurations: dict[str, Path], offset: int,
) -> None:
    evidence = evidence_fixture(configurations)
    instant = datetime.fromisoformat(evidence["evaluated_at"]) + timedelta(seconds=offset)
    evidence["observed_at"] = instant.isoformat()
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert f"observation_{'stale' if offset < 0 else 'future'}" in result["reasons"]


def test_exact_age_limit_is_allowed(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    boundary = datetime.fromisoformat(evidence["evaluated_at"]) - timedelta(seconds=300)
    for row in evidence["readiness"]:
        row["evaluated_at"] = boundary.isoformat()
    for row in evidence["health"]:
        row["observed_at"] = boundary.isoformat()
    assert assess_worker_readiness(evidence)["assessment"] == "may_run"


def test_early_unhealthy_invocation_must_not_merely_wait(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    retime(evidence, datetime.fromisoformat(evidence["scheduled_for"]) - timedelta(seconds=1))
    evidence["health"][1]["persistence_ambiguity"] = True
    result = assess_worker_readiness(evidence)
    assert result["assessment"] == "must_suspend"
    assert result["reasons"] == ["slot_not_due", "persistence_ambiguity:retry"]


def test_review_expiry_cannot_be_shorter_than_retained_authority(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    evidence["destination_review_expires_at"] = (
        datetime.fromisoformat(evidence["scheduled_for"]) + timedelta(seconds=30)
    ).isoformat()
    result = assess_worker_readiness(evidence)
    assert result["reasons"] == ["authority_exceeds_destination_review"]
    assert result["assessment"] == "must_suspend"


def test_well_formed_blocked_readiness_still_blocks(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    evidence["readiness"][1].update(status="blocked", decision="block")
    result = assess_worker_readiness(evidence)
    assert result["reasons"] == ["readiness_blocked:retry"]
    assert result["assessment"] == "must_suspend"


def test_initial_only_configuration_requires_only_initial_observations(configurations: dict[str, Path]) -> None:
    path = configurations["worker_config_path"]
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["workers"]["risk-operations-managed"]["execution_kinds"] = ["initial"]
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    evidence = evidence_fixture(configurations)
    assert len(evidence["readiness"]) == 1
    assert len(evidence["health"]) == 1
    assert assess_worker_readiness(evidence)["assessment"] == "may_run"


def test_consistent_disabled_config_cannot_pass_even_after_rehash(configurations: dict[str, Path]) -> None:
    evidence = evidence_fixture(configurations)
    plan = evidence["configuration_plan"]
    plan["worker"]["enabled"] = False
    plan["status"] = "disabled"
    plan["blocking_reasons"] = ["worker_disabled"]
    plan["schedule"]["activation_action"] = "none"
    plan["plan_id"] = _plan_id({key: value for key, value in plan.items() if key != "plan_id"})
    result = assess_worker_readiness(evidence)
    assert "configuration_blocked" in result["reasons"]
    assert result["assessment"] == "must_suspend"
