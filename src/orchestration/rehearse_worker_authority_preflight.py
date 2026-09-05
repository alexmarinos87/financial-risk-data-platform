"""Deterministic no-network rehearsal; all authority observations are synthetic."""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import sys
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition, canonical_bytes,
)
from src.orchestration.notification_worker_authority_preflight import (
    evaluate_worker_authority_preflight, validate_worker_authority_preflight,
)
from src.orchestration.notification_worker_summary import (
    MAX_SUMMARY_BYTES, write_notification_worker_summary,
)
from src.orchestration.plan_notification_worker import (
    NotificationWorker, _require_regular_file, build_notification_worker_plan,
    load_notification_workers,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    DestinationActivation, load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    aware_utc, load_retry_execution_contract,
)

MODEL_VERSION = "portfolio-risk-worker-authority-preflight-rehearsal-v1"
CONFIG_PATHS = (
    Path("config/notification_workers.yaml"),
    Path("config/notification_delivery.yaml"),
    Path("config/notification_destinations.yaml"),
)


def _rehearse(*, planned_at: datetime | str, worker_id: str) -> dict[str, Any]:
    planned = aware_utc(planned_at, "planned_at")
    if planned.microsecond:
        raise ValidationError("rehearsal planned_at must be a whole-second instant")
    for path in CONFIG_PATHS:
        _require_regular_file(path, "rehearsal configuration")
    workers = load_notification_workers(CONFIG_PATHS[0])
    original = workers.get(worker_id)
    if original is None:
        raise ValidationError("rehearsal worker does not exist")
    worker = replace(original, enabled=True)
    delivery, retry_policy, retry_execution = load_retry_execution_contract(CONFIG_PATHS[1])
    destination = load_notification_destinations(CONFIG_PATHS[2]).get(worker.destination_id)
    if destination is None:
        raise ValidationError("rehearsal destination does not exist")
    # Only these in-memory copies are enabled. No configuration is written.
    delivery = replace(delivery, enabled=True)
    retry_execution = replace(retry_execution, enabled=True)
    review_expiry = planned + timedelta(days=2)
    destination = replace(destination, activation=DestinationActivation(
        enabled=True, change_request_id="SYNTHETIC-PREFLIGHT-REHEARSAL",
        reviewed_by=("synthetic-reviewer",), reviewed_at=planned,
        review_expires_at=review_expiry,
    ))

    def plan_for(selected: NotificationWorker) -> dict[str, Any]:
        assert destination is not None
        return build_notification_worker_plan(
            worker=selected, delivery=delivery, retry_policy=retry_policy,
            retry_execution=retry_execution, destination=destination, planned_at=planned,
        )

    plan = plan_for(worker)
    slot = aware_utc(plan["schedule"]["scheduled_for"], "scheduled_for")
    grant_expiry = slot + timedelta(seconds=worker.limits.execution_timeout_seconds)
    authority = build_worker_authority_transition(
        plan=plan, request_id="SYNTHETIC-PREFLIGHT-GRANT", operator_id="synthetic-operator",
        action="activate", requested_at=planned, effective_at=planned,
        reviewed_by=["synthetic-reviewer"], expires_at=grant_expiry,
    )
    base = {
        "worker_id": worker.worker_id, "selected_transition_id": authority["transition_id"],
        "scheduled_for": slot.isoformat(), "evaluated_at": slot.isoformat(),
        "observed_at": slot.isoformat(), "current_authority": authority,
        "configuration_plan": plan, "destination_review_expires_at": review_expiry.isoformat(),
    }
    cases: list[tuple[str, str, str | None, dict[str, Any]]] = []

    def add(name: str, outcome: str, reason: str | None = None, **changes: Any) -> None:
        evidence = copy.deepcopy(base)
        evidence.update(changes)
        cases.append((name, outcome, reason, evidence))

    add("due_slot", "eligible_for_health_review")
    early = (slot - timedelta(seconds=1)).isoformat()
    add("early_slot", "wait", "slot_not_due", evaluated_at=early, observed_at=early)
    stopped = build_worker_authority_transition(
        plan=plan, request_id="SYNTHETIC-PREFLIGHT-STOP", operator_id="synthetic-operator",
        action="suspend", requested_at=planned + timedelta(seconds=1),
        effective_at=planned + timedelta(seconds=1), reason_codes=["operator_request"],
        previous=authority,
    )
    add("newer_stop", "blocked", "authority_superseded", current_authority=stopped)
    add("stale_observation", "blocked", "observation_stale", observed_at=(
        slot - timedelta(seconds=worker.readiness.max_age_seconds + 1)
    ).isoformat())
    add("future_observation", "blocked", "observation_future", observed_at=(
        slot + timedelta(seconds=1)
    ).isoformat())
    add("configuration_disabled", "blocked", "configuration_blocked",
        configuration_plan=plan_for(replace(worker, enabled=False)))
    add("authority_missing", "blocked", "authority_missing", current_authority=None)
    add("authority_expired", "blocked", "authority_expired",
        evaluated_at=grant_expiry.isoformat(), observed_at=grant_expiry.isoformat())
    # An intentionally inconsistent review observation is an adversarial fixture,
    # not a claim that a real configuration source was queried or approved.
    add("review_expired", "blocked", "destination_review_expired",
        destination_review_expires_at=slot.isoformat())
    add("wrong_slot", "blocked", "schedule_slot_mismatch",
        scheduled_for=(slot + timedelta(seconds=1)).isoformat())

    scenarios = []
    for name, expected, required_reason, evidence in cases:
        result = evaluate_worker_authority_preflight(evidence)
        validate_worker_authority_preflight(result, evidence=evidence)
        passed = result["outcome"] == expected and (
            required_reason is None or required_reason in result["reasons"]
        )
        if not passed:
            raise ValidationError(f"worker preflight rehearsal expectation failed: {name}")
        scenarios.append({
            "scenario_id": name, "expected_outcome": expected, "required_reason": required_reason,
            "passed": True, "evidence": evidence, "preflight": result,
        })
    identity = {
        "model_version": MODEL_VERSION, "planned_at": planned.isoformat(),
        "worker_id": worker.worker_id, "observations_synthetic": True,
        "scenario_count": len(scenarios), "passed_count": len(scenarios), "failed_count": 0,
        "scenarios": scenarios, "configuration_files_modified": False,
        "database_read_performed": False, "readiness_evaluated": False,
        "runtime_permission_granted": False, "scheduler_mutated": False,
        "external_request_performed": False, "shared_lock_acquired": False,
    }
    digest = hashlib.sha256(canonical_bytes(identity)).hexdigest()
    result = {"rehearsal_id": f"{MODEL_VERSION}-{digest}", **identity}
    if len(canonical_bytes(result)) > MAX_SUMMARY_BYTES:
        raise ValidationError("worker preflight rehearsal exceeds 1 MB")
    return result


def rehearse_worker_authority_preflight(
    *, planned_at: datetime | str, worker_id: str = "risk-operations-managed",
) -> dict[str, Any]:
    try:
        return _rehearse(planned_at=planned_at, worker_id=worker_id)
    except (TypeError, ValueError, RecursionError, OverflowError, UnicodeError):
        raise ValidationError("worker preflight rehearsal input is invalid") from None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rehearse authority preflight without network or database access.")
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--worker-id", default="risk-operations-managed")
    parser.add_argument("--summary-json", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.summary_json is not None and args.summary_json.resolve().is_relative_to(Path("config").resolve()):
            raise ValidationError("rehearsal output must not replace repository configuration")
        result = rehearse_worker_authority_preflight(planned_at=args.planned_at, worker_id=args.worker_id)
        if args.summary_json is not None:
            write_notification_worker_summary(args.summary_json, result)
    except (ValidationError, StorageError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except OSError:
        print("worker preflight rehearsal filesystem operation failed", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
