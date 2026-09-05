"""Read-only configuration binding around the pure worker transition contract."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.exceptions import ValidationError
from src.orchestration.notification_worker_authority_contract import (
    build_worker_authority_transition,
    canonical_bytes,
    validate_worker_authority_chain,
    validate_worker_authority_transition,
)
from src.orchestration.plan_notification_worker import (
    _require_regular_file,
    build_notification_worker_plan,
    load_notification_workers,
    validate_notification_worker_plan,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    NotificationDestination,
    evaluate_destination_activation,
    load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    aware_utc,
    load_retry_execution_contract,
)


def _bind_reviewed_plan(
    plan: Mapping[str, Any], *, worker_config_path: Path,
    delivery_config_path: Path, destination_config_path: Path,
) -> tuple[dict[str, Any], NotificationDestination]:
    retained = validate_notification_worker_plan(plan)
    workers = load_notification_workers(worker_config_path)
    worker = workers.get(retained["worker"]["worker_id"])
    if worker is None:
        raise ValidationError("reviewed worker configuration does not contain the retained worker")
    _require_regular_file(delivery_config_path, "notification delivery configuration")
    _require_regular_file(destination_config_path, "notification destination configuration")
    delivery, retry_policy, retry_execution = load_retry_execution_contract(delivery_config_path)
    destinations = load_notification_destinations(destination_config_path)
    destination = destinations.get(worker.destination_id)
    if destination is None:
        raise ValidationError("reviewed destination configuration does not contain the worker destination")
    rebuilt = build_notification_worker_plan(
        worker=worker, delivery=delivery, retry_policy=retry_policy,
        retry_execution=retry_execution, destination=destination,
        planned_at=retained["planned_at"],
    )
    if canonical_bytes(retained) != canonical_bytes(rebuilt):
        raise ValidationError("retained worker plan does not match the supplied reviewed configurations")
    return retained, destination


def _validate_retained_plan_field(value: Mapping[str, Any]) -> None:
    if not isinstance(value, Mapping):
        raise ValidationError("retained worker authority must be a mapping")
    plan = value.get("plan")
    if not isinstance(plan, Mapping):
        raise ValidationError("retained worker authority must contain a plan mapping")
    validate_notification_worker_plan(plan)


def build_reviewed_worker_authority_transition(
    *, plan: Mapping[str, Any], request_id: str, operator_id: str, action: str,
    requested_at: datetime | str, effective_at: datetime | str,
    reviewed_by: Sequence[str] = (), expires_at: datetime | str | None = None,
    reason_codes: Sequence[str] = (), previous: Mapping[str, Any] | None = None,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Construct evidence only after rebuilding its exact reviewed plan.

    Configuration provenance, authenticated identities and current retained-head
    selection are caller responsibilities. Files must be immutable snapshots in
    trusted operator-owned directories. No execution authority is exercised.
    """
    retained, destination = _bind_reviewed_plan(
        plan, worker_config_path=worker_config_path,
        delivery_config_path=delivery_config_path,
        destination_config_path=destination_config_path,
    )
    prior = None
    if previous is not None:
        _validate_retained_plan_field(previous)
        prior = validate_worker_authority_transition(previous)
    result = build_worker_authority_transition(
        plan=retained, request_id=request_id, operator_id=operator_id,
        action=action, requested_at=requested_at, effective_at=effective_at,
        reviewed_by=reviewed_by, expires_at=expires_at, reason_codes=reason_codes,
        previous=prior,
    )
    if result["to_state"] == "active":
        effective = aware_utc(result["effective_at"], "effective_at")
        expiry = aware_utc(result["expires_at"], "expires_at")
        activation = evaluate_destination_activation(destination, evaluated_at=effective)
        if activation["activation"]["status"] != "active":
            raise ValidationError("destination review is not active at authority effective time")
        review_expiry = destination.activation.review_expires_at
        if review_expiry is None or expiry > review_expiry:
            raise ValidationError("worker authority expiry exceeds the destination review expiry")
    return result


def validate_reviewed_worker_authority_transition(
    transition: Mapping[str, Any], *, previous: Mapping[str, Any] | None = None,
    worker_config_path: Path = Path("config/notification_workers.yaml"),
    delivery_config_path: Path = Path("config/notification_delivery.yaml"),
    destination_config_path: Path = Path("config/notification_destinations.yaml"),
) -> dict[str, Any]:
    """Revalidate retained transition evidence against its reviewed snapshots."""
    _validate_retained_plan_field(transition)
    if previous is not None:
        _validate_retained_plan_field(previous)
    validated = validate_worker_authority_chain(transition, previous)
    result = build_reviewed_worker_authority_transition(
        plan=validated["plan"], request_id=validated["request_id"],
        operator_id=validated["operator_id"], action=validated["action"],
        requested_at=validated["requested_at"], effective_at=validated["effective_at"],
        reviewed_by=validated["reviewed_by"], expires_at=validated["expires_at"],
        reason_codes=validated["reason_codes"], previous=previous,
        worker_config_path=worker_config_path, delivery_config_path=delivery_config_path,
        destination_config_path=destination_config_path,
    )
    if canonical_bytes(transition) != canonical_bytes(result):
        raise ValidationError("retained worker authority differs from reviewed evidence")
    return result
