from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.portfolio_risk_notification_destination_authority import (
    MODEL_VERSION as DESTINATION_AUTHORITY_MODEL_VERSION,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    NotificationDestination,
    evaluate_destination_activation,
    load_notification_destinations,
)

MODEL_VERSION = "portfolio-risk-notification-destination-transition-plan-v1"
OPERATIONS = frozenset({"disable", "rollback", "rotate"})
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
ENVIRONMENT_NAME = re.compile(r"^[A-Z][A-Z0-9_]{2,127}$")
SIDE_EFFECT_FLAGS = (
    "acknowledgement_mutated",
    "delivery_attempt_written",
    "endpoint_value_recorded",
    "external_request_performed",
    "infrastructure_deployed",
    "outbox_mutated",
)


def _exact_mapping(value: Any, label: str, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be a mapping")
    actual = set(value)
    if actual != keys:
        raise ValidationError(
            f"{label} fields are invalid; "
            f"missing={sorted(keys - actual)}, unknown={sorted(actual - keys)}"
        )
    return value


def _safe_text(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be ISO-8601") from None
    else:
        raise ValidationError(f"{label} must be timezone-aware")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _reviewers(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError("transition side reviewed_by must be an array")
    parsed = [_safe_text(item, "reviewed_by item") for item in value]
    result = [item for item in parsed if item is not None]
    if result != sorted(set(result)):
        raise ValidationError("transition side reviewed_by must be canonical")
    return result


def _metadata(destination: NotificationDestination) -> dict[str, Any]:
    return {
        "allowed_event_types": list(destination.allowed_event_types),
        "channel": destination.channel,
        "data_classification": destination.data_classification,
        "owner": {
            "contact": destination.owner.contact,
            "team": destination.owner.team,
        },
        "purpose": destination.purpose,
        "recipient_scope": destination.recipient_scope,
    }


def _side(
    destination: NotificationDestination,
    evidence: Mapping[str, Any],
) -> dict[str, Any]:
    activation = evidence["activation"]
    return {
        "activation_status": activation["status"],
        "change_request_id": activation["change_request_id"],
        "endpoint_environment_variable": destination.endpoint_env,
        "fingerprint": destination.fingerprint,
        "review_expires_at": activation["review_expires_at"],
        "reviewed_at": activation["reviewed_at"],
        "reviewed_by": list(destination.activation.reviewed_by),
    }


def canonical_transition_plan_bytes(plan: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            plan,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("destination transition plan is not canonical JSON") from None


def _plan_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_transition_plan_bytes(identity)).hexdigest()[:24]
    return f"{MODEL_VERSION}-plan-{digest}"


def _validate_transition_semantics(
    *,
    operation: str,
    current: Mapping[str, Any],
    target: Mapping[str, Any],
    prior_plan_id: str | None,
) -> None:
    current_status = current["activation_status"]
    target_status = target["activation_status"]
    endpoint_changed = (
        current["endpoint_environment_variable"]
        != target["endpoint_environment_variable"]
    )
    if current["fingerprint"] == target["fingerprint"]:
        raise ValidationError("transition fingerprints must differ")

    if operation == "rotate":
        if current_status != "active" or target_status != "active":
            raise ValidationError("rotate requires active current and target destinations")
        if not endpoint_changed:
            raise ValidationError("rotate requires a new endpoint environment identity")
        if prior_plan_id is not None:
            raise ValidationError("rotate must not reference a prior plan")
    elif operation == "disable":
        if current_status != "active" or target_status != "disabled":
            raise ValidationError("disable requires active current and disabled target")
        if endpoint_changed:
            raise ValidationError("disable must retain the endpoint environment identity")
        if prior_plan_id is not None:
            raise ValidationError("disable must not reference a prior plan")
    else:
        if current_status != "disabled" or target_status != "active":
            raise ValidationError("rollback requires disabled current and active target")
        if prior_plan_id is None:
            raise ValidationError("rollback requires prior_plan_id")
        if not endpoint_changed:
            raise ValidationError("rollback requires the prior endpoint environment identity")


def build_notification_destination_transition_plan(
    *,
    operation: str,
    current_config_path: Path,
    target_config_path: Path,
    destination_id: str,
    planned_at: datetime | str,
    prior_plan_id: str | None = None,
) -> dict[str, Any]:
    if operation not in OPERATIONS:
        raise ValidationError("notification destination transition operation is invalid")
    selected_destination_id = _safe_text(destination_id, "destination_id")
    selected_prior_plan_id = _safe_text(
        prior_plan_id,
        "prior_plan_id",
        optional=True,
    )
    assert selected_destination_id is not None
    as_of = _aware_utc(planned_at, "planned_at")

    current_destinations = load_notification_destinations(current_config_path)
    target_destinations = load_notification_destinations(target_config_path)
    current_destination = current_destinations.get(selected_destination_id)
    target_destination = target_destinations.get(selected_destination_id)
    if current_destination is None or target_destination is None:
        raise ValidationError("transition destination must exist in both configurations")
    if _metadata(current_destination) != _metadata(target_destination):
        raise ValidationError(
            "transition may change only endpoint identity and activation evidence"
        )

    current_evidence = evaluate_destination_activation(
        current_destination,
        evaluated_at=as_of,
    )
    target_evidence = evaluate_destination_activation(
        target_destination,
        evaluated_at=as_of,
    )
    current = _side(current_destination, current_evidence)
    target = _side(target_destination, target_evidence)
    _validate_transition_semantics(
        operation=operation,
        current=current,
        target=target,
        prior_plan_id=selected_prior_plan_id,
    )

    identity = {
        "current": current,
        "current_authority_accepted_by_target": False,
        "destination_id": selected_destination_id,
        "endpoint_environment_changed": (
            current["endpoint_environment_variable"]
            != target["endpoint_environment_variable"]
        ),
        "metadata_unchanged": True,
        "model_version": MODEL_VERSION,
        "operation": operation,
        "planned_at": as_of.isoformat(),
        "prior_plan_id": selected_prior_plan_id,
        "target": target,
        "target_authority_required": operation != "disable",
    }
    return {
        "plan_id": _plan_id(identity),
        **identity,
        "acknowledgement_mutated": False,
        "delivery_attempt_written": False,
        "endpoint_value_recorded": False,
        "external_request_performed": False,
        "infrastructure_deployed": False,
        "outbox_mutated": False,
    }


def _canonical_side(value: Any, label: str, *, planned_at: datetime) -> dict[str, Any]:
    side = _exact_mapping(
        value,
        label,
        {
            "activation_status",
            "change_request_id",
            "endpoint_environment_variable",
            "fingerprint",
            "review_expires_at",
            "reviewed_at",
            "reviewed_by",
        },
    )
    status = side["activation_status"]
    if status not in {"active", "disabled"}:
        raise ValidationError(f"{label}.activation_status is invalid")
    fingerprint = _safe_text(side["fingerprint"], f"{label}.fingerprint")
    endpoint = side["endpoint_environment_variable"]
    if not isinstance(endpoint, str) or not ENVIRONMENT_NAME.fullmatch(endpoint):
        raise ValidationError(f"{label}.endpoint environment is invalid")
    change_request_id = _safe_text(
        side["change_request_id"],
        f"{label}.change_request_id",
        optional=True,
    )
    reviewed_by = _reviewers(side["reviewed_by"])
    reviewed_at_value = side["reviewed_at"]
    expires_at_value = side["review_expires_at"]
    if status == "active":
        if change_request_id is None or not reviewed_by:
            raise ValidationError(f"{label} active review evidence is incomplete")
        reviewed_at = _aware_utc(reviewed_at_value, f"{label}.reviewed_at")
        expires_at = _aware_utc(expires_at_value, f"{label}.review_expires_at")
        if not reviewed_at <= planned_at < expires_at:
            raise ValidationError(f"{label} is outside its active review window")
        reviewed_at_output: str | None = reviewed_at.isoformat()
        expires_at_output: str | None = expires_at.isoformat()
    else:
        if (
            change_request_id is not None
            or reviewed_by
            or reviewed_at_value is not None
            or expires_at_value is not None
        ):
            raise ValidationError(f"{label} disabled evidence must be empty")
        reviewed_at_output = None
        expires_at_output = None
    assert fingerprint is not None
    return {
        "activation_status": status,
        "change_request_id": change_request_id,
        "endpoint_environment_variable": endpoint,
        "fingerprint": fingerprint,
        "review_expires_at": expires_at_output,
        "reviewed_at": reviewed_at_output,
        "reviewed_by": reviewed_by,
    }


def validate_notification_destination_transition_plan(
    plan: Mapping[str, Any],
) -> dict[str, Any]:
    exact = _exact_mapping(
        plan,
        "destination transition plan",
        {
            "acknowledgement_mutated",
            "current",
            "current_authority_accepted_by_target",
            "delivery_attempt_written",
            "destination_id",
            "endpoint_environment_changed",
            "endpoint_value_recorded",
            "external_request_performed",
            "infrastructure_deployed",
            "metadata_unchanged",
            "model_version",
            "operation",
            "outbox_mutated",
            "plan_id",
            "planned_at",
            "prior_plan_id",
            "target",
            "target_authority_required",
        },
    )
    if exact["model_version"] != MODEL_VERSION:
        raise ValidationError("destination transition plan model_version is unsupported")
    operation = exact["operation"]
    if operation not in OPERATIONS:
        raise ValidationError("destination transition plan operation is invalid")
    planned_at = _aware_utc(exact["planned_at"], "planned_at")
    destination_id = _safe_text(exact["destination_id"], "destination_id")
    prior_plan_id = _safe_text(
        exact["prior_plan_id"],
        "prior_plan_id",
        optional=True,
    )
    current = _canonical_side(exact["current"], "current", planned_at=planned_at)
    target = _canonical_side(exact["target"], "target", planned_at=planned_at)
    _validate_transition_semantics(
        operation=operation,
        current=current,
        target=target,
        prior_plan_id=prior_plan_id,
    )
    if exact["metadata_unchanged"] is not True:
        raise ValidationError("destination transition metadata changed")
    expected_endpoint_changed = (
        current["endpoint_environment_variable"]
        != target["endpoint_environment_variable"]
    )
    if exact["endpoint_environment_changed"] is not expected_endpoint_changed:
        raise ValidationError("endpoint environment change evidence is inconsistent")
    if exact["target_authority_required"] is not (operation != "disable"):
        raise ValidationError("target authority requirement is inconsistent")
    if exact["current_authority_accepted_by_target"] is not False:
        raise ValidationError("current authority must not authorise the target")
    if any(exact[flag] is not False for flag in SIDE_EFFECT_FLAGS):
        raise ValidationError("destination transition side-effect evidence is invalid")
    assert destination_id is not None
    identity = {
        "current": current,
        "current_authority_accepted_by_target": False,
        "destination_id": destination_id,
        "endpoint_environment_changed": expected_endpoint_changed,
        "metadata_unchanged": True,
        "model_version": MODEL_VERSION,
        "operation": operation,
        "planned_at": planned_at.isoformat(),
        "prior_plan_id": prior_plan_id,
        "target": target,
        "target_authority_required": operation != "disable",
    }
    expected_plan_id = _plan_id(identity)
    if exact["plan_id"] != expected_plan_id:
        raise ValidationError("destination transition plan identity is invalid")
    return {
        "plan_id": expected_plan_id,
        **identity,
        **{flag: False for flag in SIDE_EFFECT_FLAGS},
    }


def validate_target_destination_authority(
    *,
    plan: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> dict[str, Any]:
    validated_plan = validate_notification_destination_transition_plan(plan)
    if validated_plan["target_authority_required"] is not True:
        raise ValidationError("disabled transition must not receive target authority")
    exact = _exact_mapping(
        authority,
        "target destination authority",
        {
            "acknowledgement_mutated",
            "activation",
            "active",
            "allowed_event_types",
            "authority_id",
            "channel",
            "delivery_attempt_written",
            "destination_fingerprint",
            "destination_id",
            "endpoint_environment_variable",
            "endpoint_value_recorded",
            "evaluated_at",
            "evaluated_event_types",
            "external_request_performed",
            "model_version",
            "outbox_mutated",
        },
    )
    if exact["model_version"] != DESTINATION_AUTHORITY_MODEL_VERSION:
        raise ValidationError("target destination authority model_version is unsupported")
    if exact["active"] is not True:
        raise ValidationError("target destination authority must be active")
    target = validated_plan["target"]
    comparisons = {
        "destination_id": validated_plan["destination_id"],
        "destination_fingerprint": target["fingerprint"],
        "endpoint_environment_variable": target["endpoint_environment_variable"],
        "evaluated_at": validated_plan["planned_at"],
    }
    for field, expected in comparisons.items():
        if exact[field] != expected:
            raise ValidationError(f"target destination authority {field} does not match")
    activation = exact["activation"]
    if not isinstance(activation, Mapping) or activation.get("status") != "active":
        raise ValidationError("target destination authority activation is not active")
    if any(
        exact[flag] is not False
        for flag in (
            "acknowledgement_mutated",
            "delivery_attempt_written",
            "endpoint_value_recorded",
            "external_request_performed",
            "outbox_mutated",
        )
    ):
        raise ValidationError("target destination authority side effects are invalid")
    return dict(exact)


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("destination transition summary must not be a symbolic link")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True, allow_nan=False) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except (OSError, TypeError, ValueError):
        temporary.unlink(missing_ok=True)
        raise StorageError("unable to write destination transition summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build a delivery-free notification destination rotation, disablement, "
            "or rollback plan."
        )
    )
    parser.add_argument("--operation", choices=sorted(OPERATIONS), required=True)
    parser.add_argument("--current-config", type=Path, required=True)
    parser.add_argument("--target-config", type=Path, required=True)
    parser.add_argument("--destination-id", required=True)
    parser.add_argument("--planned-at", required=True)
    parser.add_argument("--prior-plan-id")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        plan = build_notification_destination_transition_plan(
            operation=args.operation,
            current_config_path=args.current_config,
            target_config_path=args.target_config,
            destination_id=args.destination_id,
            planned_at=args.planned_at,
            prior_plan_id=args.prior_plan_id,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, plan)
    except (StorageError, ValidationError) as exc:
        print(f"Notification destination transition rejected: {exc}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "Notification destination transition failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(plan, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
