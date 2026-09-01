from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.orchestration.deliver_portfolio_risk_notifications import (
    WebhookDeliveryConfig,
)
from src.orchestration.portfolio_risk_notification_destination_contract import (
    NotificationDestination,
    evaluate_destination_activation,
    load_notification_destinations,
)
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    RetryExecutionPolicy,
    aware_utc,
    load_retry_execution_contract,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "portfolio-risk-notification-execution-readiness-gate-v1"
EXECUTION_KINDS = frozenset({"initial", "retry"})
MAX_AMBIGUITIES = 500
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
ACTIVATION_STATUSES = frozenset(
    {
        "checklist_expired",
        "checklist_incomplete",
        "checklist_not_yet_active",
        "ready",
        "rehearsal_evidence_conflict",
        "rehearsal_failed",
        "rehearsal_missing",
        "rehearsal_rejected",
        "rehearsal_superseded",
        "review_required",
    }
)
TRANSITION_STATUSES = frozenset(
    {
        "activation_not_ready",
        "ready",
        "transition_rehearsal_missing",
        "transition_rehearsal_superseded",
    }
)
BLOCKING_REASON_ORDER = (
    "delivery_disabled",
    "retry_execution_disabled",
    "destination_not_active",
    "configuration_mismatch",
    "activation_review_missing",
    "activation_not_ready",
    "activation_identity_mismatch",
    "transition_review_missing",
    "transition_rehearsal_missing",
    "transition_rehearsal_superseded",
    "transition_not_ready",
    "transition_identity_mismatch",
    "persistence_ambiguity",
)
SIDE_EFFECT_FLAGS = (
    "acknowledgement_mutated",
    "delivery_attempt_written",
    "external_request_performed",
    "outbox_mutated",
)

EvidenceReader = Callable[..., Mapping[str, Any]]


def _safe_text(value: Any, label: str, *, optional: bool = False) -> str | None:
    if value is None and optional:
        return None
    if not isinstance(value, str) or not SAFE_TEXT.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    return aware_utc(value, label)


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


def _quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("schema_name must be non-empty text")
    return '"' + value.replace('"', '""') + '"'


def _activation_review(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    review = _exact_mapping(
        value,
        "activation review",
        {
            "authority_id",
            "checklist_id",
            "destination_fingerprint",
            "destination_id",
            "operational_activation_ready",
            "review_status",
        },
    )
    ready = review["operational_activation_ready"]
    if type(ready) is not bool:
        raise ValidationError("activation review ready flag must be boolean")
    status = review["review_status"]
    if status not in ACTIVATION_STATUSES:
        raise ValidationError("activation review status is unsupported")
    return {
        "authority_id": _safe_text(review["authority_id"], "activation authority_id"),
        "checklist_id": _safe_text(review["checklist_id"], "activation checklist_id"),
        "destination_fingerprint": _safe_text(
            review["destination_fingerprint"],
            "activation destination_fingerprint",
        ),
        "destination_id": _safe_text(
            review["destination_id"],
            "activation destination_id",
        ),
        "operational_activation_ready": ready,
        "review_status": status,
    }


def _transition_review(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    review = _exact_mapping(
        value,
        "transition review",
        {
            "activation_review_status",
            "current_authority_id",
            "current_checklist_id",
            "current_destination_fingerprint",
            "destination_id",
            "operational_activation_ready",
            "rollback_authority_id",
            "rollback_checklist_id",
            "rollback_destination_fingerprint",
            "rollback_endpoint_environment_variable",
            "rollback_plan_id",
            "transition_matches_current_activation",
            "transition_ready",
            "transition_record_id",
            "transition_rehearsal_id",
            "transition_review_status",
        },
    )
    activation_ready = review["operational_activation_ready"]
    transition_matches = review["transition_matches_current_activation"]
    transition_ready = review["transition_ready"]
    if any(
        type(value) is not bool
        for value in (activation_ready, transition_matches, transition_ready)
    ):
        raise ValidationError("transition review flags must be boolean")
    status = review["transition_review_status"]
    if status not in TRANSITION_STATUSES:
        raise ValidationError("transition review status is unsupported")
    activation_status = review["activation_review_status"]
    if activation_status not in ACTIVATION_STATUSES:
        raise ValidationError("transition activation status is unsupported")
    return {
        "activation_review_status": activation_status,
        "current_authority_id": _safe_text(
            review["current_authority_id"],
            "transition current_authority_id",
        ),
        "current_checklist_id": _safe_text(
            review["current_checklist_id"],
            "transition current_checklist_id",
        ),
        "current_destination_fingerprint": _safe_text(
            review["current_destination_fingerprint"],
            "transition current_destination_fingerprint",
        ),
        "destination_id": _safe_text(
            review["destination_id"],
            "transition destination_id",
        ),
        "operational_activation_ready": activation_ready,
        "rollback_authority_id": _safe_text(
            review["rollback_authority_id"],
            "transition rollback_authority_id",
            optional=True,
        ),
        "rollback_checklist_id": _safe_text(
            review["rollback_checklist_id"],
            "transition rollback_checklist_id",
            optional=True,
        ),
        "rollback_destination_fingerprint": _safe_text(
            review["rollback_destination_fingerprint"],
            "transition rollback_destination_fingerprint",
            optional=True,
        ),
        "rollback_endpoint_environment_variable": _safe_text(
            review["rollback_endpoint_environment_variable"],
            "transition rollback_endpoint_environment_variable",
            optional=True,
        ),
        "rollback_plan_id": _safe_text(
            review["rollback_plan_id"],
            "transition rollback_plan_id",
            optional=True,
        ),
        "transition_matches_current_activation": transition_matches,
        "transition_ready": transition_ready,
        "transition_record_id": _safe_text(
            review["transition_record_id"],
            "transition record_id",
            optional=True,
        ),
        "transition_rehearsal_id": _safe_text(
            review["transition_rehearsal_id"],
            "transition rehearsal_id",
            optional=True,
        ),
        "transition_review_status": status,
    }


def _ambiguities(
    values: Any,
    *,
    destination_id: str,
) -> list[dict[str, Any]]:
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
        raise ValidationError("ambiguities must be a sequence")
    if len(values) > MAX_AMBIGUITIES:
        raise ValidationError("ambiguity evidence exceeds the reviewed maximum")
    parsed: list[dict[str, Any]] = []
    for index, value in enumerate(values, start=1):
        ambiguity = _exact_mapping(
            value,
            f"ambiguity {index}",
            {
                "destination_binding_status",
                "destination_bound",
                "destination_fingerprint",
                "destination_id",
                "endpoint_environment_variable",
                "event_id",
                "latest_execution_record_id",
                "uncertainty_record_id",
            },
        )
        bound = ambiguity["destination_bound"]
        if type(bound) is not bool:
            raise ValidationError("ambiguity destination_bound must be boolean")
        selected_destination_id = _safe_text(
            ambiguity["destination_id"],
            "ambiguity destination_id",
            optional=True,
        )
        if selected_destination_id not in {None, destination_id}:
            raise ValidationError("ambiguity belongs to another destination")
        parsed.append(
            {
                "destination_binding_status": _safe_text(
                    ambiguity["destination_binding_status"],
                    "ambiguity destination_binding_status",
                ),
                "destination_bound": bound,
                "destination_fingerprint": _safe_text(
                    ambiguity["destination_fingerprint"],
                    "ambiguity destination_fingerprint",
                    optional=True,
                ),
                "destination_id": selected_destination_id,
                "endpoint_environment_variable": _safe_text(
                    ambiguity["endpoint_environment_variable"],
                    "ambiguity endpoint_environment_variable",
                    optional=True,
                ),
                "event_id": _safe_text(
                    ambiguity["event_id"],
                    "ambiguity event_id",
                ),
                "latest_execution_record_id": _safe_text(
                    ambiguity["latest_execution_record_id"],
                    "ambiguity latest_execution_record_id",
                    optional=True,
                ),
                "uncertainty_record_id": _safe_text(
                    ambiguity["uncertainty_record_id"],
                    "ambiguity uncertainty_record_id",
                ),
            }
        )
    parsed.sort(key=lambda item: (item["event_id"], item["uncertainty_record_id"]))
    event_ids = [item["event_id"] for item in parsed]
    if len(event_ids) != len(set(event_ids)):
        raise ValidationError("ambiguity event IDs must be unique")
    return parsed


def _configuration(
    *,
    delivery_config: WebhookDeliveryConfig,
    retry_policy_fingerprint: str,
    retry_execution_policy: RetryExecutionPolicy,
) -> dict[str, Any]:
    return {
        "delivery_enabled": delivery_config.enabled,
        "delivery_fingerprint": _safe_text(
            delivery_config.fingerprint,
            "delivery fingerprint",
        ),
        "endpoint_environment_variable": _safe_text(
            delivery_config.endpoint_env,
            "delivery endpoint environment variable",
        ),
        "retry_execution_enabled": retry_execution_policy.enabled,
        "retry_execution_policy_fingerprint": _safe_text(
            retry_execution_policy.fingerprint,
            "retry execution policy fingerprint",
        ),
        "retry_policy_fingerprint": _safe_text(
            retry_policy_fingerprint,
            "retry policy fingerprint",
        ),
    }


def _destination(
    destination: NotificationDestination,
    *,
    evaluated_at: datetime,
) -> dict[str, Any]:
    activation = evaluate_destination_activation(
        destination,
        evaluated_at=evaluated_at,
    )
    return {
        "activation_status": activation["activation"]["status"],
        "destination_id": destination.destination_id,
        "endpoint_environment_variable": destination.endpoint_env,
        "fingerprint": destination.fingerprint,
    }


def _ambiguity_summary(values: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(values),
        "event_ids": sorted(value["event_id"] for value in values),
        "record_ids": sorted(value["uncertainty_record_id"] for value in values),
        "unbound_event_ids": sorted(
            value["event_id"]
            for value in values
            if value["destination_id"] is None or not value["destination_bound"]
        ),
    }


def _derive_reasons(
    *,
    execution_kind: str,
    configuration: Mapping[str, Any],
    destination: Mapping[str, Any],
    activation_review: Mapping[str, Any] | None,
    transition_review: Mapping[str, Any] | None,
    ambiguity: Mapping[str, Any],
) -> list[str]:
    present: set[str] = set()
    if configuration["delivery_enabled"] is not True:
        present.add("delivery_disabled")
    if (
        execution_kind == "retry"
        and configuration["retry_execution_enabled"] is not True
    ):
        present.add("retry_execution_disabled")
    if destination["activation_status"] != "active":
        present.add("destination_not_active")
    if (
        configuration["endpoint_environment_variable"]
        != destination["endpoint_environment_variable"]
    ):
        present.add("configuration_mismatch")

    if activation_review is None:
        present.add("activation_review_missing")
    else:
        if activation_review["operational_activation_ready"] is not True:
            present.add("activation_not_ready")
        if (
            activation_review["destination_id"] != destination["destination_id"]
            or activation_review["destination_fingerprint"]
            != destination["fingerprint"]
        ):
            present.add("activation_identity_mismatch")

    if transition_review is None:
        present.add("transition_review_missing")
    else:
        status = transition_review["transition_review_status"]
        if status == "transition_rehearsal_missing":
            present.add("transition_rehearsal_missing")
        elif status == "transition_rehearsal_superseded":
            present.add("transition_rehearsal_superseded")
        elif status != "ready":
            present.add("transition_not_ready")
        if transition_review["transition_ready"] is not (status == "ready"):
            present.add("transition_not_ready")
        if (
            transition_review["destination_id"] != destination["destination_id"]
            or transition_review["current_destination_fingerprint"]
            != destination["fingerprint"]
        ):
            present.add("transition_identity_mismatch")
        if activation_review is not None and (
            transition_review["current_authority_id"]
            != activation_review["authority_id"]
            or transition_review["current_checklist_id"]
            != activation_review["checklist_id"]
            or transition_review["activation_review_status"]
            != activation_review["review_status"]
            or transition_review["operational_activation_ready"]
            is not activation_review["operational_activation_ready"]
        ):
            present.add("transition_identity_mismatch")
        if (
            status == "ready"
            and transition_review["transition_matches_current_activation"] is not True
        ):
            present.add("transition_identity_mismatch")

    if ambiguity["count"] > 0:
        present.add("persistence_ambiguity")
    return [reason for reason in BLOCKING_REASON_ORDER if reason in present]


def _decision_identity(
    *,
    execution_kind: str,
    evaluated_at: str,
    configuration: Mapping[str, Any],
    destination: Mapping[str, Any],
    activation_review: Mapping[str, Any] | None,
    transition_review: Mapping[str, Any] | None,
    ambiguity: Mapping[str, Any],
    decision: str,
    blocking_reasons: Sequence[str],
) -> dict[str, Any]:
    return {
        "activation_review": activation_review,
        "ambiguity": ambiguity,
        "blocking_reasons": list(blocking_reasons),
        "configuration": configuration,
        "decision": decision,
        "destination": destination,
        "evaluated_at": evaluated_at,
        "execution_kind": execution_kind,
        "model_version": MODEL_VERSION,
        "transition_review": transition_review,
    }


def _decision_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-decision-{digest}"


def evaluate_notification_execution_readiness(
    *,
    execution_kind: str,
    evaluated_at: datetime | str,
    delivery_config: WebhookDeliveryConfig,
    retry_policy_fingerprint: str,
    retry_execution_policy: RetryExecutionPolicy,
    destination: NotificationDestination,
    activation_review: Mapping[str, Any] | None,
    transition_review: Mapping[str, Any] | None,
    ambiguities: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if execution_kind not in EXECUTION_KINDS:
        raise ValidationError("execution_kind must be initial or retry")
    as_of = _aware_utc(evaluated_at, "evaluated_at")
    selected_activation = _activation_review(activation_review)
    selected_transition = _transition_review(transition_review)
    selected_ambiguities = _ambiguities(
        ambiguities,
        destination_id=destination.destination_id,
    )
    configuration = _configuration(
        delivery_config=delivery_config,
        retry_policy_fingerprint=retry_policy_fingerprint,
        retry_execution_policy=retry_execution_policy,
    )
    destination_evidence = _destination(destination, evaluated_at=as_of)
    ambiguity = _ambiguity_summary(selected_ambiguities)
    reasons = _derive_reasons(
        execution_kind=execution_kind,
        configuration=configuration,
        destination=destination_evidence,
        activation_review=selected_activation,
        transition_review=selected_transition,
        ambiguity=ambiguity,
    )
    decision = "allow" if not reasons else "block"
    identity = _decision_identity(
        execution_kind=execution_kind,
        evaluated_at=as_of.isoformat(),
        configuration=configuration,
        destination=destination_evidence,
        activation_review=selected_activation,
        transition_review=selected_transition,
        ambiguity=ambiguity,
        decision=decision,
        blocking_reasons=reasons,
    )
    return {
        "decision_id": _decision_id(identity),
        **identity,
        "read_only": True,
        "acknowledgement_mutated": False,
        "delivery_attempt_written": False,
        "external_request_performed": False,
        "outbox_mutated": False,
    }


def _canonical_configuration(value: Any) -> dict[str, Any]:
    configuration = _exact_mapping(
        value,
        "readiness configuration",
        {
            "delivery_enabled",
            "delivery_fingerprint",
            "endpoint_environment_variable",
            "retry_execution_enabled",
            "retry_execution_policy_fingerprint",
            "retry_policy_fingerprint",
        },
    )
    for flag in ("delivery_enabled", "retry_execution_enabled"):
        if type(configuration[flag]) is not bool:
            raise ValidationError(f"{flag} must be boolean")
    return {
        "delivery_enabled": configuration["delivery_enabled"],
        "delivery_fingerprint": _safe_text(
            configuration["delivery_fingerprint"],
            "delivery_fingerprint",
        ),
        "endpoint_environment_variable": _safe_text(
            configuration["endpoint_environment_variable"],
            "endpoint_environment_variable",
        ),
        "retry_execution_enabled": configuration["retry_execution_enabled"],
        "retry_execution_policy_fingerprint": _safe_text(
            configuration["retry_execution_policy_fingerprint"],
            "retry_execution_policy_fingerprint",
        ),
        "retry_policy_fingerprint": _safe_text(
            configuration["retry_policy_fingerprint"],
            "retry_policy_fingerprint",
        ),
    }


def _canonical_destination(value: Any) -> dict[str, Any]:
    destination = _exact_mapping(
        value,
        "readiness destination",
        {
            "activation_status",
            "destination_id",
            "endpoint_environment_variable",
            "fingerprint",
        },
    )
    status = destination["activation_status"]
    if status not in {"active", "disabled", "not_yet_reviewed", "review_expired"}:
        raise ValidationError("destination activation status is unsupported")
    return {
        "activation_status": status,
        "destination_id": _safe_text(destination["destination_id"], "destination_id"),
        "endpoint_environment_variable": _safe_text(
            destination["endpoint_environment_variable"],
            "destination endpoint environment variable",
        ),
        "fingerprint": _safe_text(
            destination["fingerprint"],
            "destination fingerprint",
        ),
    }


def _canonical_ambiguity_summary(value: Any) -> dict[str, Any]:
    summary = _exact_mapping(
        value,
        "readiness ambiguity",
        {"count", "event_ids", "record_ids", "unbound_event_ids"},
    )
    count = summary["count"]
    if type(count) is not int or not 0 <= count <= MAX_AMBIGUITIES:
        raise ValidationError("ambiguity count is outside the reviewed maximum")
    parsed_lists: dict[str, list[str]] = {}
    for key in ("event_ids", "record_ids", "unbound_event_ids"):
        values = summary[key]
        if not isinstance(values, list):
            raise ValidationError(f"{key} must be an array")
        parsed = [_safe_text(item, f"{key} item") for item in values]
        if parsed != sorted(set(parsed)):
            raise ValidationError(f"{key} must be sorted with no duplicates")
        parsed_lists[key] = [item for item in parsed if item is not None]
    if count != len(parsed_lists["event_ids"]) or count != len(
        parsed_lists["record_ids"]
    ):
        raise ValidationError("ambiguity counts do not reconcile")
    if not set(parsed_lists["unbound_event_ids"]).issubset(
        parsed_lists["event_ids"]
    ):
        raise ValidationError("unbound ambiguity events are inconsistent")
    return {"count": count, **parsed_lists}


def validate_notification_execution_readiness_decision(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    exact = _exact_mapping(
        payload,
        "notification execution readiness decision",
        {
            "acknowledgement_mutated",
            "activation_review",
            "ambiguity",
            "blocking_reasons",
            "configuration",
            "decision",
            "decision_id",
            "delivery_attempt_written",
            "destination",
            "evaluated_at",
            "execution_kind",
            "external_request_performed",
            "model_version",
            "outbox_mutated",
            "read_only",
            "transition_review",
        },
    )
    if exact["model_version"] != MODEL_VERSION:
        raise ValidationError("notification readiness model_version is unsupported")
    execution_kind = exact["execution_kind"]
    if execution_kind not in EXECUTION_KINDS:
        raise ValidationError("execution_kind must be initial or retry")
    evaluated_at = _aware_utc(exact["evaluated_at"], "evaluated_at")
    configuration = _canonical_configuration(exact["configuration"])
    destination = _canonical_destination(exact["destination"])
    activation = _activation_review(exact["activation_review"])
    transition = _transition_review(exact["transition_review"])
    ambiguity = _canonical_ambiguity_summary(exact["ambiguity"])
    reasons = exact["blocking_reasons"]
    if not isinstance(reasons, list):
        raise ValidationError("blocking_reasons must be an array")
    if any(reason not in BLOCKING_REASON_ORDER for reason in reasons):
        raise ValidationError("blocking_reasons contains an unsupported reason")
    expected_order = [reason for reason in BLOCKING_REASON_ORDER if reason in reasons]
    if reasons != expected_order or len(reasons) != len(set(reasons)):
        raise ValidationError("blocking_reasons must use canonical order")
    expected_reasons = _derive_reasons(
        execution_kind=execution_kind,
        configuration=configuration,
        destination=destination,
        activation_review=activation,
        transition_review=transition,
        ambiguity=ambiguity,
    )
    if reasons != expected_reasons:
        raise ValidationError("blocking_reasons do not match retained evidence")
    decision = exact["decision"]
    expected_decision = "allow" if not expected_reasons else "block"
    if decision != expected_decision:
        raise ValidationError("decision does not match blocking reasons")
    if exact["read_only"] is not True:
        raise ValidationError("notification readiness evidence must be read-only")
    if any(exact[flag] is not False for flag in SIDE_EFFECT_FLAGS):
        raise ValidationError("notification readiness side-effect evidence is invalid")
    identity = _decision_identity(
        execution_kind=execution_kind,
        evaluated_at=evaluated_at.isoformat(),
        configuration=configuration,
        destination=destination,
        activation_review=activation,
        transition_review=transition,
        ambiguity=ambiguity,
        decision=decision,
        blocking_reasons=reasons,
    )
    if exact["decision_id"] != _decision_id(identity):
        raise ValidationError("decision_id does not match canonical evidence")
    return {
        "decision_id": exact["decision_id"],
        **identity,
        "read_only": True,
        "acknowledgement_mutated": False,
        "delivery_attempt_written": False,
        "external_request_performed": False,
        "outbox_mutated": False,
    }


def canonical_notification_execution_readiness_bytes(
    decision: Mapping[str, Any],
) -> bytes:
    validated = validate_notification_execution_readiness_decision(decision)
    try:
        return json.dumps(
            validated,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("notification readiness decision is not canonical JSON") from None


def read_notification_execution_readiness_evidence(
    *,
    dsn: str,
    destination_id: str,
    schema_name: str = "risk_platform",
) -> dict[str, Any]:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    selected_destination_id = _safe_text(destination_id, "destination_id")
    assert selected_destination_id is not None
    schema = _quote_identifier(schema_name)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness requires psycopg") from exc

    activation_query = f"""
        SELECT
            destination_id,
            destination_fingerprint,
            authority_id,
            checklist_id,
            review_status,
            operational_activation_ready
        FROM {schema}.current_notification_activation_rehearsal_review
        WHERE destination_id = %s
        LIMIT 2
    """
    transition_query = f"""
        SELECT
            destination_id,
            current_destination_fingerprint,
            current_authority_id,
            current_checklist_id,
            activation_review_status,
            operational_activation_ready,
            transition_record_id,
            transition_rehearsal_id,
            rollback_plan_id,
            rollback_authority_id,
            rollback_checklist_id,
            rollback_destination_fingerprint,
            rollback_endpoint_environment_variable,
            transition_matches_current_activation,
            transition_review_status,
            transition_ready
        FROM {schema}.current_notification_destination_transition_review
        WHERE destination_id = %s
        LIMIT 2
    """
    ambiguity_query = f"""
        SELECT
            event_id,
            uncertainty_record_id,
            latest_execution_record_id,
            destination_id,
            destination_fingerprint,
            endpoint_environment_variable,
            destination_bound,
            destination_binding_status
        FROM {schema}.current_notification_retry_destination_ambiguities
        WHERE destination_id = %s OR destination_id IS NULL
        ORDER BY event_id, uncertainty_record_id
        LIMIT %s
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(activation_query, (selected_destination_id,))
                activation_rows = [dict(row) for row in cursor.fetchall()]
                cursor.execute(transition_query, (selected_destination_id,))
                transition_rows = [dict(row) for row in cursor.fetchall()]
                cursor.execute(
                    ambiguity_query,
                    (selected_destination_id, MAX_AMBIGUITIES + 1),
                )
                ambiguity_rows = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError("notification readiness evidence could not be read") from None
    if len(activation_rows) > 1:
        raise StorageError("current activation review grain is not unique")
    if len(transition_rows) > 1:
        raise StorageError("current transition review grain is not unique")
    if len(ambiguity_rows) > MAX_AMBIGUITIES:
        raise StorageError("current ambiguity evidence exceeds the reviewed maximum")
    return {
        "activation_review": activation_rows[0] if activation_rows else None,
        "transition_review": transition_rows[0] if transition_rows else None,
        "ambiguities": ambiguity_rows,
    }


def run_notification_execution_readiness_gate(
    *,
    execution_kind: str,
    destination_id: str,
    evaluated_at: datetime | str,
    delivery_config_path: Path,
    destination_config_path: Path,
    dsn: str,
    schema_name: str = "risk_platform",
    evidence_reader: EvidenceReader | None = None,
) -> dict[str, Any]:
    delivery_config, retry_policy, retry_execution_policy = (
        load_retry_execution_contract(delivery_config_path)
    )
    destinations = load_notification_destinations(destination_config_path)
    destination = destinations.get(destination_id)
    if destination is None:
        raise ValidationError("notification destination does not exist")
    selected_reader = evidence_reader or read_notification_execution_readiness_evidence
    evidence = selected_reader(
        dsn=dsn,
        destination_id=destination_id,
        schema_name=schema_name,
    )
    exact = _exact_mapping(
        evidence,
        "notification readiness evidence",
        {"activation_review", "ambiguities", "transition_review"},
    )
    return evaluate_notification_execution_readiness(
        execution_kind=execution_kind,
        evaluated_at=evaluated_at,
        delivery_config=delivery_config,
        retry_policy_fingerprint=retry_policy.fingerprint,
        retry_execution_policy=retry_execution_policy,
        destination=destination,
        activation_review=exact["activation_review"],
        transition_review=exact["transition_review"],
        ambiguities=exact["ambiguities"],
    )


def _timestamp(value: str) -> datetime:
    try:
        return _aware_utc(value, "evaluated_at")
    except ValidationError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    if path.is_symlink():
        raise StorageError("notification readiness summary must not be a symbolic link")
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
        raise StorageError("unable to write notification readiness summary") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate one read-only notification execution readiness gate."
    )
    parser.add_argument(
        "--execution-kind",
        required=True,
        choices=sorted(EXECUTION_KINDS),
    )
    parser.add_argument("--destination-id", required=True)
    parser.add_argument("--evaluated-at", required=True, type=_timestamp)
    parser.add_argument(
        "--delivery-config",
        type=Path,
        default=Path("config/notification_delivery.yaml"),
    )
    parser.add_argument(
        "--destination-config",
        type=Path,
        default=Path("config/notification_destinations.yaml"),
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument("--schema", default="risk_platform")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        decision = run_notification_execution_readiness_gate(
            execution_kind=args.execution_kind,
            destination_id=args.destination_id,
            evaluated_at=args.evaluated_at,
            delivery_config_path=args.delivery_config,
            destination_config_path=args.destination_config,
            dsn=args.dsn,
            schema_name=args.schema,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, decision)
    except ValidationError as exc:
        print(f"Notification readiness rejected: {exc}", file=sys.stderr)
        return 1
    except StorageError:
        print(
            "Notification readiness failed: PostgreSQL or local evidence could not be read",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Notification readiness failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(decision, sort_keys=True, allow_nan=False))
    return 0 if decision["decision"] == "allow" else 2


if __name__ == "__main__":
    raise SystemExit(main())
