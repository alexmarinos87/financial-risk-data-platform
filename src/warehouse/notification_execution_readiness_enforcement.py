from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.notification_execution_readiness_gate import (
    run_notification_execution_readiness_gate,
    validate_notification_execution_readiness_decision,
)
from src.warehouse.notification_execution_readiness_history_contract import (
    validate_notification_execution_readiness_record,
)

MODEL_VERSION = "portfolio-risk-notification-execution-readiness-enforcement-v1"
EXECUTION_KINDS = frozenset({"initial", "retry"})
REVIEW_STATUSES = frozenset(
    {
        "allowed",
        "blocked",
        "decision_missing",
        "decision_stale",
        "decision_superseded",
    }
)
MAX_READINESS_AGE = timedelta(minutes=5)
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")

ReviewReader = Callable[..., Mapping[str, Any] | None]
GateRunner = Callable[..., Mapping[str, Any]]


def _safe_text(value: Any, label: str) -> str:
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


def _lock_identity(value: Any) -> dict[str, str]:
    lock = _exact_mapping(
        value,
        "notification delivery lock evidence",
        {"acquired", "key_fingerprint", "model_version", "scope"},
    )
    if lock["acquired"] is not True:
        raise ValidationError("notification execution readiness requires an acquired lock")
    return {
        "key_fingerprint": _safe_text(
            lock["key_fingerprint"],
            "delivery lock key_fingerprint",
        ),
        "model_version": _safe_text(
            lock["model_version"],
            "delivery lock model_version",
        ),
        "scope": _safe_text(lock["scope"], "delivery lock scope"),
    }


def read_current_notification_execution_readiness_review(
    *,
    dsn: str,
    destination_id: str,
    execution_kind: str,
    schema_name: str = "risk_platform",
) -> Mapping[str, Any] | None:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    selected_destination_id = _safe_text(destination_id, "destination_id")
    if execution_kind not in EXECUTION_KINDS:
        raise ValidationError("execution_kind must be initial or retry")
    schema = _quote_identifier(schema_name)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:  # pragma: no cover - required in CI.
        raise RuntimeError("Notification readiness enforcement requires psycopg") from exc

    statement = f"""
        SELECT
            review.destination_id,
            review.execution_kind,
            review.readiness_record_id,
            review.readiness_request_id,
            review.decision_id,
            review.decision_evaluated_at,
            review.decision_recorded_at,
            review.decision,
            review.blocking_reasons_json,
            review.readiness_review_status,
            review.execution_ready,
            retained.record_json
        FROM {schema}.current_notification_execution_readiness_review review
        LEFT JOIN {schema}.notification_execution_readiness_decisions retained
          ON retained.record_id = review.readiness_record_id
        WHERE review.destination_id = %s
          AND review.execution_kind = %s
        LIMIT 2
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(statement, (selected_destination_id, execution_kind))
                rows = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "current notification execution readiness review could not be read"
        ) from None
    if len(rows) > 1:
        raise StorageError("current notification execution readiness grain is not unique")
    return rows[0] if rows else None


def _allowed_record(
    value: Mapping[str, Any] | None,
    *,
    destination_id: str,
    execution_kind: str,
) -> dict[str, Any]:
    if value is None:
        raise ValidationError("notification execution readiness decision is missing")
    review = _exact_mapping(
        value,
        "current notification execution readiness review",
        {
            "blocking_reasons_json",
            "decision",
            "decision_evaluated_at",
            "decision_id",
            "decision_recorded_at",
            "destination_id",
            "execution_kind",
            "execution_ready",
            "readiness_record_id",
            "readiness_request_id",
            "readiness_review_status",
            "record_json",
        },
    )
    if review["destination_id"] != destination_id:
        raise ValidationError("readiness review belongs to another destination")
    if review["execution_kind"] != execution_kind:
        raise ValidationError("readiness review belongs to another execution kind")
    status = review["readiness_review_status"]
    if status not in REVIEW_STATUSES:
        raise ValidationError("readiness review status is unsupported")
    if type(review["execution_ready"]) is not bool:
        raise ValidationError("readiness execution_ready must be boolean")
    if status != "allowed" or review["execution_ready"] is not True:
        raise ValidationError(
            f"notification execution readiness is not allowed: {status}"
        )
    if review["decision"] != "allow":
        raise ValidationError("allowed readiness review must retain an allow decision")
    if review["blocking_reasons_json"] != []:
        raise ValidationError("allowed readiness review must have no blocking reasons")
    record_json = review["record_json"]
    if not isinstance(record_json, Mapping):
        raise StorageError("allowed readiness review has no retained canonical record")
    record = validate_notification_execution_readiness_record(record_json)
    decision = record["decision"]

    row_identity = {
        "record_id": _safe_text(review["readiness_record_id"], "readiness_record_id"),
        "request_id": _safe_text(
            review["readiness_request_id"],
            "readiness_request_id",
        ),
        "decision_id": _safe_text(review["decision_id"], "decision_id"),
        "decision_evaluated_at": _aware_utc(
            review["decision_evaluated_at"],
            "decision_evaluated_at",
        ).isoformat(),
        "decision_recorded_at": _aware_utc(
            review["decision_recorded_at"],
            "decision_recorded_at",
        ).isoformat(),
    }
    expected_identity = {
        "record_id": record["record_id"],
        "request_id": record["request_id"],
        "decision_id": decision["decision_id"],
        "decision_evaluated_at": decision["evaluated_at"],
        "decision_recorded_at": record["recorded_at"],
    }
    if row_identity != expected_identity:
        raise ValidationError("readiness serving row does not match its retained record")
    if decision["destination"]["destination_id"] != destination_id:
        raise ValidationError("retained readiness record belongs to another destination")
    if decision["execution_kind"] != execution_kind:
        raise ValidationError("retained readiness record belongs to another execution kind")
    if decision["decision"] != "allow" or decision["blocking_reasons"]:
        raise ValidationError("retained readiness record does not grant execution")
    return record


def _substantive_decision(value: Mapping[str, Any]) -> dict[str, Any]:
    decision = validate_notification_execution_readiness_decision(value)
    return {
        key: decision[key]
        for key in decision
        if key not in {"decision_id", "evaluated_at"}
    }


def _enforcement_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-enforcement-{digest}"


def _enforcement_evidence(
    *,
    destination_id: str,
    execution_kind: str,
    enforced_at: datetime,
    record: Mapping[str, Any],
    refreshed_decision: Mapping[str, Any],
    lock: Mapping[str, str],
) -> dict[str, Any]:
    retained_decision = record["decision"]
    identity = {
        "destination_id": destination_id,
        "enforced_at": enforced_at.isoformat(),
        "execution_kind": execution_kind,
        "lock": dict(lock),
        "model_version": MODEL_VERSION,
        "readiness_record_id": record["record_id"],
        "readiness_request_id": record["request_id"],
        "refreshed_decision_id": refreshed_decision["decision_id"],
        "retained_decision_id": retained_decision["decision_id"],
    }
    return {
        "enforcement_id": _enforcement_id(identity),
        **identity,
        "execution_ready": True,
        "readiness_review_status": "allowed",
        "refreshed_decision_evaluated_at": refreshed_decision["evaluated_at"],
        "retained_decision_evaluated_at": retained_decision["evaluated_at"],
        "substantive_evidence_match": True,
    }


def validate_notification_execution_readiness_enforcement(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    evidence = _exact_mapping(
        value,
        "notification execution readiness enforcement",
        {
            "destination_id",
            "enforced_at",
            "enforcement_id",
            "execution_kind",
            "execution_ready",
            "lock",
            "model_version",
            "readiness_record_id",
            "readiness_request_id",
            "readiness_review_status",
            "refreshed_decision_evaluated_at",
            "refreshed_decision_id",
            "retained_decision_evaluated_at",
            "retained_decision_id",
            "substantive_evidence_match",
        },
    )
    if evidence["model_version"] != MODEL_VERSION:
        raise ValidationError("readiness enforcement model_version is unsupported")
    execution_kind = evidence["execution_kind"]
    if execution_kind not in EXECUTION_KINDS:
        raise ValidationError("execution_kind must be initial or retry")
    if evidence["execution_ready"] is not True:
        raise ValidationError("readiness enforcement must grant execution")
    if evidence["readiness_review_status"] != "allowed":
        raise ValidationError("readiness enforcement must retain allowed status")
    if evidence["substantive_evidence_match"] is not True:
        raise ValidationError("readiness enforcement must match substantive evidence")
    enforced_at = _aware_utc(evidence["enforced_at"], "enforced_at")
    refreshed_at = _aware_utc(
        evidence["refreshed_decision_evaluated_at"],
        "refreshed_decision_evaluated_at",
    )
    retained_at = _aware_utc(
        evidence["retained_decision_evaluated_at"],
        "retained_decision_evaluated_at",
    )
    if refreshed_at != enforced_at:
        raise ValidationError("refreshed decision time must equal enforcement time")
    if enforced_at < retained_at or enforced_at - retained_at > MAX_READINESS_AGE:
        raise ValidationError("retained readiness decision is outside the enforcement age")
    stored_lock = evidence["lock"]
    if not isinstance(stored_lock, Mapping):
        raise ValidationError("readiness enforcement lock must be a mapping")
    lock = _lock_identity({**dict(stored_lock), "acquired": True})
    identity = {
        "destination_id": _safe_text(evidence["destination_id"], "destination_id"),
        "enforced_at": enforced_at.isoformat(),
        "execution_kind": execution_kind,
        "lock": lock,
        "model_version": MODEL_VERSION,
        "readiness_record_id": _safe_text(
            evidence["readiness_record_id"],
            "readiness_record_id",
        ),
        "readiness_request_id": _safe_text(
            evidence["readiness_request_id"],
            "readiness_request_id",
        ),
        "refreshed_decision_id": _safe_text(
            evidence["refreshed_decision_id"],
            "refreshed_decision_id",
        ),
        "retained_decision_id": _safe_text(
            evidence["retained_decision_id"],
            "retained_decision_id",
        ),
    }
    if evidence["enforcement_id"] != _enforcement_id(identity):
        raise ValidationError("readiness enforcement_id does not match canonical evidence")
    return {
        "enforcement_id": evidence["enforcement_id"],
        **identity,
        "execution_ready": True,
        "readiness_review_status": "allowed",
        "refreshed_decision_evaluated_at": refreshed_at.isoformat(),
        "retained_decision_evaluated_at": retained_at.isoformat(),
        "substantive_evidence_match": True,
    }


def enforce_notification_execution_readiness(
    *,
    dsn: str,
    destination_id: str,
    execution_kind: str,
    evaluated_at: datetime | str,
    delivery_config_path: Path,
    destination_config_path: Path,
    lock_evidence: Mapping[str, Any],
    schema_name: str = "risk_platform",
    review_reader: ReviewReader | None = None,
    gate_runner: GateRunner | None = None,
) -> dict[str, Any]:
    selected_destination_id = _safe_text(destination_id, "destination_id")
    if execution_kind not in EXECUTION_KINDS:
        raise ValidationError("execution_kind must be initial or retry")
    as_of = _aware_utc(evaluated_at, "evaluated_at")
    lock = _lock_identity(lock_evidence)
    selected_reader = review_reader or read_current_notification_execution_readiness_review
    review = selected_reader(
        dsn=dsn,
        destination_id=selected_destination_id,
        execution_kind=execution_kind,
        schema_name=schema_name,
    )
    record = _allowed_record(
        review,
        destination_id=selected_destination_id,
        execution_kind=execution_kind,
    )
    retained_decision = record["decision"]
    retained_at = _aware_utc(retained_decision["evaluated_at"], "decision.evaluated_at")
    if as_of < retained_at:
        raise ValidationError("execution time precedes retained readiness decision")
    if as_of - retained_at > MAX_READINESS_AGE:
        raise ValidationError("retained readiness decision is stale")

    selected_gate_runner = gate_runner or run_notification_execution_readiness_gate
    refreshed = validate_notification_execution_readiness_decision(
        selected_gate_runner(
            execution_kind=execution_kind,
            destination_id=selected_destination_id,
            evaluated_at=as_of,
            delivery_config_path=delivery_config_path,
            destination_config_path=destination_config_path,
            dsn=dsn,
            schema_name=schema_name,
        )
    )
    if refreshed["decision"] != "allow":
        reasons = ",".join(refreshed["blocking_reasons"])
        raise ValidationError(
            f"refreshed notification execution readiness is blocked: {reasons}"
        )
    if _substantive_decision(retained_decision) != _substantive_decision(refreshed):
        raise ValidationError(
            "retained notification execution readiness was superseded during preflight"
        )
    evidence = _enforcement_evidence(
        destination_id=selected_destination_id,
        execution_kind=execution_kind,
        enforced_at=as_of,
        record=record,
        refreshed_decision=refreshed,
        lock=lock,
    )
    return validate_notification_execution_readiness_enforcement(evidence)
