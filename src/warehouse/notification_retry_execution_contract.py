from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any, cast

from src.common.exceptions import ValidationError
from src.orchestration.portfolio_risk_notification_retry_execution_policy import (
    aware_utc,
    safe_segment,
    safe_text,
)

MODEL_VERSION = "portfolio-risk-notification-retry-execution-record-v1"
TERMINAL_STATUSES = frozenset(
    {
        "completed",
        "failed_before_request",
        "failed_after_request",
        "persistence_uncertain",
    }
)
MAX_EVENTS = 100
HEX_24_PATTERN = re.compile(r"^[0-9a-f]{24}$")
HEX_64_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FAILURE_CODE_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")

TOP_LEVEL_FIELDS = frozenset(
    {
        "record_id",
        "model_version",
        "request_id",
        "plan_id",
        "execution_id",
        "terminal_status",
        "started_at",
        "finished_at",
        "failure_stage",
        "failure_code",
        "configuration",
        "concurrency_control",
        "requested_event_ids",
        "persisted_event_ids",
        "persisted_attempt_ids",
        "execution",
    }
)
CONFIGURATION_FIELDS = frozenset(
    {
        "delivery_fingerprint",
        "retry_policy_fingerprint",
        "retry_execution_policy_fingerprint",
    }
)
CONCURRENCY_FIELDS = frozenset({"model_version", "key_fingerprint"})
EXECUTION_CONFIGURATION_FIELDS = CONFIGURATION_FIELDS
EXECUTION_CONCURRENCY_FIELDS = frozenset(
    {
        "performed",
        "acquired",
        "released",
        "held_through_revalidation",
        "held_through_attempt_persistence",
        "model_version",
        "scope",
        "key_fingerprint",
    }
)


def _exact_mapping(
    value: Any,
    expected: frozenset[str],
    label: str,
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError(f"{label} must be an object")
    fields = set(value)
    if fields != expected:
        missing = sorted(expected - fields)
        unknown = sorted(fields - expected)
        raise ValidationError(
            f"{label} fields are not exact; missing={missing}, unknown={unknown}"
        )
    return value


def _optional_safe_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    return safe_text(value, label)


def _safe_id_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or len(value) > MAX_EVENTS:
        raise ValidationError(f"{label} must be an array of at most {MAX_EVENTS} IDs")
    parsed = [safe_text(item, f"{label}[{index}]") for index, item in enumerate(value)]
    if len(parsed) != len(set(parsed)):
        raise ValidationError(f"{label} must not contain duplicates")
    return parsed


def canonical_record_bytes(record: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("retry execution record is not canonical JSON") from None


def _record_id(payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_record_bytes(payload)).hexdigest()[:24]
    return f"{MODEL_VERSION}-record-{digest}"


def _validate_execution_summary(
    value: Any,
    *,
    execution_id: str,
    request_id: str,
    plan_id: str,
    configuration: Mapping[str, str],
    lock_model_version: str,
    lock_key_fingerprint: str,
    requested_event_ids: list[str],
    persisted_event_ids: list[str],
    persisted_attempt_ids: list[str],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValidationError("completed execution must retain an execution object")
    summary = dict(value)
    if summary.get("execution_id") != execution_id:
        raise ValidationError("execution execution_id does not match the record")
    if summary.get("request_id") != request_id:
        raise ValidationError("execution request_id does not match the record")
    if summary.get("plan_id") != plan_id:
        raise ValidationError("execution plan_id does not match the record")

    summary_configuration = _exact_mapping(
        summary.get("configuration"),
        EXECUTION_CONFIGURATION_FIELDS,
        "execution configuration",
    )
    if dict(summary_configuration) != dict(configuration):
        raise ValidationError("execution configuration does not match the record")

    execution = summary.get("execution")
    if not isinstance(execution, Mapping):
        raise ValidationError("execution summary is missing execution evidence")
    if execution.get("requested") is not True or execution.get("performed") is not True:
        raise ValidationError("completed execution must have been explicitly performed")

    outcomes = summary.get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != len(requested_event_ids):
        raise ValidationError("completed execution outcomes do not match requested events")
    outcome_event_ids: list[str] = []
    outcome_attempt_ids: list[str] = []
    for index, outcome in enumerate(outcomes):
        if not isinstance(outcome, Mapping):
            raise ValidationError(f"execution outcomes[{index}] must be an object")
        outcome_event_ids.append(
            safe_text(outcome.get("event_id"), f"execution outcomes[{index}].event_id")
        )
        outcome_attempt_ids.append(
            safe_text(
                outcome.get("attempt_id"),
                f"execution outcomes[{index}].attempt_id",
            )
        )
        if outcome.get("outcome") not in {"succeeded", "failed"}:
            raise ValidationError("execution outcome must be succeeded or failed")
    if outcome_event_ids != requested_event_ids:
        raise ValidationError("execution event ordering does not match the record")
    if persisted_event_ids != requested_event_ids:
        raise ValidationError("completed execution must persist every requested event")
    if outcome_attempt_ids != persisted_attempt_ids:
        raise ValidationError("execution attempt IDs do not match persisted evidence")
    if execution.get("external_requests_performed") != len(outcomes):
        raise ValidationError("execution request count does not match outcomes")
    if execution.get("delivery_attempts_written") != len(outcomes):
        raise ValidationError("execution attempt count does not match outcomes")

    concurrency = _exact_mapping(
        summary.get("concurrency_control"),
        EXECUTION_CONCURRENCY_FIELDS,
        "execution concurrency_control",
    )
    for field in (
        "performed",
        "acquired",
        "released",
        "held_through_revalidation",
        "held_through_attempt_persistence",
    ):
        if concurrency.get(field) is not True:
            raise ValidationError(f"completed execution concurrency {field} must be true")
    if concurrency.get("model_version") != lock_model_version:
        raise ValidationError("execution lock model does not match the record")
    if concurrency.get("key_fingerprint") != lock_key_fingerprint:
        raise ValidationError("execution lock fingerprint does not match the record")

    endpoint = summary.get("endpoint")
    if not isinstance(endpoint, Mapping) or endpoint.get("full_url_recorded") is not False:
        raise ValidationError("completed execution must not retain the endpoint URL")
    safe_text(endpoint.get("host"), "execution endpoint host", maximum=253)

    if summary.get("response_bodies_recorded") is not False:
        raise ValidationError("response bodies must not be retained")
    for field in ("plan_mutated", "acknowledgement_mutated", "dead_letter_mutated"):
        if summary.get(field) is not False:
            raise ValidationError(f"completed execution {field} must be false")
    return summary


def validate_notification_retry_execution_record(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    record = _exact_mapping(value, TOP_LEVEL_FIELDS, "retry execution record")
    if record.get("model_version") != MODEL_VERSION:
        raise ValidationError("retry execution record model_version is unsupported")

    request_id = cast(str, safe_segment(record.get("request_id"), "request_id"))
    plan_id = safe_text(record.get("plan_id"), "plan_id")
    execution_id = _optional_safe_text(record.get("execution_id"), "execution_id")
    terminal_status = record.get("terminal_status")
    if terminal_status not in TERMINAL_STATUSES:
        raise ValidationError("retry execution terminal_status is unsupported")
    started_at = aware_utc(record.get("started_at"), "started_at")
    finished_at = aware_utc(record.get("finished_at"), "finished_at")
    if finished_at < started_at:
        raise ValidationError("finished_at must not precede started_at")
    failure_stage = _optional_safe_text(record.get("failure_stage"), "failure_stage")
    failure_code = _optional_safe_text(record.get("failure_code"), "failure_code")
    if failure_code is not None and not FAILURE_CODE_PATTERN.fullmatch(failure_code):
        raise ValidationError("failure_code is invalid")

    configuration_value = _exact_mapping(
        record.get("configuration"),
        CONFIGURATION_FIELDS,
        "configuration",
    )
    parsed_configuration = {
        field: safe_text(configuration_value.get(field), f"configuration.{field}")
        for field in sorted(CONFIGURATION_FIELDS)
    }
    concurrency = _exact_mapping(
        record.get("concurrency_control"),
        CONCURRENCY_FIELDS,
        "concurrency_control",
    )
    lock_model = safe_text(
        concurrency.get("model_version"),
        "concurrency_control.model_version",
    )
    lock_fingerprint = safe_text(
        concurrency.get("key_fingerprint"),
        "concurrency_control.key_fingerprint",
    )
    if not HEX_24_PATTERN.fullmatch(lock_fingerprint):
        raise ValidationError("concurrency_control key_fingerprint is invalid")

    requested_event_ids = _safe_id_list(
        record.get("requested_event_ids"),
        "requested_event_ids",
    )
    persisted_event_ids = _safe_id_list(
        record.get("persisted_event_ids"),
        "persisted_event_ids",
    )
    persisted_attempt_ids = _safe_id_list(
        record.get("persisted_attempt_ids"),
        "persisted_attempt_ids",
    )
    if len(persisted_event_ids) != len(persisted_attempt_ids):
        raise ValidationError("persisted event and attempt evidence must align")
    if persisted_event_ids != requested_event_ids[: len(persisted_event_ids)]:
        raise ValidationError("persisted events must be an ordered requested-event prefix")

    parsed_execution: dict[str, Any] | None = None
    if terminal_status == "completed":
        if execution_id is None:
            raise ValidationError("completed execution requires execution_id")
        if failure_stage is not None or failure_code is not None:
            raise ValidationError("completed execution must not retain failure evidence")
        if not requested_event_ids:
            raise ValidationError("completed execution requires requested events")
        parsed_execution = _validate_execution_summary(
            record.get("execution"),
            execution_id=execution_id,
            request_id=request_id,
            plan_id=plan_id,
            configuration=parsed_configuration,
            lock_model_version=lock_model,
            lock_key_fingerprint=lock_fingerprint,
            requested_event_ids=requested_event_ids,
            persisted_event_ids=persisted_event_ids,
            persisted_attempt_ids=persisted_attempt_ids,
        )
    else:
        if execution_id is not None or record.get("execution") is not None:
            raise ValidationError("failed execution must not retain a completed execution")
        if failure_stage is None or failure_code is None:
            raise ValidationError("failed execution requires bounded failure evidence")
        if terminal_status == "failed_before_request":
            if requested_event_ids or persisted_event_ids:
                raise ValidationError(
                    "failed_before_request must not retain request or attempt evidence"
                )
        elif terminal_status == "failed_after_request":
            if not requested_event_ids or persisted_event_ids != requested_event_ids:
                raise ValidationError(
                    "failed_after_request requires persisted evidence for every request"
                )
        elif terminal_status == "persistence_uncertain":
            if not requested_event_ids or len(persisted_event_ids) >= len(
                requested_event_ids
            ):
                raise ValidationError(
                    "persistence_uncertain requires an unpersisted requested event"
                )

    canonical_without_id = {
        "model_version": MODEL_VERSION,
        "request_id": request_id,
        "plan_id": plan_id,
        "execution_id": execution_id,
        "terminal_status": terminal_status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "failure_stage": failure_stage,
        "failure_code": failure_code,
        "configuration": parsed_configuration,
        "concurrency_control": {
            "model_version": lock_model,
            "key_fingerprint": lock_fingerprint,
        },
        "requested_event_ids": requested_event_ids,
        "persisted_event_ids": persisted_event_ids,
        "persisted_attempt_ids": persisted_attempt_ids,
        "execution": parsed_execution,
    }
    expected_id = _record_id(canonical_without_id)
    if record.get("record_id") != expected_id:
        raise ValidationError("retry execution record_id does not match its evidence")
    validated = {"record_id": expected_id, **canonical_without_id}
    rendered = canonical_record_bytes(validated).decode("utf-8").casefold()
    for forbidden in (
        "postgresql://",
        '"dsn":',
        '"password":',
        '"secret":',
        '"token":',
        '"authorization":',
    ):
        if forbidden in rendered:
            raise ValidationError("retry execution record contains forbidden secret material")
    return validated


def build_notification_retry_execution_record(
    *,
    request_id: str,
    plan_id: str,
    terminal_status: str,
    started_at: datetime | str,
    finished_at: datetime | str,
    delivery_fingerprint: str,
    retry_policy_fingerprint: str,
    retry_execution_policy_fingerprint: str,
    delivery_lock_model_version: str,
    delivery_lock_key_fingerprint: str,
    requested_event_ids: Sequence[str] = (),
    persisted_event_ids: Sequence[str] = (),
    persisted_attempt_ids: Sequence[str] = (),
    execution_id: str | None = None,
    failure_stage: str | None = None,
    failure_code: str | None = None,
    execution: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model_version": MODEL_VERSION,
        "request_id": request_id,
        "plan_id": plan_id,
        "execution_id": execution_id,
        "terminal_status": terminal_status,
        "started_at": aware_utc(started_at, "started_at").isoformat(),
        "finished_at": aware_utc(finished_at, "finished_at").isoformat(),
        "failure_stage": failure_stage,
        "failure_code": failure_code,
        "configuration": {
            "delivery_fingerprint": delivery_fingerprint,
            "retry_policy_fingerprint": retry_policy_fingerprint,
            "retry_execution_policy_fingerprint": (
                retry_execution_policy_fingerprint
            ),
        },
        "concurrency_control": {
            "model_version": delivery_lock_model_version,
            "key_fingerprint": delivery_lock_key_fingerprint,
        },
        "requested_event_ids": list(requested_event_ids),
        "persisted_event_ids": list(persisted_event_ids),
        "persisted_attempt_ids": list(persisted_attempt_ids),
        "execution": dict(execution) if execution is not None else None,
    }
    payload["record_id"] = _record_id(payload)
    return validate_notification_retry_execution_record(payload)


def notification_retry_execution_document_sha256(
    record: Mapping[str, Any],
) -> str:
    validated = validate_notification_retry_execution_record(record)
    digest = hashlib.sha256(canonical_record_bytes(validated)).hexdigest()
    if not HEX_64_PATTERN.fullmatch(digest):  # pragma: no cover
        raise AssertionError("SHA-256 implementation returned an invalid digest")
    return digest
