from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from src.common.exceptions import ValidationError

MODEL_VERSION = "portfolio-risk-notification-retry-execution-record-v1"
EXECUTION_MODEL_VERSION = "portfolio-risk-manual-retry-execution-v1"
CHANNEL = "webhook"
MAX_EVENTS = 100
TERMINAL_STATUSES = frozenset(
    {
        "completed",
        "failed_before_request",
        "failed_after_request",
        "persistence_uncertain",
    }
)
FAILURE_CODES = frozenset(
    {
        "overlap_error",
        "storage_error",
        "unexpected_error",
        "validation_error",
    }
)
SAFE_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,511}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


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


def _bounded_count(value: Any, label: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_EVENTS:
        raise ValidationError(
            f"{label} must be an integer between 0 and {MAX_EVENTS}"
        )
    return value


def _optional_boolean(value: Any, label: str) -> bool | None:
    if value is None:
        return None
    if type(value) is not bool:
        raise ValidationError(f"{label} must be boolean or null")
    return value


def _safe_id_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be an array")
    parsed = [_safe_text(item, f"{label} item") for item in value]
    result = [item for item in parsed if item is not None]
    if len(result) != len(set(result)):
        raise ValidationError(f"{label} must contain no duplicates")
    return result


def canonical_retry_execution_record_bytes(
    record: Mapping[str, Any],
) -> bytes:
    try:
        return json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("retry execution record is not canonical JSON") from None


def _record_id(identity: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(canonical_retry_execution_record_bytes(identity)).hexdigest()
    return f"{MODEL_VERSION}-record-{digest[:24]}"


def _validate_outcome(
    value: Any,
    *,
    index: int,
    executed_at: datetime,
) -> tuple[str, str, str]:
    outcome = _exact_mapping(
        value,
        f"execution summary outcome {index}",
        {
            "attempt_id",
            "attempt_number",
            "attempted_at",
            "error_code",
            "event_id",
            "http_status",
            "outcome",
            "payload_sha256",
        },
    )
    attempt_id = _safe_text(outcome["attempt_id"], "outcome.attempt_id")
    event_id = _safe_text(outcome["event_id"], "outcome.event_id")
    assert attempt_id is not None and event_id is not None
    attempt_number = outcome["attempt_number"]
    if type(attempt_number) is not int or not 1 <= attempt_number <= 10:
        raise ValidationError("outcome.attempt_number is invalid")
    attempted_at = _aware_utc(outcome["attempted_at"], "outcome.attempted_at")
    if attempted_at < executed_at:
        raise ValidationError("outcome.attempted_at precedes execution start")
    http_status = outcome["http_status"]
    if http_status is not None and (
        type(http_status) is not int or not 100 <= http_status <= 599
    ):
        raise ValidationError("outcome.http_status is invalid")
    error_code = _safe_text(
        outcome["error_code"],
        "outcome.error_code",
        optional=True,
    )
    outcome_name = outcome["outcome"]
    if outcome_name == "succeeded":
        if http_status is None or not 200 <= http_status <= 299 or error_code is not None:
            raise ValidationError("succeeded outcome evidence is inconsistent")
    elif outcome_name == "failed":
        if error_code is None or (
            http_status is not None and 200 <= http_status <= 299
        ):
            raise ValidationError("failed outcome evidence is inconsistent")
    else:
        raise ValidationError("outcome.outcome is invalid")
    payload_sha256 = outcome["payload_sha256"]
    if not isinstance(payload_sha256, str) or not SHA256_PATTERN.fullmatch(
        payload_sha256
    ):
        raise ValidationError("outcome.payload_sha256 is invalid")
    return attempt_id, event_id, outcome_name


def _execution_summary_fields(summary: Mapping[str, Any]) -> dict[str, Any]:
    exact = _exact_mapping(
        summary,
        "execution summary",
        {
            "execution_id",
            "model_version",
            "request_id",
            "plan_id",
            "executed_at",
            "channel",
            "endpoint",
            "configuration",
            "revalidation",
            "selection",
            "outcomes",
            "outcome_counts",
            "execution",
            "concurrency_control",
            "response_bodies_recorded",
            "plan_mutated",
            "acknowledgement_mutated",
            "dead_letter_mutated",
        },
    )
    if exact["model_version"] != EXECUTION_MODEL_VERSION:
        raise ValidationError("execution summary model_version is unsupported")
    if exact["channel"] != CHANNEL:
        raise ValidationError("execution summary channel is unsupported")
    if any(
        exact[key] is not False
        for key in (
            "response_bodies_recorded",
            "plan_mutated",
            "acknowledgement_mutated",
            "dead_letter_mutated",
        )
    ):
        raise ValidationError("execution summary side-effect declarations are invalid")

    executed_at = _aware_utc(exact["executed_at"], "executed_at")
    execution = _exact_mapping(
        exact["execution"],
        "execution summary execution",
        {
            "requested",
            "performed",
            "external_requests_performed",
            "delivery_attempts_written",
        },
    )
    counts = _exact_mapping(
        exact["outcome_counts"],
        "execution summary outcome_counts",
        {"succeeded", "failed"},
    )
    concurrency = _exact_mapping(
        exact["concurrency_control"],
        "execution summary concurrency_control",
        {
            "performed",
            "acquired",
            "released",
            "held_through_revalidation",
            "held_through_attempt_persistence",
            "model_version",
            "scope",
            "key_fingerprint",
        },
    )
    configuration = _exact_mapping(
        exact["configuration"],
        "execution summary configuration",
        {
            "delivery_fingerprint",
            "retry_execution_policy_fingerprint",
            "retry_policy_fingerprint",
        },
    )
    endpoint = _exact_mapping(
        exact["endpoint"],
        "execution summary endpoint",
        {"host", "full_url_recorded"},
    )
    revalidation = _exact_mapping(
        exact["revalidation"],
        "execution summary revalidation",
        {
            "performed",
            "current_plan_id",
            "events_checked",
            "exact_event_evidence_unchanged",
        },
    )
    selection = _exact_mapping(
        exact["selection"],
        "execution summary selection",
        {"planned_retryable_events", "executed_events", "max_events"},
    )
    outcomes = exact["outcomes"]
    if not isinstance(outcomes, list):
        raise ValidationError("execution summary outcomes must be an array")

    attempt_ids: list[str] = []
    event_ids: list[str] = []
    outcome_names: list[str] = []
    for index, outcome in enumerate(outcomes):
        attempt_id, event_id, outcome_name = _validate_outcome(
            outcome,
            index=index,
            executed_at=executed_at,
        )
        attempt_ids.append(attempt_id)
        event_ids.append(event_id)
        outcome_names.append(outcome_name)
    if len(attempt_ids) != len(set(attempt_ids)):
        raise ValidationError("execution summary attempt IDs must be unique")
    if len(event_ids) != len(set(event_ids)):
        raise ValidationError("execution summary event IDs must be unique")

    external_requests = _bounded_count(
        execution["external_requests_performed"],
        "external_requests_performed",
    )
    attempts_written = _bounded_count(
        execution["delivery_attempts_written"],
        "delivery_attempts_written",
    )
    succeeded = _bounded_count(counts["succeeded"], "succeeded")
    failed = _bounded_count(counts["failed"], "failed")
    planned = _bounded_count(
        selection["planned_retryable_events"],
        "planned_retryable_events",
    )
    executed = _bounded_count(selection["executed_events"], "executed_events")
    max_events = _bounded_count(selection["max_events"], "max_events")
    events_checked = _bounded_count(revalidation["events_checked"], "events_checked")
    if execution["requested"] is not True or execution["performed"] is not True:
        raise ValidationError("completed execution summary must be performed")
    if external_requests != len(outcomes) or attempts_written != len(outcomes):
        raise ValidationError("execution summary request and attempt counts disagree")
    if succeeded != outcome_names.count("succeeded"):
        raise ValidationError("execution summary succeeded count disagrees")
    if failed != outcome_names.count("failed"):
        raise ValidationError("execution summary failed count disagrees")
    if planned != executed or executed != len(outcomes) or max_events < executed:
        raise ValidationError("execution summary selection counts disagree")
    if events_checked < executed:
        raise ValidationError("execution summary revalidation count is too small")
    if revalidation["performed"] is not True:
        raise ValidationError("execution summary must retain revalidation evidence")
    if revalidation["exact_event_evidence_unchanged"] is not True:
        raise ValidationError("execution summary event evidence was not unchanged")
    if revalidation["current_plan_id"] != exact["plan_id"]:
        raise ValidationError("execution summary current plan identity disagrees")
    if any(
        concurrency[key] is not True
        for key in (
            "performed",
            "acquired",
            "released",
            "held_through_revalidation",
            "held_through_attempt_persistence",
        )
    ):
        raise ValidationError("completed execution must retain released lock evidence")
    if endpoint["full_url_recorded"] is not False:
        raise ValidationError("execution summary must not retain the full endpoint URL")

    return {
        "execution_id": _safe_text(exact["execution_id"], "execution_id"),
        "request_id": _safe_text(exact["request_id"], "request_id"),
        "plan_id": _safe_text(exact["plan_id"], "plan_id"),
        "executed_at": executed_at,
        "endpoint_host": _safe_text(endpoint["host"], "endpoint.host"),
        "delivery_fingerprint": _safe_text(
            configuration["delivery_fingerprint"],
            "delivery_fingerprint",
        ),
        "retry_policy_fingerprint": _safe_text(
            configuration["retry_policy_fingerprint"],
            "retry_policy_fingerprint",
        ),
        "retry_execution_policy_fingerprint": _safe_text(
            configuration["retry_execution_policy_fingerprint"],
            "retry_execution_policy_fingerprint",
        ),
        "lock_model_version": _safe_text(
            concurrency["model_version"],
            "lock.model_version",
        ),
        "lock_key_fingerprint": _safe_text(
            concurrency["key_fingerprint"],
            "lock.key_fingerprint",
        ),
        "lock_acquired": True,
        "lock_released": True,
        "request_count": external_requests,
        "attempts_persisted": attempts_written,
        "succeeded_count": succeeded,
        "failed_count": failed,
        "attempt_ids": attempt_ids,
        "event_ids": event_ids,
    }


def build_retry_execution_record(
    *,
    request_id: str,
    plan_id: str,
    started_at: datetime | str,
    finished_at: datetime | str,
    recorded_at: datetime | str,
    terminal_status: str,
    failure_code: str | None,
    request_count: int,
    attempts_persisted: int,
    succeeded_count: int,
    failed_count: int,
    attempt_ids: Sequence[str],
    requested_event_ids: Sequence[str],
    persisted_event_ids: Sequence[str],
    execution_summary: Mapping[str, Any] | None,
    endpoint_host: str | None = None,
    delivery_fingerprint: str | None = None,
    retry_policy_fingerprint: str | None = None,
    retry_execution_policy_fingerprint: str | None = None,
    lock_model_version: str | None = None,
    lock_key_fingerprint: str | None = None,
    lock_acquired: bool | None = None,
    lock_released: bool | None = None,
) -> dict[str, Any]:
    selected_request_id = _safe_text(request_id, "request_id")
    selected_plan_id = _safe_text(plan_id, "plan_id")
    assert selected_request_id is not None and selected_plan_id is not None
    started = _aware_utc(started_at, "started_at")
    finished = _aware_utc(finished_at, "finished_at")
    recorded = _aware_utc(recorded_at, "recorded_at")
    if finished < started:
        raise ValidationError("finished_at must not precede started_at")
    if recorded < finished:
        raise ValidationError("recorded_at must not precede finished_at")
    if terminal_status not in TERMINAL_STATUSES:
        raise ValidationError("terminal_status is invalid")
    if failure_code is not None and failure_code not in FAILURE_CODES:
        raise ValidationError("failure_code is invalid")

    requests = _bounded_count(request_count, "request_count")
    persisted = _bounded_count(attempts_persisted, "attempts_persisted")
    succeeded = _bounded_count(succeeded_count, "succeeded_count")
    failed = _bounded_count(failed_count, "failed_count")
    selected_attempt_ids = _safe_id_list(list(attempt_ids), "attempt_ids")
    selected_requested_events = _safe_id_list(
        list(requested_event_ids),
        "requested_event_ids",
    )
    selected_persisted_events = _safe_id_list(
        list(persisted_event_ids),
        "persisted_event_ids",
    )
    if requests != len(selected_requested_events):
        raise ValidationError("request_count must match requested_event_ids")
    if persisted != len(selected_attempt_ids):
        raise ValidationError("attempts_persisted must match attempt_ids")
    if persisted != len(selected_persisted_events):
        raise ValidationError("attempts_persisted must match persisted_event_ids")
    if persisted > requests:
        raise ValidationError("attempts_persisted must not exceed request_count")
    if selected_persisted_events != selected_requested_events[:persisted]:
        raise ValidationError(
            "persisted_event_ids must be the persisted prefix of requested_event_ids"
        )
    if succeeded + failed != persisted:
        raise ValidationError("persisted attempt outcome counts disagree")

    selected_endpoint_host = _safe_text(
        endpoint_host,
        "endpoint_host",
        optional=True,
    )
    selected_delivery_fingerprint = _safe_text(
        delivery_fingerprint,
        "delivery_fingerprint",
        optional=True,
    )
    selected_retry_policy_fingerprint = _safe_text(
        retry_policy_fingerprint,
        "retry_policy_fingerprint",
        optional=True,
    )
    selected_execution_policy_fingerprint = _safe_text(
        retry_execution_policy_fingerprint,
        "retry_execution_policy_fingerprint",
        optional=True,
    )
    selected_lock_model = _safe_text(
        lock_model_version,
        "lock_model_version",
        optional=True,
    )
    selected_lock_key = _safe_text(
        lock_key_fingerprint,
        "lock_key_fingerprint",
        optional=True,
    )
    selected_lock_acquired = _optional_boolean(lock_acquired, "lock_acquired")
    selected_lock_released = _optional_boolean(lock_released, "lock_released")
    if (selected_lock_model is None) != (selected_lock_key is None):
        raise ValidationError("lock identity must be complete or absent")
    if selected_lock_released is True and selected_lock_acquired is not True:
        raise ValidationError("released lock evidence requires acquired lock evidence")

    execution_id: str | None = None
    canonical_summary: dict[str, Any] | None = None
    if terminal_status == "completed":
        if failure_code is not None or execution_summary is None:
            raise ValidationError("completed execution requires a summary and no failure")
        summary = dict(execution_summary)
        fields = _execution_summary_fields(summary)
        if fields["request_id"] != selected_request_id:
            raise ValidationError("execution summary request_id does not match")
        if fields["plan_id"] != selected_plan_id:
            raise ValidationError("execution summary plan_id does not match")
        if fields["executed_at"] != started:
            raise ValidationError("execution summary executed_at does not match started_at")
        if fields["request_count"] != requests:
            raise ValidationError("execution summary request count does not match")
        if fields["attempts_persisted"] != persisted:
            raise ValidationError("execution summary attempt count does not match")
        if fields["succeeded_count"] != succeeded or fields["failed_count"] != failed:
            raise ValidationError("execution summary outcome counts do not match")
        if fields["attempt_ids"] != selected_attempt_ids:
            raise ValidationError("execution summary attempt IDs do not match")
        if fields["event_ids"] != selected_requested_events:
            raise ValidationError("execution summary requested event IDs do not match")
        if selected_requested_events != selected_persisted_events:
            raise ValidationError("completed execution must persist every request")
        execution_id = fields["execution_id"]
        selected_endpoint_host = fields["endpoint_host"]
        selected_delivery_fingerprint = fields["delivery_fingerprint"]
        selected_retry_policy_fingerprint = fields["retry_policy_fingerprint"]
        selected_execution_policy_fingerprint = fields[
            "retry_execution_policy_fingerprint"
        ]
        selected_lock_model = fields["lock_model_version"]
        selected_lock_key = fields["lock_key_fingerprint"]
        selected_lock_acquired = True
        selected_lock_released = True
        canonical_summary = summary
    else:
        if failure_code is None or execution_summary is not None:
            raise ValidationError("failed execution requires a bounded failure only")
        if terminal_status == "failed_before_request" and (
            requests != 0 or persisted != 0
        ):
            raise ValidationError("failed_before_request must have zero side effects")
        if terminal_status == "failed_after_request" and not (
            requests > 0 and requests == persisted
        ):
            raise ValidationError(
                "failed_after_request requires every request to have attempt evidence"
            )
        if terminal_status == "persistence_uncertain" and not requests > persisted:
            raise ValidationError(
                "persistence_uncertain requires a request without attempt evidence"
            )

    identity = {
        "attempt_ids": selected_attempt_ids,
        "attempts_persisted": persisted,
        "channel": CHANNEL,
        "delivery_fingerprint": selected_delivery_fingerprint,
        "endpoint_host": selected_endpoint_host,
        "execution_id": execution_id,
        "execution_summary": canonical_summary,
        "failed_count": failed,
        "failure_code": failure_code,
        "finished_at": finished.isoformat(),
        "lock_acquired": selected_lock_acquired,
        "lock_key_fingerprint": selected_lock_key,
        "lock_model_version": selected_lock_model,
        "lock_released": selected_lock_released,
        "model_version": MODEL_VERSION,
        "persisted_event_ids": selected_persisted_events,
        "plan_id": selected_plan_id,
        "recorded_at": recorded.isoformat(),
        "request_count": requests,
        "request_id": selected_request_id,
        "requested_event_ids": selected_requested_events,
        "retry_execution_policy_fingerprint": selected_execution_policy_fingerprint,
        "retry_policy_fingerprint": selected_retry_policy_fingerprint,
        "started_at": started.isoformat(),
        "succeeded_count": succeeded,
        "terminal_status": terminal_status,
    }
    return {"record_id": _record_id(identity), **identity}


def validate_retry_execution_record(record: Mapping[str, Any]) -> dict[str, Any]:
    exact = _exact_mapping(
        record,
        "retry execution record",
        {
            "record_id",
            "attempt_ids",
            "attempts_persisted",
            "channel",
            "delivery_fingerprint",
            "endpoint_host",
            "execution_id",
            "execution_summary",
            "failed_count",
            "failure_code",
            "finished_at",
            "lock_acquired",
            "lock_key_fingerprint",
            "lock_model_version",
            "lock_released",
            "model_version",
            "persisted_event_ids",
            "plan_id",
            "recorded_at",
            "request_count",
            "request_id",
            "requested_event_ids",
            "retry_execution_policy_fingerprint",
            "retry_policy_fingerprint",
            "started_at",
            "succeeded_count",
            "terminal_status",
        },
    )
    if exact["model_version"] != MODEL_VERSION or exact["channel"] != CHANNEL:
        raise ValidationError("retry execution record contract is unsupported")
    rebuilt = build_retry_execution_record(
        request_id=exact["request_id"],
        plan_id=exact["plan_id"],
        started_at=exact["started_at"],
        finished_at=exact["finished_at"],
        recorded_at=exact["recorded_at"],
        terminal_status=exact["terminal_status"],
        failure_code=exact["failure_code"],
        request_count=exact["request_count"],
        attempts_persisted=exact["attempts_persisted"],
        succeeded_count=exact["succeeded_count"],
        failed_count=exact["failed_count"],
        attempt_ids=exact["attempt_ids"],
        requested_event_ids=exact["requested_event_ids"],
        persisted_event_ids=exact["persisted_event_ids"],
        execution_summary=exact["execution_summary"],
        endpoint_host=exact["endpoint_host"],
        delivery_fingerprint=exact["delivery_fingerprint"],
        retry_policy_fingerprint=exact["retry_policy_fingerprint"],
        retry_execution_policy_fingerprint=exact[
            "retry_execution_policy_fingerprint"
        ],
        lock_model_version=exact["lock_model_version"],
        lock_key_fingerprint=exact["lock_key_fingerprint"],
        lock_acquired=exact["lock_acquired"],
        lock_released=exact["lock_released"],
    )
    if exact["record_id"] != rebuilt["record_id"]:
        raise ValidationError("retry execution record_id does not match content")
    if dict(exact) != rebuilt:
        raise ValidationError("retry execution record is not canonical")
    return rebuilt
