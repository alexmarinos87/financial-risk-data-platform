from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.common.exceptions import StorageError, ValidationError

MODEL_VERSION = "local-schedule-run-v1"
MAX_RUN_BYTES = 1_000_000
MAX_SESSIONS = 31
MAX_STAGES_PER_SESSION = 128
MAX_REQUEST_ID_LENGTH = 128
MAX_TEXT_LENGTH = 256

RUN_ID_PATTERN = re.compile(r"^local-schedule-run-v1-run-[0-9a-f]{24}$")
PLAN_ID_PATTERN = re.compile(
    r"^readiness-aware-schedule-plan-v1-plan-[0-9a-f]{24}$"
)
AUTHORITY_ID_PATTERN = re.compile(
    r"^operational-readiness-execution-authority-v1-authority-[0-9a-f]{24}$"
)
DECISION_ID_PATTERN = re.compile(
    r"^operational-readiness-gate-v1-decision-[0-9a-f]{24}$"
)
OVERRIDE_ID_PATTERN = re.compile(
    r"^operational-readiness-override-v1-[0-9a-f]{24}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")

RUN_KEYS = frozenset(
    {
        "run_id",
        "model_version",
        "request_id",
        "plan_id",
        "authority_id",
        "authority_type",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "calendar_fingerprint",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_id",
        "mandate_fingerprint",
        "as_of_date",
        "latest_expected_session",
        "readiness_decision_id",
        "readiness_document_sha256",
        "override_id",
        "authorized_at",
        "started_at",
        "finished_at",
        "run_status",
        "checkpoint_before",
        "checkpoint_after",
        "selected_session_count",
        "started_session_count",
        "completed_session_count",
        "failed_session",
        "failed_stage_index",
        "failed_stage_name",
        "failure_code",
        "sessions",
        "provider_request_performed",
        "notification_delivery_performed",
        "cloud_schedule_activated",
    }
)
SESSION_KEYS = frozenset(
    {
        "session_date",
        "mandate_id",
        "mandate_fingerprint",
        "status",
        "started_at",
        "finished_at",
        "checkpoint_after",
        "failed_stage_index",
        "failed_stage_name",
        "failure_code",
        "stages",
    }
)
STAGE_KEYS = frozenset(
    {
        "stage_index",
        "stage_name",
        "status",
        "started_at",
        "finished_at",
        "failure_code",
    }
)


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _bounded_text(value: Any, label: str, maximum: int = MAX_TEXT_LENGTH) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{label} must be non-empty text")
    parsed = value.strip()
    if len(parsed) > maximum or any(ord(character) < 32 for character in parsed):
        raise ValidationError(f"{label} must be bounded printable text")
    return parsed


def _pattern(value: Any, label: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValidationError(f"{label} is incompatible")
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


def _optional_timestamp(value: Any, label: str) -> datetime | None:
    return None if value is None else _aware_utc(value, label)


def _calendar_date(value: Any, label: str) -> date:
    if isinstance(value, datetime):
        raise ValidationError(f"{label} must be a calendar date")
    if isinstance(value, date):
        return value
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if value != parsed.isoformat():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def _optional_date(value: Any, label: str) -> date | None:
    return None if value is None else _calendar_date(value, label)


def _optional_integer(value: Any, label: str) -> int | None:
    if value is None:
        return None
    if type(value) is not int or value < 0:
        raise ValidationError(f"{label} must be a non-negative integer")
    return value


def _optional_text(value: Any, label: str) -> str | None:
    return None if value is None else _bounded_text(value, label)


def _request_id(value: Any) -> str:
    return _bounded_text(value, "request_id", MAX_REQUEST_ID_LENGTH)


def build_local_schedule_run_id(
    *,
    request_identifier: str,
    plan_id: str,
    authority_id: str,
) -> str:
    request = _request_id(request_identifier)
    canonical_plan_id = _pattern(plan_id, "plan_id", PLAN_ID_PATTERN)
    canonical_authority_id = _pattern(
        authority_id,
        "authority_id",
        AUTHORITY_ID_PATTERN,
    )
    digest = hashlib.sha256(
        json.dumps(
            {
                "authority_id": canonical_authority_id,
                "model_version": MODEL_VERSION,
                "plan_id": canonical_plan_id,
                "request_id": request,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-run-{digest}"


def _validate_stage(
    payload: Any,
    *,
    expected_index: int,
    session_started_at: datetime,
    session_finished_at: datetime,
    previous_finished_at: datetime | None,
) -> tuple[dict[str, Any], datetime]:
    if not isinstance(payload, Mapping) or set(payload) != STAGE_KEYS:
        raise ValidationError("local schedule stage has an invalid shape")
    stage_index = payload.get("stage_index")
    if stage_index != expected_index:
        raise ValidationError("stage_index must use contiguous zero-based order")
    status = payload.get("status")
    if status not in {"completed", "failed"}:
        raise ValidationError("stage status must be completed or failed")
    started_at = _aware_utc(payload.get("started_at"), "stage.started_at")
    finished_at = _aware_utc(payload.get("finished_at"), "stage.finished_at")
    if not session_started_at <= started_at <= finished_at <= session_finished_at:
        raise ValidationError("stage timestamps fall outside the session interval")
    if previous_finished_at is not None and started_at < previous_finished_at:
        raise ValidationError("stage timestamps must use non-overlapping order")
    failure_code = _optional_text(payload.get("failure_code"), "stage.failure_code")
    if (status == "failed") != (failure_code is not None):
        raise ValidationError("failed stage evidence must include one failure_code")
    return (
        {
            "stage_index": expected_index,
            "stage_name": _bounded_text(payload.get("stage_name"), "stage_name"),
            "status": status,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "failure_code": failure_code,
        },
        finished_at,
    )


def _validate_session(payload: Any, *, expected_date: date) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != SESSION_KEYS:
        raise ValidationError("local schedule session has an invalid shape")
    session_date = _calendar_date(payload.get("session_date"), "session_date")
    if session_date != expected_date:
        raise ValidationError("session evidence does not match selected session order")
    status = payload.get("status")
    if status not in {"selected", "completed", "failed"}:
        raise ValidationError("session status is incompatible")
    started_at = _optional_timestamp(payload.get("started_at"), "session.started_at")
    finished_at = _optional_timestamp(payload.get("finished_at"), "session.finished_at")
    checkpoint_after = _optional_date(
        payload.get("checkpoint_after"),
        "session.checkpoint_after",
    )
    failed_stage_index = _optional_integer(
        payload.get("failed_stage_index"),
        "session.failed_stage_index",
    )
    failed_stage_name = _optional_text(
        payload.get("failed_stage_name"),
        "session.failed_stage_name",
    )
    failure_code = _optional_text(payload.get("failure_code"), "session.failure_code")
    stages = payload.get("stages")
    if not isinstance(stages, list) or len(stages) > MAX_STAGES_PER_SESSION:
        raise ValidationError("session stages must be a bounded array")

    if status == "selected":
        if any(
            value is not None
            for value in (
                started_at,
                finished_at,
                checkpoint_after,
                failed_stage_index,
                failed_stage_name,
                failure_code,
            )
        ) or stages:
            raise ValidationError("unstarted selected session contains outcome evidence")
        validated_stages: list[dict[str, Any]] = []
    else:
        if started_at is None or finished_at is None or finished_at < started_at:
            raise ValidationError("started session requires an ordered time interval")
        if not stages:
            raise ValidationError("started session requires stage outcomes")
        validated_stages = []
        previous_finished_at: datetime | None = None
        for index, stage in enumerate(stages):
            validated, previous_finished_at = _validate_stage(
                stage,
                expected_index=index,
                session_started_at=started_at,
                session_finished_at=finished_at,
                previous_finished_at=previous_finished_at,
            )
            validated_stages.append(validated)

        failed_stages = [
            stage for stage in validated_stages if stage["status"] == "failed"
        ]
        if status == "completed":
            if failed_stages or checkpoint_after != session_date:
                raise ValidationError(
                    "completed session stages and checkpoint do not reconcile"
                )
            if any(
                value is not None
                for value in (failed_stage_index, failed_stage_name, failure_code)
            ):
                raise ValidationError("completed session contains failure evidence")
        else:
            if checkpoint_after is not None or len(failed_stages) != 1:
                raise ValidationError("failed session evidence is incompatible")
            failed_stage = failed_stages[0]
            if failed_stage is not validated_stages[-1]:
                raise ValidationError("failed stage must be the final attempted stage")
            if (
                failed_stage_index != failed_stage["stage_index"]
                or failed_stage_name != failed_stage["stage_name"]
                or failure_code != failed_stage["failure_code"]
            ):
                raise ValidationError("session failure fields do not match failed stage")

    return {
        "session_date": session_date.isoformat(),
        "mandate_id": _safe_segment(payload.get("mandate_id"), "session.mandate_id"),
        "mandate_fingerprint": _bounded_text(
            payload.get("mandate_fingerprint"),
            "session.mandate_fingerprint",
        ),
        "status": status,
        "started_at": started_at.isoformat() if started_at is not None else None,
        "finished_at": finished_at.isoformat() if finished_at is not None else None,
        "checkpoint_after": (
            checkpoint_after.isoformat() if checkpoint_after is not None else None
        ),
        "failed_stage_index": failed_stage_index,
        "failed_stage_name": failed_stage_name,
        "failure_code": failure_code,
        "stages": validated_stages,
    }


def validate_local_schedule_run(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != RUN_KEYS:
        raise ValidationError("local schedule run has an invalid shape")
    if payload.get("model_version") != MODEL_VERSION:
        raise ValidationError("local schedule run model version is unsupported")

    request = _request_id(payload.get("request_id"))
    plan_id = _pattern(payload.get("plan_id"), "plan_id", PLAN_ID_PATTERN)
    authority_id = _pattern(
        payload.get("authority_id"),
        "authority_id",
        AUTHORITY_ID_PATTERN,
    )
    if request != authority_id:
        raise ValidationError("request_id must equal the execution authority ID")
    run_id = _pattern(payload.get("run_id"), "run_id", RUN_ID_PATTERN)
    expected_run_id = build_local_schedule_run_id(
        request_identifier=request,
        plan_id=plan_id,
        authority_id=authority_id,
    )
    if run_id != expected_run_id:
        raise ValidationError("run_id does not match request, plan and authority")

    authority_type = payload.get("authority_type")
    if authority_type not in {"gate_allow", "active_override"}:
        raise ValidationError("authority_type is incompatible")
    override_id = payload.get("override_id")
    if authority_type == "gate_allow":
        if override_id is not None:
            raise ValidationError("gate_allow run must not include an override_id")
        canonical_override_id: str | None = None
    else:
        canonical_override_id = _pattern(
            override_id,
            "override_id",
            OVERRIDE_ID_PATTERN,
        )

    authorized_at = _aware_utc(payload.get("authorized_at"), "authorized_at")
    started_at = _aware_utc(payload.get("started_at"), "started_at")
    finished_at = _aware_utc(payload.get("finished_at"), "finished_at")
    if not authorized_at <= started_at <= finished_at:
        raise ValidationError("run timestamps do not follow authorization order")

    as_of_date = _calendar_date(payload.get("as_of_date"), "as_of_date")
    latest_expected_session = _calendar_date(
        payload.get("latest_expected_session"),
        "latest_expected_session",
    )
    if latest_expected_session > as_of_date:
        raise ValidationError("latest_expected_session is after as_of_date")

    sessions = payload.get("sessions")
    if not isinstance(sessions, list) or not 1 <= len(sessions) <= MAX_SESSIONS:
        raise ValidationError("sessions must be a non-empty bounded array")
    raw_session_dates = [
        _calendar_date(
            session.get("session_date") if isinstance(session, Mapping) else None,
            "session_date",
        )
        for session in sessions
    ]
    if raw_session_dates != sorted(raw_session_dates) or len(raw_session_dates) != len(
        set(raw_session_dates)
    ):
        raise ValidationError("sessions must use unique ascending dates")
    if raw_session_dates[-1] != latest_expected_session:
        raise ValidationError("selected sessions must end at latest_expected_session")

    validated_sessions = [
        _validate_session(session, expected_date=session_date)
        for session, session_date in zip(sessions, raw_session_dates, strict=True)
    ]
    statuses = [session["status"] for session in validated_sessions]
    completed_count = statuses.count("completed")
    failed_count = statuses.count("failed")
    started_count = completed_count + failed_count
    completed_prefix = ["completed"] * completed_count
    if failed_count == 0:
        expected_statuses = completed_prefix + ["selected"] * (
            len(statuses) - completed_count
        )
    else:
        expected_statuses = (
            completed_prefix
            + ["failed"]
            + ["selected"] * (len(statuses) - completed_count - 1)
        )
    if statuses != expected_statuses or failed_count > 1:
        raise ValidationError("session statuses do not form a terminal execution prefix")

    selected_count = payload.get("selected_session_count")
    supplied_started_count = payload.get("started_session_count")
    supplied_completed_count = payload.get("completed_session_count")
    if (
        selected_count != len(validated_sessions)
        or supplied_started_count != started_count
        or supplied_completed_count != completed_count
    ):
        raise ValidationError("run session counts do not reconcile")

    checkpoint_before = _optional_date(
        payload.get("checkpoint_before"),
        "checkpoint_before",
    )
    checkpoint_after = _optional_date(
        payload.get("checkpoint_after"),
        "checkpoint_after",
    )
    expected_checkpoint_after = (
        raw_session_dates[completed_count - 1]
        if completed_count
        else checkpoint_before
    )
    if checkpoint_after != expected_checkpoint_after:
        raise ValidationError("checkpoint_after does not match completed session prefix")

    run_status = payload.get("run_status")
    failed_session = _optional_date(payload.get("failed_session"), "failed_session")
    failed_stage_index = _optional_integer(
        payload.get("failed_stage_index"),
        "failed_stage_index",
    )
    failed_stage_name = _optional_text(
        payload.get("failed_stage_name"),
        "failed_stage_name",
    )
    failure_code = _optional_text(payload.get("failure_code"), "failure_code")
    if run_status == "completed":
        if completed_count != len(validated_sessions) or failed_count:
            raise ValidationError("completed run does not contain all completed sessions")
        if any(
            value is not None
            for value in (
                failed_session,
                failed_stage_index,
                failed_stage_name,
                failure_code,
            )
        ):
            raise ValidationError("completed run contains failure evidence")
    elif run_status == "failed":
        if failure_code is None:
            raise ValidationError("failed run requires a failure_code")
        failed_sessions = [
            session for session in validated_sessions if session["status"] == "failed"
        ]
        if failed_sessions:
            failed = failed_sessions[0]
            if (
                failed_session != date.fromisoformat(failed["session_date"])
                or failed_stage_index != failed["failed_stage_index"]
                or failed_stage_name != failed["failed_stage_name"]
                or failure_code != failed["failure_code"]
            ):
                raise ValidationError("run failure fields do not match failed session")
        elif any(
            value is not None
            for value in (failed_session, failed_stage_index, failed_stage_name)
        ):
            raise ValidationError("pre-session failure contains session failure fields")
    else:
        raise ValidationError("run_status must be completed or failed")

    session_start_values = [
        _aware_utc(session["started_at"], "session.started_at")
        for session in validated_sessions
        if session["started_at"] is not None
    ]
    session_finish_values = [
        _aware_utc(session["finished_at"], "session.finished_at")
        for session in validated_sessions
        if session["finished_at"] is not None
    ]
    if session_start_values and started_at > min(session_start_values):
        raise ValidationError("run started_at is after the first session start")
    if session_finish_values and finished_at < max(session_finish_values):
        raise ValidationError("run finished_at is before a session finish")

    for flag in (
        "provider_request_performed",
        "notification_delivery_performed",
        "cloud_schedule_activated",
    ):
        if payload.get(flag) is not False:
            raise ValidationError(f"{flag} must remain false")

    return {
        "run_id": run_id,
        "model_version": MODEL_VERSION,
        "request_id": request,
        "plan_id": plan_id,
        "authority_id": authority_id,
        "authority_type": authority_type,
        "schedule_id": _safe_segment(payload.get("schedule_id"), "schedule_id"),
        "schedule_fingerprint": _bounded_text(
            payload.get("schedule_fingerprint"),
            "schedule_fingerprint",
        ),
        "calendar_id": _safe_segment(payload.get("calendar_id"), "calendar_id"),
        "calendar_fingerprint": _bounded_text(
            payload.get("calendar_fingerprint"),
            "calendar_fingerprint",
        ),
        "portfolio_id": _safe_segment(payload.get("portfolio_id"), "portfolio_id"),
        "risk_limit_policy_id": _safe_segment(
            payload.get("risk_limit_policy_id"),
            "risk_limit_policy_id",
        ),
        "mandate_id": _safe_segment(payload.get("mandate_id"), "mandate_id"),
        "mandate_fingerprint": _bounded_text(
            payload.get("mandate_fingerprint"),
            "mandate_fingerprint",
        ),
        "as_of_date": as_of_date.isoformat(),
        "latest_expected_session": latest_expected_session.isoformat(),
        "readiness_decision_id": _pattern(
            payload.get("readiness_decision_id"),
            "readiness_decision_id",
            DECISION_ID_PATTERN,
        ),
        "readiness_document_sha256": _pattern(
            payload.get("readiness_document_sha256"),
            "readiness_document_sha256",
            SHA256_PATTERN,
        ),
        "override_id": canonical_override_id,
        "authorized_at": authorized_at.isoformat(),
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "run_status": run_status,
        "checkpoint_before": (
            checkpoint_before.isoformat() if checkpoint_before is not None else None
        ),
        "checkpoint_after": (
            checkpoint_after.isoformat() if checkpoint_after is not None else None
        ),
        "selected_session_count": len(validated_sessions),
        "started_session_count": started_count,
        "completed_session_count": completed_count,
        "failed_session": failed_session.isoformat() if failed_session else None,
        "failed_stage_index": failed_stage_index,
        "failed_stage_name": failed_stage_name,
        "failure_code": failure_code,
        "sessions": validated_sessions,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def canonical_local_schedule_run_bytes(run: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            run,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("local schedule run is not canonical JSON") from None


def read_local_schedule_run(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise StorageError("local schedule run must not be a symbolic link")
    if not path.is_file():
        raise StorageError("local schedule run must be a regular file")
    try:
        if path.stat().st_size > MAX_RUN_BYTES:
            raise StorageError("local schedule run exceeds the byte limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except StorageError:
        raise
    except (OSError, ValueError):
        raise StorageError("local schedule run could not be read") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("local schedule run must be a JSON object")
    return validate_local_schedule_run(payload)
