from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from ..common.exceptions import ValidationError
from .operational_service_level_objective_policy import (
    EXPECTED_UNITS,
    MODEL_VERSION,
    OBJECTIVE_NAMES,
    ObjectiveTarget,
    OperationalObjectivePolicy,
    load_operational_objective_policy,
    parse_operational_objective_policy,
)
from .operational_service_level_objective_reports import (
    MAX_REPORT_ROWS,
    select_current_operational_reports,
)

POLICY_FINGERPRINT_PATTERN = re.compile(
    r"^operational-slo-policy-[0-9a-f]{24}$"
)
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class OperationalObjectiveOutput:
    report: Mapping[str, Any]
    objectives: tuple[Mapping[str, Any], ...]


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _fingerprint(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    return value


def _validate_sessions(
    expected_sessions: tuple[date, ...],
    *,
    through_session: date,
    maximum: int,
) -> None:
    if (
        not isinstance(expected_sessions, tuple)
        or not expected_sessions
        or len(expected_sessions) > maximum
        or any(
            not isinstance(session, date) or isinstance(session, datetime)
            for session in expected_sessions
        )
        or tuple(sorted(set(expected_sessions))) != expected_sessions
        or expected_sessions[-1] != through_session
    ):
        raise ValidationError("expected session window is invalid")


def evaluate_operational_objectives(
    *,
    objective_policy: OperationalObjectivePolicy,
    through_session: date,
    expected_sessions: tuple[date, ...],
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_id: str,
    mandate_fingerprint: str,
    reports: Iterable[Mapping[str, Any]],
) -> OperationalObjectiveOutput:
    if not isinstance(through_session, date) or isinstance(through_session, datetime):
        raise ValidationError("through_session must be a calendar date")
    _validate_sessions(
        expected_sessions,
        through_session=through_session,
        maximum=objective_policy.window_sessions,
    )
    operational_policy_fingerprint = _fingerprint(
        operational_policy_fingerprint,
        "operational_policy_fingerprint",
    )
    if not POLICY_FINGERPRINT_PATTERN.fullmatch(operational_policy_fingerprint):
        raise ValidationError("operational policy fingerprint is incompatible")
    schedule_id = _safe_segment(schedule_id, "schedule_id")
    schedule_fingerprint = _fingerprint(schedule_fingerprint, "schedule_fingerprint")
    calendar_id = _safe_segment(calendar_id, "calendar_id")
    portfolio_id = _safe_segment(portfolio_id, "portfolio_id")
    risk_limit_policy_id = _safe_segment(
        risk_limit_policy_id,
        "risk_limit_policy_id",
    )
    mandate_id = _safe_segment(mandate_id, "mandate_id")
    mandate_fingerprint = _fingerprint(mandate_fingerprint, "mandate_fingerprint")
    expected_contract = {
        "policy_id": objective_policy.operational_policy_id,
        "policy_fingerprint": operational_policy_fingerprint,
        "schedule_id": schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "portfolio_id": portfolio_id,
        "risk_limit_policy_id": risk_limit_policy_id,
        "mandate_fingerprint": mandate_fingerprint,
    }
    selected = select_current_operational_reports(
        reports,
        expected_contract=expected_contract,
        expected_sessions=expected_sessions,
    )
    by_session = {
        report["latest_expected_session"]: report for report in selected.reports
    }
    window = [
        by_session[session] for session in expected_sessions if session in by_session
    ]
    missing_sessions = tuple(
        session for session in expected_sessions if session not in by_session
    )
    observations_available = len(window)
    observations_expected = len(expected_sessions)
    history_status = (
        "ready"
        if observations_available >= objective_policy.minimum_observations
        else "insufficient"
    )
    objective_rows: list[dict[str, Any]] = []
    for objective_name in OBJECTIVE_NAMES:
        target: ObjectiveTarget = objective_policy.objectives[objective_name]
        successful = sum(
            1
            for report in window
            if report["metrics"][target.source_metric_name] is not None
            and float(report["metrics"][target.source_metric_name])
            <= target.success_threshold
        )
        attainment_ratio = successful / observations_expected
        status = (
            "insufficient"
            if history_status == "insufficient"
            else (
                "met"
                if attainment_ratio >= target.target_ratio
                else "missed"
            )
        )
        objective_rows.append(
            {
                "objective_name": objective_name,
                "source_metric_name": target.source_metric_name,
                "source_unit": EXPECTED_UNITS[target.source_metric_name],
                "success_threshold": target.success_threshold,
                "target_ratio": target.target_ratio,
                "successful_observations": successful,
                "failed_observations": observations_available - successful,
                "missing_report_observations": len(missing_sessions),
                "observations_available": observations_available,
                "observations_expected": observations_expected,
                "attainment_ratio": attainment_ratio,
                "status": status,
            }
        )
    overall_status = (
        "insufficient"
        if history_status == "insufficient"
        else (
            "missed"
            if any(row["status"] == "missed" for row in objective_rows)
            else "met"
        )
    )
    input_ids = [str(report["calculation_id"]) for report in window]
    input_digests = [str(report["document_sha256"]) for report in window]
    calculated_at = max(report["as_of"] for report in window)
    missing_dates = [session.isoformat() for session in missing_sessions]
    identity = {
        "calculated_at": calculated_at.isoformat(),
        "expected_sessions": [session.isoformat() for session in expected_sessions],
        "input_report_calculation_ids": input_ids,
        "input_report_document_sha256": input_digests,
        "mandate_fingerprint": mandate_fingerprint,
        "missing_report_sessions": missing_dates,
        "objective_policy_fingerprint": objective_policy.fingerprint,
        "objectives": objective_rows,
        "operational_policy_fingerprint": operational_policy_fingerprint,
        "schedule_fingerprint": schedule_fingerprint,
        "through_session": through_session.isoformat(),
    }
    digest = hashlib.sha256(
        json.dumps(
            identity,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    report = {
        "calculation_id": f"{MODEL_VERSION}-report-{digest}",
        "model_version": MODEL_VERSION,
        "objective_policy_id": objective_policy.objective_policy_id,
        "objective_policy_fingerprint": objective_policy.fingerprint,
        "operational_policy_id": objective_policy.operational_policy_id,
        "operational_policy_fingerprint": operational_policy_fingerprint,
        "schedule_id": schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "portfolio_id": portfolio_id,
        "risk_limit_policy_id": risk_limit_policy_id,
        "mandate_id": mandate_id,
        "mandate_fingerprint": mandate_fingerprint,
        "through_session": through_session.isoformat(),
        "window_start_session": expected_sessions[0].isoformat(),
        "window_end_session": expected_sessions[-1].isoformat(),
        "window_sessions": objective_policy.window_sessions,
        "minimum_observations": objective_policy.minimum_observations,
        "observations_available": observations_available,
        "observations_expected": observations_expected,
        "missing_report_sessions": missing_dates,
        "window_complete": (
            observations_expected == objective_policy.window_sessions
            and not missing_sessions
        ),
        "history_status": history_status,
        "overall_status": overall_status,
        "calculated_at": calculated_at.isoformat(),
        "input_report_calculation_ids": input_ids,
        "input_report_document_sha256": input_digests,
        "objectives": objective_rows,
        "input_rows_scanned": selected.input_rows_scanned,
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
        "automated_remediation_performed": False,
    }
    return OperationalObjectiveOutput(
        report=report,
        objectives=tuple(objective_rows),
    )


__all__ = [
    "MAX_REPORT_ROWS",
    "MODEL_VERSION",
    "OBJECTIVE_NAMES",
    "ObjectiveTarget",
    "OperationalObjectiveOutput",
    "OperationalObjectivePolicy",
    "evaluate_operational_objectives",
    "load_operational_objective_policy",
    "parse_operational_objective_policy",
]
