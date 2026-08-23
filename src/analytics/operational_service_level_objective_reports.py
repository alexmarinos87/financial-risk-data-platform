from __future__ import annotations

import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ..common.exceptions import ValidationError
from .operational_service_level_objective_policy import EXPECTED_UNITS
from .operational_service_levels import METRIC_NAMES

MAX_REPORT_ROWS = 10_000
REPORT_ID_PATTERN = re.compile(
    r"^operational-service-levels-v1-report-[0-9a-f]{24}$"
)
DOCUMENT_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class SelectedOperationalReports:
    reports: tuple[Mapping[str, Any], ...]
    input_rows_scanned: int


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


def _metric_values(value: Any) -> dict[str, float | None]:
    if not isinstance(value, list) or len(value) != len(METRIC_NAMES):
        raise ValidationError("report metrics must contain four indicators")
    result: dict[str, float | None] = {}
    names: list[str] = []
    for raw in value:
        if not isinstance(raw, Mapping):
            raise ValidationError("report metrics must contain mappings")
        name = raw.get("metric_name")
        if name not in METRIC_NAMES or not isinstance(name, str):
            raise ValidationError("report metric name is unsupported")
        if raw.get("unit") != EXPECTED_UNITS[name]:
            raise ValidationError("report metric unit is incompatible")
        observed = raw.get("observed_value")
        if observed is None:
            if name != "schedule_lag_sessions" or raw.get("reason") != "checkpoint_missing":
                raise ValidationError("only a missing checkpoint may have no value")
            parsed: float | None = None
        else:
            if isinstance(observed, bool) or not isinstance(observed, (int, float)):
                raise ValidationError("report metric value must be numeric")
            parsed = float(observed)
            if not math.isfinite(parsed) or parsed < 0:
                raise ValidationError("report metric value must be finite and non-negative")
        result[name] = parsed
        names.append(name)
    if tuple(names) != METRIC_NAMES:
        raise ValidationError("report metrics must use the canonical order")
    return result


def _normalise_report(
    raw: Mapping[str, Any],
    *,
    expected_contract: Mapping[str, str],
    expected_sessions: frozenset[date],
) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        raise ValidationError("objective input must contain mappings")
    if any(raw.get(key) != expected for key, expected in expected_contract.items()):
        return None
    calculation_id = raw.get("calculation_id")
    if not isinstance(calculation_id, str) or not REPORT_ID_PATTERN.fullmatch(
        calculation_id
    ):
        raise ValidationError("report calculation_id is incompatible")
    document_sha256 = raw.get("document_sha256")
    if not isinstance(document_sha256, str) or not DOCUMENT_SHA256_PATTERN.fullmatch(
        document_sha256
    ):
        raise ValidationError("report document_sha256 is incompatible")
    session = _calendar_date(
        raw.get("latest_expected_session"),
        "latest_expected_session",
    )
    if session not in expected_sessions:
        return None
    return {
        **expected_contract,
        "calculation_id": calculation_id,
        "document_sha256": document_sha256,
        "as_of": _aware_utc(raw.get("as_of"), "report as_of"),
        "latest_expected_session": session,
        "metrics": _metric_values(raw.get("metrics_json")),
    }


def _signature(report: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        *(report[key] for key in (
            "policy_id",
            "policy_fingerprint",
            "schedule_id",
            "schedule_fingerprint",
            "calendar_id",
            "portfolio_id",
            "risk_limit_policy_id",
            "mandate_fingerprint",
        )),
        report["as_of"],
        report["latest_expected_session"],
        report["document_sha256"],
        tuple(report["metrics"].items()),
    )


def select_current_operational_reports(
    reports: Iterable[Mapping[str, Any]],
    *,
    expected_contract: Mapping[str, str],
    expected_sessions: tuple[date, ...],
) -> SelectedOperationalReports:
    current: dict[date, dict[str, Any]] = {}
    seen_ids: dict[str, tuple[Any, ...]] = {}
    input_rows = 0
    session_set = frozenset(expected_sessions)
    for raw in reports:
        input_rows += 1
        if input_rows > MAX_REPORT_ROWS:
            raise ValidationError("objective input exceeds the row limit")
        report = _normalise_report(
            raw,
            expected_contract=expected_contract,
            expected_sessions=session_set,
        )
        if report is None:
            continue
        calculation_id = report["calculation_id"]
        signature = _signature(report)
        previous = seen_ids.get(calculation_id)
        if previous is not None:
            if previous != signature:
                raise ValidationError("report calculation ID has conflicting content")
            continue
        seen_ids[calculation_id] = signature
        session = report["latest_expected_session"]
        existing = current.get(session)
        if existing is None or (
            report["as_of"],
            calculation_id,
        ) > (
            existing["as_of"],
            existing["calculation_id"],
        ):
            current[session] = report
    if not current:
        raise ValidationError("no reports matched the requested objective window")
    return SelectedOperationalReports(
        reports=tuple(current[session] for session in sorted(current)),
        input_rows_scanned=input_rows,
    )
