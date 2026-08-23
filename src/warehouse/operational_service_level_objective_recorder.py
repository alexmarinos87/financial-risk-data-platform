from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.analytics.operational_service_level_objectives import (
    MAX_REPORT_ROWS,
    MODEL_VERSION,
    OBJECTIVE_NAMES,
)
from src.analytics.operational_service_level_objective_policy import (
    EXPECTED_UNITS,
    OBJECTIVE_SOURCE_METRICS,
)
from src.common.exceptions import StorageError, ValidationError
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_REPORT_BYTES = 2_000_000
CALCULATION_ID_PATTERN = re.compile(
    r"^operational-slo-attainment-v1-report-[0-9a-f]{24}$"
)
OBJECTIVE_POLICY_FINGERPRINT_PATTERN = re.compile(
    r"^operational-slo-objective-policy-[0-9a-f]{24}$"
)
OPERATIONAL_POLICY_FINGERPRINT_PATTERN = re.compile(
    r"^operational-slo-policy-[0-9a-f]{24}$"
)
REPORT_ID_PATTERN = re.compile(
    r"^operational-service-levels-v1-report-[0-9a-f]{24}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
REPORT_KEYS = frozenset(
    {
        "calculation_id",
        "model_version",
        "objective_policy_id",
        "objective_policy_fingerprint",
        "operational_policy_id",
        "operational_policy_fingerprint",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_id",
        "mandate_fingerprint",
        "through_session",
        "window_start_session",
        "window_end_session",
        "window_sessions",
        "minimum_observations",
        "observations_available",
        "observations_expected",
        "missing_report_sessions",
        "window_complete",
        "history_status",
        "overall_status",
        "calculated_at",
        "input_report_calculation_ids",
        "input_report_document_sha256",
        "objectives",
        "input_rows_scanned",
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
        "automated_remediation_performed",
    }
)
OBJECTIVE_KEYS = frozenset(
    {
        "objective_name",
        "source_metric_name",
        "source_unit",
        "success_threshold",
        "target_ratio",
        "successful_observations",
        "failed_observations",
        "missing_report_observations",
        "observations_available",
        "observations_expected",
        "attainment_ratio",
        "status",
    }
)


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _fingerprint(value: Any, label: str, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    if pattern is not None and not pattern.fullmatch(value):
        raise ValidationError(f"{label} is incompatible")
    return value


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


def _bounded_int(
    value: Any,
    label: str,
    *,
    minimum: int,
    maximum: int,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValidationError(
            f"{label} must be an integer between {minimum} and {maximum}"
        )
    return value


def _finite_nonnegative(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def _date_array(value: Any, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be an array")
    parsed = [_calendar_date(item, label).isoformat() for item in value]
    if parsed != sorted(set(parsed)):
        raise ValidationError(f"{label} must be unique and ordered")
    return parsed


def _text_array(
    value: Any,
    label: str,
    *,
    pattern: re.Pattern[str],
) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be an array")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str) or not pattern.fullmatch(item):
            raise ValidationError(f"{label} contains an incompatible value")
        parsed.append(item)
    if len(parsed) != len(set(parsed)):
        raise ValidationError(f"{label} must not contain duplicates")
    return parsed


def _objectives(
    value: Any,
    *,
    history_status: str,
    observations_available: int,
    observations_expected: int,
    missing_count: int,
) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(value, list) or len(value) != len(OBJECTIVE_NAMES):
        raise ValidationError("objectives must contain the four supported rows")
    rows: list[dict[str, Any]] = []
    statuses: list[str] = []
    names: list[str] = []
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != OBJECTIVE_KEYS:
            raise ValidationError("each objective must match the strict contract")
        name = raw.get("objective_name")
        if name not in OBJECTIVE_NAMES or not isinstance(name, str):
            raise ValidationError("objective_name is unsupported")
        metric_name = OBJECTIVE_SOURCE_METRICS[name]
        if raw.get("source_metric_name") != metric_name:
            raise ValidationError("objective source metric is incompatible")
        if raw.get("source_unit") != EXPECTED_UNITS[metric_name]:
            raise ValidationError("objective source unit is incompatible")
        threshold = _finite_nonnegative(
            raw.get("success_threshold"),
            f"{name}.success_threshold",
        )
        target_ratio = _finite_nonnegative(
            raw.get("target_ratio"),
            f"{name}.target_ratio",
        )
        if not 0 < target_ratio <= 1:
            raise ValidationError("objective target_ratio must be in (0, 1]")
        successful = _bounded_int(
            raw.get("successful_observations"),
            f"{name}.successful_observations",
            minimum=0,
            maximum=observations_available,
        )
        failed = _bounded_int(
            raw.get("failed_observations"),
            f"{name}.failed_observations",
            minimum=0,
            maximum=observations_available,
        )
        if successful + failed != observations_available:
            raise ValidationError("objective success and failure counts do not reconcile")
        if raw.get("missing_report_observations") != missing_count:
            raise ValidationError("objective missing-report count does not reconcile")
        if raw.get("observations_available") != observations_available:
            raise ValidationError("objective available count does not reconcile")
        if raw.get("observations_expected") != observations_expected:
            raise ValidationError("objective expected count does not reconcile")
        attainment_ratio = _finite_nonnegative(
            raw.get("attainment_ratio"),
            f"{name}.attainment_ratio",
        )
        expected_ratio = successful / observations_expected
        if not math.isclose(
            attainment_ratio,
            expected_ratio,
            rel_tol=0.0,
            abs_tol=1e-12,
        ):
            raise ValidationError("objective attainment ratio does not reconcile")
        expected_status = (
            "insufficient"
            if history_status == "insufficient"
            else ("met" if attainment_ratio >= target_ratio else "missed")
        )
        if raw.get("status") != expected_status:
            raise ValidationError("objective status does not match attainment")
        rows.append(
            {
                "objective_name": name,
                "source_metric_name": metric_name,
                "source_unit": EXPECTED_UNITS[metric_name],
                "success_threshold": threshold,
                "target_ratio": target_ratio,
                "successful_observations": successful,
                "failed_observations": failed,
                "missing_report_observations": missing_count,
                "observations_available": observations_available,
                "observations_expected": observations_expected,
                "attainment_ratio": attainment_ratio,
                "status": expected_status,
            }
        )
        names.append(name)
        statuses.append(expected_status)
    if tuple(names) != OBJECTIVE_NAMES:
        raise ValidationError("objectives must use the canonical order")
    overall = (
        "insufficient"
        if history_status == "insufficient"
        else ("missed" if "missed" in statuses else "met")
    )
    return rows, overall


def validate_operational_objective_report(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != REPORT_KEYS:
        raise ValidationError("operational objective report has an invalid shape")
    calculation_id = payload.get("calculation_id")
    if not isinstance(calculation_id, str) or not CALCULATION_ID_PATTERN.fullmatch(
        calculation_id
    ):
        raise ValidationError("calculation_id is incompatible")
    if payload.get("model_version") != MODEL_VERSION:
        raise ValidationError("operational objective model version is unsupported")
    through_session = _calendar_date(payload.get("through_session"), "through_session")
    window_start = _calendar_date(
        payload.get("window_start_session"),
        "window_start_session",
    )
    window_end = _calendar_date(
        payload.get("window_end_session"),
        "window_end_session",
    )
    if window_start > window_end or window_end != through_session:
        raise ValidationError("objective report window boundaries are inconsistent")
    window_sessions = _bounded_int(
        payload.get("window_sessions"),
        "window_sessions",
        minimum=2,
        maximum=2520,
    )
    minimum_observations = _bounded_int(
        payload.get("minimum_observations"),
        "minimum_observations",
        minimum=2,
        maximum=window_sessions,
    )
    observations_expected = _bounded_int(
        payload.get("observations_expected"),
        "observations_expected",
        minimum=1,
        maximum=window_sessions,
    )
    observations_available = _bounded_int(
        payload.get("observations_available"),
        "observations_available",
        minimum=0,
        maximum=observations_expected,
    )
    missing_sessions = _date_array(
        payload.get("missing_report_sessions"),
        "missing_report_sessions",
    )
    if len(missing_sessions) != observations_expected - observations_available:
        raise ValidationError("missing report sessions do not reconcile")
    input_ids = _text_array(
        payload.get("input_report_calculation_ids"),
        "input_report_calculation_ids",
        pattern=REPORT_ID_PATTERN,
    )
    input_digests = _text_array(
        payload.get("input_report_document_sha256"),
        "input_report_document_sha256",
        pattern=SHA256_PATTERN,
    )
    if len(input_ids) != observations_available or len(input_digests) != observations_available:
        raise ValidationError("input report evidence does not reconcile")
    history_status = payload.get("history_status")
    expected_history = (
        "ready"
        if observations_available >= minimum_observations
        else "insufficient"
    )
    if history_status != expected_history:
        raise ValidationError("history_status does not match observation count")
    objectives, expected_overall = _objectives(
        payload.get("objectives"),
        history_status=expected_history,
        observations_available=observations_available,
        observations_expected=observations_expected,
        missing_count=len(missing_sessions),
    )
    if payload.get("overall_status") != expected_overall:
        raise ValidationError("overall_status does not match objectives")
    expected_complete = (
        observations_expected == window_sessions and not missing_sessions
    )
    if payload.get("window_complete") is not expected_complete:
        raise ValidationError("window_complete does not match the window evidence")
    input_rows_scanned = _bounded_int(
        payload.get("input_rows_scanned"),
        "input_rows_scanned",
        minimum=observations_available,
        maximum=MAX_REPORT_ROWS,
    )
    for flag in (
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
        "automated_remediation_performed",
    ):
        if payload.get(flag) is not False:
            raise ValidationError(f"{flag} must be false for report-only evidence")
    report = {
        "calculation_id": calculation_id,
        "model_version": MODEL_VERSION,
        "objective_policy_id": _safe_segment(
            payload.get("objective_policy_id"),
            "objective_policy_id",
        ),
        "objective_policy_fingerprint": _fingerprint(
            payload.get("objective_policy_fingerprint"),
            "objective_policy_fingerprint",
            OBJECTIVE_POLICY_FINGERPRINT_PATTERN,
        ),
        "operational_policy_id": _safe_segment(
            payload.get("operational_policy_id"),
            "operational_policy_id",
        ),
        "operational_policy_fingerprint": _fingerprint(
            payload.get("operational_policy_fingerprint"),
            "operational_policy_fingerprint",
            OPERATIONAL_POLICY_FINGERPRINT_PATTERN,
        ),
        "schedule_id": _safe_segment(payload.get("schedule_id"), "schedule_id"),
        "schedule_fingerprint": _fingerprint(
            payload.get("schedule_fingerprint"),
            "schedule_fingerprint",
        ),
        "calendar_id": _safe_segment(payload.get("calendar_id"), "calendar_id"),
        "portfolio_id": _safe_segment(payload.get("portfolio_id"), "portfolio_id"),
        "risk_limit_policy_id": _safe_segment(
            payload.get("risk_limit_policy_id"),
            "risk_limit_policy_id",
        ),
        "mandate_id": _safe_segment(payload.get("mandate_id"), "mandate_id"),
        "mandate_fingerprint": _fingerprint(
            payload.get("mandate_fingerprint"),
            "mandate_fingerprint",
        ),
        "through_session": through_session.isoformat(),
        "window_start_session": window_start.isoformat(),
        "window_end_session": window_end.isoformat(),
        "window_sessions": window_sessions,
        "minimum_observations": minimum_observations,
        "observations_available": observations_available,
        "observations_expected": observations_expected,
        "missing_report_sessions": missing_sessions,
        "window_complete": expected_complete,
        "history_status": expected_history,
        "overall_status": expected_overall,
        "calculated_at": _aware_utc(
            payload.get("calculated_at"),
            "calculated_at",
        ).isoformat(),
        "input_report_calculation_ids": input_ids,
        "input_report_document_sha256": input_digests,
        "objectives": objectives,
        "input_rows_scanned": input_rows_scanned,
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
        "automated_remediation_performed": False,
    }
    return report


def canonical_objective_report_bytes(report: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            report,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("operational objective report is not canonical JSON") from None


def read_objective_report(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise StorageError("operational objective report must not be a symbolic link")
    if not path.is_file():
        raise StorageError("operational objective report must be a regular file")
    try:
        if path.stat().st_size > MAX_REPORT_BYTES:
            raise StorageError("operational objective report exceeds the byte limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except StorageError:
        raise
    except (OSError, ValueError):
        raise StorageError("operational objective report could not be read") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational objective report must be a JSON object")
    return validate_operational_objective_report(payload)


def record_operational_objective_report(
    *,
    dsn: str,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_operational_objective_report(report)
    canonical = canonical_objective_report_bytes(validated)
    document_sha256 = hashlib.sha256(canonical).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational objective recording requires psycopg") from exc

    created = False
    recorded_at: datetime | None = None
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_platform.operational_service_level_objective_reports (
                        calculation_id,
                        model_version,
                        objective_policy_id,
                        objective_policy_fingerprint,
                        operational_policy_id,
                        operational_policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_id,
                        mandate_fingerprint,
                        through_session,
                        window_start_session,
                        window_end_session,
                        window_sessions,
                        minimum_observations,
                        observations_available,
                        observations_expected,
                        missing_report_sessions,
                        window_complete,
                        history_status,
                        overall_status,
                        calculated_at,
                        input_report_calculation_ids,
                        input_report_document_sha256,
                        objectives_json,
                        input_rows_scanned,
                        provider_request_performed,
                        external_delivery_performed,
                        cloud_schedule_activated,
                        automated_remediation_performed,
                        report_json,
                        document_sha256
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (calculation_id) DO NOTHING
                    RETURNING recorded_at
                    """,
                    (
                        validated["calculation_id"],
                        validated["model_version"],
                        validated["objective_policy_id"],
                        validated["objective_policy_fingerprint"],
                        validated["operational_policy_id"],
                        validated["operational_policy_fingerprint"],
                        validated["schedule_id"],
                        validated["schedule_fingerprint"],
                        validated["calendar_id"],
                        validated["portfolio_id"],
                        validated["risk_limit_policy_id"],
                        validated["mandate_id"],
                        validated["mandate_fingerprint"],
                        date.fromisoformat(validated["through_session"]),
                        date.fromisoformat(validated["window_start_session"]),
                        date.fromisoformat(validated["window_end_session"]),
                        validated["window_sessions"],
                        validated["minimum_observations"],
                        validated["observations_available"],
                        validated["observations_expected"],
                        Jsonb(validated["missing_report_sessions"]),
                        validated["window_complete"],
                        validated["history_status"],
                        validated["overall_status"],
                        datetime.fromisoformat(validated["calculated_at"]),
                        Jsonb(validated["input_report_calculation_ids"]),
                        Jsonb(validated["input_report_document_sha256"]),
                        Jsonb(validated["objectives"]),
                        validated["input_rows_scanned"],
                        False,
                        False,
                        False,
                        False,
                        Jsonb(validated),
                        document_sha256,
                    ),
                )
                inserted = cursor.fetchone()
                created = inserted is not None
                cursor.execute(
                    """
                    SELECT document_sha256, recorded_at
                    FROM risk_platform.operational_service_level_objective_reports
                    WHERE calculation_id = %s
                    """,
                    (validated["calculation_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "operational objective report is unavailable after insert"
                    )
                if stored[0] != document_sha256:
                    raise ValidationError(
                        "calculation_id already exists with different objective content"
                    )
                if not isinstance(stored[1], datetime):
                    raise StorageError(
                        "stored operational objective timestamp is incompatible"
                    )
                recorded_at = stored[1].astimezone(timezone.utc)
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("operational objective database operation failed") from None
    if recorded_at is None:  # pragma: no cover - guarded above.
        raise StorageError("operational objective record result is unavailable")
    return {
        "calculation_id": validated["calculation_id"],
        "model_version": MODEL_VERSION,
        "objective_policy_id": validated["objective_policy_id"],
        "through_session": validated["through_session"],
        "history_status": validated["history_status"],
        "overall_status": validated["overall_status"],
        "document_sha256": document_sha256,
        "recorded_at": recorded_at.isoformat(),
        "created": created,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record one append-only operational SLO objective report."
    )
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = record_operational_objective_report(
            dsn=args.dsn,
            report=read_objective_report(args.report),
        )
    except ValidationError:
        print(
            "Operational objective recording failed: report evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational objective recording failed: file or PostgreSQL operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational objective recording failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
