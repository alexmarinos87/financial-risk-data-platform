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

from src.analytics.operational_service_levels import (
    METRIC_NAMES,
    MODEL_VERSION,
    STATUS_RANK,
)
from src.common.exceptions import StorageError, ValidationError
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_REPORT_BYTES = 1_000_000
CALCULATION_ID_PATTERN = re.compile(
    r"^operational-service-levels-v1-report-[0-9a-f]{24}$"
)
POLICY_FINGERPRINT_PATTERN = re.compile(
    r"^operational-slo-policy-[0-9a-f]{24}$"
)
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
EXPECTED_UNITS = {
    "schedule_lag_sessions": "sessions",
    "market_freshness_exception_count": "constituents",
    "notification_retry_exhausted_count": "events",
    "notification_oldest_dead_letter_age_seconds": "seconds",
}
REPORT_KEYS = frozenset(
    {
        "calculation_id",
        "model_version",
        "policy_id",
        "policy_fingerprint",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "as_of",
        "latest_expected_session",
        "schedule_checkpoint",
        "expected_constituent_count",
        "freshness_exceptions",
        "notification_retry_exhausted_events",
        "maximum_notification_attempts",
        "overall_status",
        "metrics",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_id",
        "mandate_fingerprint",
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
    }
)
METRIC_KEYS = frozenset(
    {
        "metric_name",
        "observed_value",
        "unit",
        "warning_threshold",
        "critical_threshold",
        "status",
        "reason",
    }
)


def _safe_text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _fingerprint(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    return value


def _aware_utc(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValidationError(f"{label} must be an ISO-8601 timestamp") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _calendar_date(value: Any, label: str) -> date:
    if not isinstance(value, str):
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise ValidationError(f"{label} must use YYYY-MM-DD") from None
    if value != parsed.isoformat():
        raise ValidationError(f"{label} must use YYYY-MM-DD")
    return parsed


def _finite_nonnegative(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def _string_array(value: Any, label: str) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError(f"{label} must be a JSON array")
    parsed: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item or len(item) > 256:
            raise ValidationError(f"{label} must contain bounded text values")
        if item != item.strip() or any(ord(character) < 32 for character in item):
            raise ValidationError(f"{label} must contain canonical text values")
        parsed.append(item)
    if len(set(parsed)) != len(parsed):
        raise ValidationError(f"{label} must not contain duplicates")
    return parsed


def _validate_metrics(value: Any) -> tuple[list[dict[str, Any]], str]:
    if not isinstance(value, list) or len(value) != len(METRIC_NAMES):
        raise ValidationError("metrics must contain the four supported indicators")
    metrics: list[dict[str, Any]] = []
    names: list[str] = []
    statuses: list[str] = []
    for raw_metric in value:
        if not isinstance(raw_metric, Mapping) or set(raw_metric) != METRIC_KEYS:
            raise ValidationError("each metric must match the strict metric contract")
        name = raw_metric.get("metric_name")
        if name not in METRIC_NAMES or not isinstance(name, str):
            raise ValidationError("metric_name is unsupported")
        if raw_metric.get("unit") != EXPECTED_UNITS[name]:
            raise ValidationError("metric unit does not match the indicator")
        warning = _finite_nonnegative(
            raw_metric.get("warning_threshold"),
            f"{name}.warning_threshold",
        )
        critical = _finite_nonnegative(
            raw_metric.get("critical_threshold"),
            f"{name}.critical_threshold",
        )
        if critical <= warning:
            raise ValidationError("metric critical threshold must exceed warning")
        observed_raw = raw_metric.get("observed_value")
        reason = raw_metric.get("reason")
        if observed_raw is None:
            if name != "schedule_lag_sessions" or reason != "checkpoint_missing":
                raise ValidationError(
                    "only a missing schedule checkpoint may have no observed value"
                )
            expected_status = "critical"
            observed: float | None = None
        else:
            observed = _finite_nonnegative(observed_raw, f"{name}.observed_value")
            if reason is not None:
                raise ValidationError("metrics with values must not carry a reason")
            if observed >= critical:
                expected_status = "critical"
            elif observed >= warning:
                expected_status = "warning"
            else:
                expected_status = "ok"
        if raw_metric.get("status") != expected_status:
            raise ValidationError("metric status does not match its value and thresholds")
        metric = {
            "metric_name": name,
            "observed_value": observed,
            "unit": EXPECTED_UNITS[name],
            "warning_threshold": warning,
            "critical_threshold": critical,
            "status": expected_status,
            "reason": reason,
        }
        metrics.append(metric)
        names.append(name)
        statuses.append(expected_status)
    if tuple(names) != METRIC_NAMES:
        raise ValidationError("metrics must use the canonical indicator order")
    overall = max(statuses, key=STATUS_RANK.__getitem__)
    return metrics, overall


def validate_operational_service_level_report(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != REPORT_KEYS:
        raise ValidationError("operational service-level report has an invalid shape")
    calculation_id = payload.get("calculation_id")
    if not isinstance(calculation_id, str) or not CALCULATION_ID_PATTERN.fullmatch(
        calculation_id
    ):
        raise ValidationError("calculation_id is incompatible")
    if payload.get("model_version") != MODEL_VERSION:
        raise ValidationError("operational service-level model version is unsupported")
    policy_fingerprint = payload.get("policy_fingerprint")
    if not isinstance(policy_fingerprint, str) or not POLICY_FINGERPRINT_PATTERN.fullmatch(
        policy_fingerprint
    ):
        raise ValidationError("policy_fingerprint is incompatible")
    as_of = _aware_utc(payload.get("as_of"), "as_of")
    latest_expected_session = _calendar_date(
        payload.get("latest_expected_session"),
        "latest_expected_session",
    )
    checkpoint_raw = payload.get("schedule_checkpoint")
    checkpoint = (
        None
        if checkpoint_raw is None
        else _calendar_date(checkpoint_raw, "schedule_checkpoint")
    )
    if checkpoint is not None and checkpoint > latest_expected_session:
        raise ValidationError("schedule checkpoint exceeds the latest expected session")
    expected_count = payload.get("expected_constituent_count")
    if type(expected_count) is not int or not 1 <= expected_count <= 50:
        raise ValidationError("expected_constituent_count must be between 1 and 50")
    attempts = payload.get("maximum_notification_attempts")
    if type(attempts) is not int or not 1 <= attempts <= 10:
        raise ValidationError("maximum_notification_attempts must be between 1 and 10")
    metrics, expected_overall = _validate_metrics(payload.get("metrics"))
    if payload.get("overall_status") != expected_overall:
        raise ValidationError("overall_status does not match metric severity")
    for flag in (
        "provider_request_performed",
        "external_delivery_performed",
        "cloud_schedule_activated",
    ):
        if payload.get(flag) is not False:
            raise ValidationError(f"{flag} must be false for report-only evidence")
    report = {
        "calculation_id": calculation_id,
        "model_version": MODEL_VERSION,
        "policy_id": _safe_text(payload.get("policy_id"), "policy_id"),
        "policy_fingerprint": policy_fingerprint,
        "schedule_id": _safe_text(payload.get("schedule_id"), "schedule_id"),
        "schedule_fingerprint": _fingerprint(
            payload.get("schedule_fingerprint"),
            "schedule_fingerprint",
        ),
        "calendar_id": _safe_text(payload.get("calendar_id"), "calendar_id"),
        "as_of": as_of.isoformat(),
        "latest_expected_session": latest_expected_session.isoformat(),
        "schedule_checkpoint": checkpoint.isoformat() if checkpoint is not None else None,
        "expected_constituent_count": expected_count,
        "freshness_exceptions": _string_array(
            payload.get("freshness_exceptions"),
            "freshness_exceptions",
        ),
        "notification_retry_exhausted_events": _string_array(
            payload.get("notification_retry_exhausted_events"),
            "notification_retry_exhausted_events",
        ),
        "maximum_notification_attempts": attempts,
        "overall_status": expected_overall,
        "metrics": metrics,
        "portfolio_id": _safe_text(payload.get("portfolio_id"), "portfolio_id"),
        "risk_limit_policy_id": _safe_text(
            payload.get("risk_limit_policy_id"),
            "risk_limit_policy_id",
        ),
        "mandate_id": _safe_text(payload.get("mandate_id"), "mandate_id"),
        "mandate_fingerprint": _fingerprint(
            payload.get("mandate_fingerprint"),
            "mandate_fingerprint",
        ),
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }
    return report


def canonical_report_bytes(report: Mapping[str, Any]) -> bytes:
    try:
        return json.dumps(
            report,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("operational service-level report is not canonical JSON") from None


def read_report(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise StorageError("operational service-level report must not be a symbolic link")
    if not path.is_file():
        raise StorageError("operational service-level report must be a regular file")
    try:
        if path.stat().st_size > MAX_REPORT_BYTES:
            raise StorageError("operational service-level report exceeds the byte limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except StorageError:
        raise
    except (OSError, ValueError):
        raise StorageError("operational service-level report could not be read") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational service-level report must be a JSON object")
    return validate_operational_service_level_report(payload)


def record_operational_service_level_report(
    *,
    dsn: str,
    report: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_operational_service_level_report(report)
    canonical = canonical_report_bytes(validated)
    document_sha256 = hashlib.sha256(canonical).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational service-level recording requires psycopg") from exc

    created = False
    recorded_at: datetime | None = None
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_platform.operational_service_level_reports (
                        calculation_id,
                        model_version,
                        policy_id,
                        policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_id,
                        mandate_fingerprint,
                        as_of,
                        latest_expected_session,
                        schedule_checkpoint,
                        expected_constituent_count,
                        freshness_exceptions,
                        notification_retry_exhausted_events,
                        maximum_notification_attempts,
                        overall_status,
                        metrics_json,
                        provider_request_performed,
                        external_delivery_performed,
                        cloud_schedule_activated,
                        report_json,
                        document_sha256
                    )
                    VALUES (
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
                        validated["policy_id"],
                        validated["policy_fingerprint"],
                        validated["schedule_id"],
                        validated["schedule_fingerprint"],
                        validated["calendar_id"],
                        validated["portfolio_id"],
                        validated["risk_limit_policy_id"],
                        validated["mandate_id"],
                        validated["mandate_fingerprint"],
                        datetime.fromisoformat(validated["as_of"]),
                        date.fromisoformat(validated["latest_expected_session"]),
                        (
                            date.fromisoformat(validated["schedule_checkpoint"])
                            if validated["schedule_checkpoint"] is not None
                            else None
                        ),
                        validated["expected_constituent_count"],
                        Jsonb(validated["freshness_exceptions"]),
                        Jsonb(validated["notification_retry_exhausted_events"]),
                        validated["maximum_notification_attempts"],
                        validated["overall_status"],
                        Jsonb(validated["metrics"]),
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
                    FROM risk_platform.operational_service_level_reports
                    WHERE calculation_id = %s
                    """,
                    (validated["calculation_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "operational service-level report is unavailable after insert"
                    )
                if stored[0] != document_sha256:
                    raise ValidationError(
                        "calculation_id already exists with different report content"
                    )
                if not isinstance(stored[1], datetime):
                    raise StorageError(
                        "stored operational service-level timestamp is incompatible"
                    )
                recorded_at = stored[1].astimezone(timezone.utc)
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError(
            "operational service-level database operation failed"
        ) from None
    if recorded_at is None:  # pragma: no cover - guarded above.
        raise StorageError("operational service-level record result is unavailable")
    return {
        "calculation_id": validated["calculation_id"],
        "model_version": validated["model_version"],
        "policy_id": validated["policy_id"],
        "policy_fingerprint": validated["policy_fingerprint"],
        "schedule_id": validated["schedule_id"],
        "portfolio_id": validated["portfolio_id"],
        "as_of": validated["as_of"],
        "overall_status": validated["overall_status"],
        "document_sha256": document_sha256,
        "recorded_at": recorded_at.isoformat(),
        "created": created,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record one append-only operational service-level report."
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
        report = read_report(args.report)
        summary = record_operational_service_level_report(
            dsn=args.dsn,
            report=report,
        )
    except ValidationError:
        print(
            "Operational service-level recording failed: report evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational service-level recording failed: file or PostgreSQL operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational service-level recording failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
