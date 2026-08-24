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

from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_readiness_gate import MODEL_VERSION
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MAX_DECISION_BYTES = 1_000_000
MAX_REPORT_AGE_SECONDS = 7 * 24 * 60 * 60
DECISION_ID_PATTERN = re.compile(
    r"^operational-readiness-gate-v1-decision-[0-9a-f]{24}$"
)
GATE_FINGERPRINT_PATTERN = re.compile(
    r"^operational-readiness-gate-[0-9a-f]{24}$"
)
OPERATIONAL_POLICY_FINGERPRINT_PATTERN = re.compile(
    r"^operational-slo-policy-[0-9a-f]{24}$"
)
REPORT_ID_PATTERN = re.compile(
    r"^operational-service-levels-v1-report-[0-9a-f]{24}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
REASON_ORDER = (
    "report_missing",
    "report_timestamp_future",
    "report_age_exceeds_limit",
    "report_session_mismatch",
    "report_status_critical",
    "report_status_warning",
)
DECISION_KEYS = frozenset(
    {
        "decision_id",
        "model_version",
        "gate_id",
        "gate_fingerprint",
        "operational_policy_id",
        "operational_policy_fingerprint",
        "schedule_id",
        "schedule_fingerprint",
        "calendar_id",
        "portfolio_id",
        "risk_limit_policy_id",
        "mandate_fingerprint",
        "evaluated_at",
        "latest_expected_session",
        "max_report_age_seconds",
        "allow_warning",
        "report_calculation_id",
        "report_document_sha256",
        "report_as_of",
        "report_latest_expected_session",
        "report_status",
        "report_age_seconds",
        "report_future_seconds",
        "decision",
        "reasons",
        "schedule_executed",
        "provider_request_performed",
        "notification_delivery_performed",
        "cloud_schedule_activated",
    }
)


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _fingerprint(
    value: Any,
    label: str,
    pattern: re.Pattern[str] | None = None,
) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    if pattern is not None and not pattern.fullmatch(value):
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


def _optional_timestamp(value: Any, label: str) -> datetime | None:
    return None if value is None else _aware_utc(value, label)


def _optional_date(value: Any, label: str) -> date | None:
    return None if value is None else _calendar_date(value, label)


def _optional_pattern(
    value: Any,
    label: str,
    pattern: re.Pattern[str],
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not pattern.fullmatch(value):
        raise ValidationError(f"{label} is incompatible")
    return value


def _optional_nonnegative(value: Any, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{label} must be a finite non-negative number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed < 0:
        raise ValidationError(f"{label} must be a finite non-negative number")
    return parsed


def _reasons(value: Any) -> list[str]:
    if not isinstance(value, list):
        raise ValidationError("reasons must be an array")
    if any(not isinstance(item, str) or item not in REASON_ORDER for item in value):
        raise ValidationError("reasons contains an unsupported value")
    if len(value) != len(set(value)):
        raise ValidationError("reasons must not contain duplicates")
    expected = [reason for reason in REASON_ORDER if reason in value]
    if value != expected:
        raise ValidationError("reasons must use canonical order")
    return list(value)


def _decision_id_payload(decision: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "calendar_id": decision["calendar_id"],
        "decision": decision["decision"],
        "evaluated_at": decision["evaluated_at"],
        "gate_fingerprint": decision["gate_fingerprint"],
        "latest_expected_session": decision["latest_expected_session"],
        "mandate_fingerprint": decision["mandate_fingerprint"],
        "operational_policy_fingerprint": decision[
            "operational_policy_fingerprint"
        ],
        "portfolio_id": decision["portfolio_id"],
        "reasons": decision["reasons"],
        "report_calculation_id": decision["report_calculation_id"],
        "report_document_sha256": decision["report_document_sha256"],
        "risk_limit_policy_id": decision["risk_limit_policy_id"],
        "schedule_fingerprint": decision["schedule_fingerprint"],
        "schedule_id": decision["schedule_id"],
    }


def _expected_decision_id(decision: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(
        json.dumps(
            _decision_id_payload(decision),
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return f"{MODEL_VERSION}-decision-{digest}"


def validate_operational_readiness_decision(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != DECISION_KEYS:
        raise ValidationError("operational readiness decision has an invalid shape")
    if payload.get("model_version") != MODEL_VERSION:
        raise ValidationError("operational readiness model version is unsupported")
    decision_id = payload.get("decision_id")
    if not isinstance(decision_id, str) or not DECISION_ID_PATTERN.fullmatch(
        decision_id
    ):
        raise ValidationError("decision_id is incompatible")

    evaluated_at = _aware_utc(payload.get("evaluated_at"), "evaluated_at")
    expected_session = _calendar_date(
        payload.get("latest_expected_session"),
        "latest_expected_session",
    )
    max_age = payload.get("max_report_age_seconds")
    if type(max_age) is not int or not 1 <= max_age <= MAX_REPORT_AGE_SECONDS:
        raise ValidationError("max_report_age_seconds is outside the supported range")
    allow_warning = payload.get("allow_warning")
    if type(allow_warning) is not bool:
        raise ValidationError("allow_warning must be boolean")
    decision = payload.get("decision")
    if decision not in {"allow", "block"}:
        raise ValidationError("decision must be allow or block")
    reasons = _reasons(payload.get("reasons"))

    report_calculation_id = _optional_pattern(
        payload.get("report_calculation_id"),
        "report_calculation_id",
        REPORT_ID_PATTERN,
    )
    report_document_sha256 = _optional_pattern(
        payload.get("report_document_sha256"),
        "report_document_sha256",
        SHA256_PATTERN,
    )
    report_as_of = _optional_timestamp(payload.get("report_as_of"), "report_as_of")
    report_session = _optional_date(
        payload.get("report_latest_expected_session"),
        "report_latest_expected_session",
    )
    report_status = payload.get("report_status")
    if report_status is not None and report_status not in {"ok", "warning", "critical"}:
        raise ValidationError("report_status is incompatible")
    report_age_seconds = _optional_nonnegative(
        payload.get("report_age_seconds"),
        "report_age_seconds",
    )
    report_future_seconds = _optional_nonnegative(
        payload.get("report_future_seconds"),
        "report_future_seconds",
    )

    report_fields = (
        report_calculation_id,
        report_document_sha256,
        report_as_of,
        report_session,
        report_status,
        report_age_seconds,
        report_future_seconds,
    )
    expected_reasons: list[str] = []
    if report_calculation_id is None:
        if any(value is not None for value in report_fields[1:]):
            raise ValidationError("missing report evidence must use null report fields")
        expected_reasons.append("report_missing")
    else:
        if any(value is None for value in report_fields[1:]):
            raise ValidationError("retained report evidence must be complete")
        assert report_as_of is not None
        assert report_session is not None
        assert report_age_seconds is not None
        assert report_future_seconds is not None
        delta_seconds = (evaluated_at - report_as_of).total_seconds()
        expected_age = max(0.0, delta_seconds)
        expected_future = max(0.0, -delta_seconds)
        if not math.isclose(
            report_age_seconds,
            expected_age,
            rel_tol=0.0,
            abs_tol=1e-9,
        ) or not math.isclose(
            report_future_seconds,
            expected_future,
            rel_tol=0.0,
            abs_tol=1e-9,
        ):
            raise ValidationError("report age evidence does not reconcile")
        if delta_seconds < 0:
            expected_reasons.append("report_timestamp_future")
        elif expected_age > max_age:
            expected_reasons.append("report_age_exceeds_limit")
        if report_session != expected_session:
            expected_reasons.append("report_session_mismatch")
        if report_status == "critical":
            expected_reasons.append("report_status_critical")
        elif report_status == "warning" and not allow_warning:
            expected_reasons.append("report_status_warning")

    if reasons != expected_reasons:
        raise ValidationError("reasons do not match retained readiness evidence")
    expected_decision = "allow" if not expected_reasons else "block"
    if decision != expected_decision:
        raise ValidationError("decision does not match readiness reasons")
    for flag in (
        "schedule_executed",
        "provider_request_performed",
        "notification_delivery_performed",
        "cloud_schedule_activated",
    ):
        if payload.get(flag) is not False:
            raise ValidationError(f"{flag} must be false for read-only evidence")

    validated = {
        "decision_id": decision_id,
        "model_version": MODEL_VERSION,
        "gate_id": _safe_segment(payload.get("gate_id"), "gate_id"),
        "gate_fingerprint": _fingerprint(
            payload.get("gate_fingerprint"),
            "gate_fingerprint",
            GATE_FINGERPRINT_PATTERN,
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
        "mandate_fingerprint": _fingerprint(
            payload.get("mandate_fingerprint"),
            "mandate_fingerprint",
        ),
        "evaluated_at": evaluated_at.isoformat(),
        "latest_expected_session": expected_session.isoformat(),
        "max_report_age_seconds": max_age,
        "allow_warning": allow_warning,
        "report_calculation_id": report_calculation_id,
        "report_document_sha256": report_document_sha256,
        "report_as_of": report_as_of.isoformat() if report_as_of is not None else None,
        "report_latest_expected_session": (
            report_session.isoformat() if report_session is not None else None
        ),
        "report_status": report_status,
        "report_age_seconds": report_age_seconds,
        "report_future_seconds": report_future_seconds,
        "decision": expected_decision,
        "reasons": expected_reasons,
        "schedule_executed": False,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }
    if validated["decision_id"] != _expected_decision_id(validated):
        raise ValidationError("decision_id does not match canonical readiness evidence")
    return validated


def canonical_operational_readiness_decision_bytes(
    decision: Mapping[str, Any],
) -> bytes:
    try:
        return json.dumps(
            decision,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise ValidationError("operational readiness decision is not canonical JSON") from None


def read_operational_readiness_decision(path: Path) -> dict[str, Any]:
    if path.is_symlink():
        raise StorageError("operational readiness decision must not be a symbolic link")
    if not path.is_file():
        raise StorageError("operational readiness decision must be a regular file")
    try:
        if path.stat().st_size > MAX_DECISION_BYTES:
            raise StorageError("operational readiness decision exceeds the byte limit")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except StorageError:
        raise
    except (OSError, ValueError):
        raise StorageError("operational readiness decision could not be read") from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational readiness decision must be a JSON object")
    return validate_operational_readiness_decision(payload)


def record_operational_readiness_decision(
    *,
    dsn: str,
    decision: Mapping[str, Any],
) -> dict[str, Any]:
    validated = validate_operational_readiness_decision(decision)
    canonical = canonical_operational_readiness_decision_bytes(validated)
    document_sha256 = hashlib.sha256(canonical).hexdigest()
    try:
        import psycopg
        from psycopg.types.json import Jsonb
    except ImportError as exc:  # pragma: no cover - dependency is required in CI.
        raise RuntimeError("Operational readiness recording requires psycopg") from exc

    created = False
    recorded_at: datetime | None = None
    try:
        with psycopg.connect(dsn) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO risk_platform.operational_readiness_decisions (
                        decision_id,
                        model_version,
                        gate_id,
                        gate_fingerprint,
                        operational_policy_id,
                        operational_policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_fingerprint,
                        evaluated_at,
                        latest_expected_session,
                        max_report_age_seconds,
                        allow_warning,
                        report_calculation_id,
                        report_document_sha256,
                        report_as_of,
                        report_latest_expected_session,
                        report_status,
                        report_age_seconds,
                        report_future_seconds,
                        decision,
                        reasons,
                        schedule_executed,
                        provider_request_performed,
                        notification_delivery_performed,
                        cloud_schedule_activated,
                        decision_json,
                        document_sha256
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s
                    )
                    ON CONFLICT (decision_id) DO NOTHING
                    RETURNING recorded_at
                    """,
                    (
                        validated["decision_id"],
                        validated["model_version"],
                        validated["gate_id"],
                        validated["gate_fingerprint"],
                        validated["operational_policy_id"],
                        validated["operational_policy_fingerprint"],
                        validated["schedule_id"],
                        validated["schedule_fingerprint"],
                        validated["calendar_id"],
                        validated["portfolio_id"],
                        validated["risk_limit_policy_id"],
                        validated["mandate_fingerprint"],
                        datetime.fromisoformat(validated["evaluated_at"]),
                        date.fromisoformat(validated["latest_expected_session"]),
                        validated["max_report_age_seconds"],
                        validated["allow_warning"],
                        validated["report_calculation_id"],
                        validated["report_document_sha256"],
                        (
                            datetime.fromisoformat(validated["report_as_of"])
                            if validated["report_as_of"] is not None
                            else None
                        ),
                        (
                            date.fromisoformat(
                                validated["report_latest_expected_session"]
                            )
                            if validated["report_latest_expected_session"] is not None
                            else None
                        ),
                        validated["report_status"],
                        validated["report_age_seconds"],
                        validated["report_future_seconds"],
                        validated["decision"],
                        Jsonb(validated["reasons"]),
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
                    FROM risk_platform.operational_readiness_decisions
                    WHERE decision_id = %s
                    """,
                    (validated["decision_id"],),
                )
                stored = cursor.fetchone()
                if stored is None:
                    raise StorageError(
                        "operational readiness decision is unavailable after insert"
                    )
                if stored[0] != document_sha256:
                    raise ValidationError(
                        "decision_id already exists with different readiness content"
                    )
                if not isinstance(stored[1], datetime):
                    raise StorageError(
                        "stored operational readiness timestamp is incompatible"
                    )
                recorded_at = stored[1].astimezone(timezone.utc)
            connection.commit()
    except (ValidationError, StorageError):
        raise
    except Exception:
        raise StorageError("operational readiness database operation failed") from None
    if recorded_at is None:  # pragma: no cover - guarded above.
        raise StorageError("operational readiness record result is unavailable")
    return {
        "decision_id": validated["decision_id"],
        "model_version": MODEL_VERSION,
        "gate_id": validated["gate_id"],
        "latest_expected_session": validated["latest_expected_session"],
        "decision": validated["decision"],
        "reason_count": len(validated["reasons"]),
        "document_sha256": document_sha256,
        "recorded_at": recorded_at.isoformat(),
        "created": created,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Record one append-only operational readiness decision."
    )
    parser.add_argument("--decision", type=Path, required=True)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        summary = record_operational_readiness_decision(
            dsn=args.dsn,
            decision=read_operational_readiness_decision(args.decision),
        )
    except ValidationError:
        print(
            "Operational readiness recording failed: decision evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational readiness recording failed: file or PostgreSQL operation failed",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational readiness recording failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
