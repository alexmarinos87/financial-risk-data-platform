from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from src.analytics.market_calendar import MarketCalendar, load_market_calendar
from src.analytics.operational_service_levels import (
    OperationalServiceLevelPolicy,
    load_operational_service_level_policy,
)
from src.analytics.portfolio_mandates import (
    PortfolioMandate,
    load_portfolio_mandate,
)
from src.common.config import load_yaml
from src.common.exceptions import StorageError, ValidationError
from src.orchestration.run_local_portfolio_schedule import (
    LocalPortfolioSchedule,
    load_local_portfolio_schedule,
)
from src.warehouse.postgres_loader import DEFAULT_POSTGRES_DSN

MODEL_VERSION = "operational-readiness-gate-v1"
MAX_REPORT_AGE_SECONDS = 7 * 24 * 60 * 60
CALCULATION_ID_PATTERN = re.compile(
    r"^operational-service-levels-v1-report-[0-9a-f]{24}$"
)
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SAFE_SEGMENT_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


@dataclass(frozen=True, slots=True)
class OperationalReadinessGatePolicy:
    gate_id: str
    operational_policy_id: str
    max_report_age_seconds: int
    allow_warning: bool

    @property
    def fingerprint(self) -> str:
        payload = {
            "allow_warning": self.allow_warning,
            "gate_id": self.gate_id,
            "max_report_age_seconds": self.max_report_age_seconds,
            "model_version": MODEL_VERSION,
            "operational_policy_id": self.operational_policy_id,
        }
        digest = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(
                "utf-8"
            )
        ).hexdigest()[:24]
        return f"operational-readiness-gate-{digest}"


GatePolicyLoader = Callable[[Path, str], OperationalReadinessGatePolicy]
OperationalPolicyLoader = Callable[[Path, str], OperationalServiceLevelPolicy]
ScheduleLoader = Callable[[Path, str], LocalPortfolioSchedule]
CalendarLoader = Callable[[Path, str], MarketCalendar]
MandateLoader = Callable[[Path, str, date], PortfolioMandate]
ReportReader = Callable[..., Mapping[str, Any] | None]


def _safe_segment(value: Any, label: str) -> str:
    if not isinstance(value, str) or not SAFE_SEGMENT_PATTERN.fullmatch(value):
        raise ValidationError(f"{label} must be one safe text segment")
    return value


def _bounded_fingerprint(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValidationError(f"{label} must be non-empty bounded text")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise ValidationError(f"{label} must be canonical bounded text")
    return value


def _positive_age_limit(value: Any) -> int:
    if type(value) is not int or not 1 <= value <= MAX_REPORT_AGE_SECONDS:
        raise ValidationError(
            "max_report_age_seconds must be an integer between 1 and "
            f"{MAX_REPORT_AGE_SECONDS}"
        )
    return value


def parse_operational_readiness_gate_policy(
    payload: Mapping[str, Any],
    gate_id: str,
) -> OperationalReadinessGatePolicy:
    if not isinstance(payload, Mapping):
        raise ValidationError("operational readiness configuration must be a mapping")
    gate_id = _safe_segment(gate_id, "gate_id")
    gates = payload.get("gates")
    if not isinstance(gates, Mapping):
        raise ValidationError("operational readiness configuration must define gates")
    candidate = gates.get(gate_id)
    if not isinstance(candidate, Mapping) or set(candidate) != {
        "operational_policy_id",
        "max_report_age_seconds",
        "allow_warning",
    }:
        raise ValidationError(
            f"operational readiness gate '{gate_id}' has an invalid contract"
        )
    allow_warning = candidate.get("allow_warning")
    if type(allow_warning) is not bool:
        raise ValidationError("allow_warning must be boolean")
    return OperationalReadinessGatePolicy(
        gate_id=gate_id,
        operational_policy_id=_safe_segment(
            candidate.get("operational_policy_id"),
            "operational_policy_id",
        ),
        max_report_age_seconds=_positive_age_limit(
            candidate.get("max_report_age_seconds")
        ),
        allow_warning=allow_warning,
    )


def load_operational_readiness_gate_policy(
    path: Path,
    gate_id: str,
) -> OperationalReadinessGatePolicy:
    try:
        payload = load_yaml(path)
    except Exception:
        raise ValidationError(
            "operational readiness configuration could not be loaded"
        ) from None
    if not isinstance(payload, Mapping):
        raise ValidationError("operational readiness configuration must be a mapping")
    return parse_operational_readiness_gate_policy(payload, gate_id)


def _aware_utc(value: Any, label: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            raise ValidationError(f"{label} must be an ISO-8601 timestamp") from None
    else:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValidationError(f"{label} must be a timezone-aware timestamp")
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


def _quote_identifier(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValidationError("schema_name must be non-empty text")
    return '"' + value.replace('"', '""') + '"'


def read_latest_operational_report(
    *,
    dsn: str,
    operational_policy_id: str,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
    schema_name: str = "risk_platform",
) -> Mapping[str, Any] | None:
    if not isinstance(dsn, str) or not dsn.strip():
        raise ValidationError("PostgreSQL DSN must be non-empty text")
    schema = _quote_identifier(schema_name)
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise RuntimeError("Operational readiness requires psycopg") from exc

    query = f"""
        SELECT
            calculation_id,
            policy_id,
            policy_fingerprint,
            schedule_id,
            schedule_fingerprint,
            calendar_id,
            portfolio_id,
            risk_limit_policy_id,
            mandate_fingerprint,
            as_of,
            latest_expected_session,
            overall_status,
            document_sha256
        FROM {schema}.latest_operational_service_level_reports
        WHERE policy_id = %s
          AND policy_fingerprint = %s
          AND schedule_id = %s
          AND schedule_fingerprint = %s
          AND calendar_id = %s
          AND portfolio_id = %s
          AND risk_limit_policy_id = %s
          AND mandate_fingerprint = %s
        ORDER BY as_of DESC, calculation_id DESC
        LIMIT 2
    """
    try:
        with psycopg.connect(dsn, row_factory=dict_row) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    query,
                    (
                        operational_policy_id,
                        operational_policy_fingerprint,
                        schedule_id,
                        schedule_fingerprint,
                        calendar_id,
                        portfolio_id,
                        risk_limit_policy_id,
                        mandate_fingerprint,
                    ),
                )
                rows = [dict(row) for row in cursor.fetchall()]
    except Exception:
        raise StorageError(
            "Unable to read the latest operational service-level report"
        ) from None
    if len(rows) > 1:
        raise StorageError(
            "latest operational service-level report grain is not unique"
        )
    return rows[0] if rows else None


def _validate_report(
    report: Mapping[str, Any],
    *,
    operational_policy_id: str,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
) -> dict[str, Any]:
    calculation_id = report.get("calculation_id")
    if not isinstance(calculation_id, str) or not CALCULATION_ID_PATTERN.fullmatch(
        calculation_id
    ):
        raise ValidationError("latest operational report calculation ID is invalid")
    document_sha256 = report.get("document_sha256")
    if not isinstance(document_sha256, str) or not SHA256_PATTERN.fullmatch(
        document_sha256
    ):
        raise ValidationError("latest operational report document digest is invalid")
    expected_text = {
        "policy_id": operational_policy_id,
        "policy_fingerprint": operational_policy_fingerprint,
        "schedule_id": schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "portfolio_id": portfolio_id,
        "risk_limit_policy_id": risk_limit_policy_id,
        "mandate_fingerprint": mandate_fingerprint,
    }
    for key, expected in expected_text.items():
        if report.get(key) != expected:
            raise ValidationError(
                f"latest operational report {key} does not match current configuration"
            )
    status = report.get("overall_status")
    if status not in {"ok", "warning", "critical"}:
        raise ValidationError("latest operational report status is invalid")
    return {
        "calculation_id": calculation_id,
        "document_sha256": document_sha256,
        "as_of": _aware_utc(report.get("as_of"), "report as_of"),
        "latest_expected_session": _calendar_date(
            report.get("latest_expected_session"),
            "report latest_expected_session",
        ),
        "overall_status": status,
    }


def evaluate_operational_readiness(
    *,
    gate_policy: OperationalReadinessGatePolicy,
    evaluated_at: datetime,
    latest_expected_session: date,
    operational_policy_fingerprint: str,
    schedule_id: str,
    schedule_fingerprint: str,
    calendar_id: str,
    portfolio_id: str,
    risk_limit_policy_id: str,
    mandate_fingerprint: str,
    report: Mapping[str, Any] | None,
) -> dict[str, Any]:
    evaluated_at = _aware_utc(evaluated_at, "evaluated_at")
    schedule_id = _safe_segment(schedule_id, "schedule_id")
    calendar_id = _safe_segment(calendar_id, "calendar_id")
    portfolio_id = _safe_segment(portfolio_id, "portfolio_id")
    risk_limit_policy_id = _safe_segment(
        risk_limit_policy_id,
        "risk_limit_policy_id",
    )
    operational_policy_fingerprint = _bounded_fingerprint(
        operational_policy_fingerprint,
        "operational_policy_fingerprint",
    )
    schedule_fingerprint = _bounded_fingerprint(
        schedule_fingerprint,
        "schedule_fingerprint",
    )
    mandate_fingerprint = _bounded_fingerprint(
        mandate_fingerprint,
        "mandate_fingerprint",
    )

    reasons: list[str] = []
    validated_report: dict[str, Any] | None = None
    report_age_seconds: float | None = None
    report_future_seconds: float | None = None
    if report is None:
        reasons.append("report_missing")
    else:
        validated_report = _validate_report(
            report,
            operational_policy_id=gate_policy.operational_policy_id,
            operational_policy_fingerprint=operational_policy_fingerprint,
            schedule_id=schedule_id,
            schedule_fingerprint=schedule_fingerprint,
            calendar_id=calendar_id,
            portfolio_id=portfolio_id,
            risk_limit_policy_id=risk_limit_policy_id,
            mandate_fingerprint=mandate_fingerprint,
        )
        delta_seconds = (
            evaluated_at - validated_report["as_of"]
        ).total_seconds()
        report_age_seconds = max(0.0, delta_seconds)
        report_future_seconds = max(0.0, -delta_seconds)
        if delta_seconds < 0:
            reasons.append("report_timestamp_future")
        elif report_age_seconds > gate_policy.max_report_age_seconds:
            reasons.append("report_age_exceeds_limit")
        if validated_report["latest_expected_session"] != latest_expected_session:
            reasons.append("report_session_mismatch")
        status = validated_report["overall_status"]
        if status == "critical":
            reasons.append("report_status_critical")
        elif status == "warning" and not gate_policy.allow_warning:
            reasons.append("report_status_warning")

    decision = "allow" if not reasons else "block"
    identity_payload = {
        "calendar_id": calendar_id,
        "decision": decision,
        "evaluated_at": evaluated_at.isoformat(),
        "gate_fingerprint": gate_policy.fingerprint,
        "latest_expected_session": latest_expected_session.isoformat(),
        "mandate_fingerprint": mandate_fingerprint,
        "operational_policy_fingerprint": operational_policy_fingerprint,
        "portfolio_id": portfolio_id,
        "reasons": reasons,
        "report_calculation_id": (
            validated_report["calculation_id"]
            if validated_report is not None
            else None
        ),
        "report_document_sha256": (
            validated_report["document_sha256"]
            if validated_report is not None
            else None
        ),
        "risk_limit_policy_id": risk_limit_policy_id,
        "schedule_fingerprint": schedule_fingerprint,
        "schedule_id": schedule_id,
    }
    digest = hashlib.sha256(
        json.dumps(
            identity_payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()[:24]
    return {
        "decision_id": f"{MODEL_VERSION}-decision-{digest}",
        "model_version": MODEL_VERSION,
        "gate_id": gate_policy.gate_id,
        "gate_fingerprint": gate_policy.fingerprint,
        "operational_policy_id": gate_policy.operational_policy_id,
        "operational_policy_fingerprint": operational_policy_fingerprint,
        "schedule_id": schedule_id,
        "schedule_fingerprint": schedule_fingerprint,
        "calendar_id": calendar_id,
        "portfolio_id": portfolio_id,
        "risk_limit_policy_id": risk_limit_policy_id,
        "mandate_fingerprint": mandate_fingerprint,
        "evaluated_at": evaluated_at.isoformat(),
        "latest_expected_session": latest_expected_session.isoformat(),
        "max_report_age_seconds": gate_policy.max_report_age_seconds,
        "allow_warning": gate_policy.allow_warning,
        "report_calculation_id": (
            validated_report["calculation_id"]
            if validated_report is not None
            else None
        ),
        "report_document_sha256": (
            validated_report["document_sha256"]
            if validated_report is not None
            else None
        ),
        "report_as_of": (
            validated_report["as_of"].isoformat()
            if validated_report is not None
            else None
        ),
        "report_latest_expected_session": (
            validated_report["latest_expected_session"].isoformat()
            if validated_report is not None
            else None
        ),
        "report_status": (
            validated_report["overall_status"]
            if validated_report is not None
            else None
        ),
        "report_age_seconds": report_age_seconds,
        "report_future_seconds": report_future_seconds,
        "decision": decision,
        "reasons": reasons,
        "schedule_executed": False,
        "provider_request_performed": False,
        "notification_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def run_operational_readiness_gate(
    *,
    gate_id: str,
    evaluated_at: datetime,
    dsn: str,
    gate_config_path: Path,
    operational_policy_config_path: Path,
    schedule_config_path: Path,
    calendar_config_path: Path,
    portfolio_config_path: Path,
    schema_name: str = "risk_platform",
    gate_policy_loader: GatePolicyLoader | None = None,
    operational_policy_loader: OperationalPolicyLoader | None = None,
    schedule_loader: ScheduleLoader | None = None,
    calendar_loader: CalendarLoader | None = None,
    mandate_loader: MandateLoader | None = None,
    report_reader: ReportReader | None = None,
) -> dict[str, Any]:
    evaluated_at = _aware_utc(evaluated_at, "evaluated_at")
    selected_gate_loader = (
        gate_policy_loader or load_operational_readiness_gate_policy
    )
    gate_policy = selected_gate_loader(gate_config_path, gate_id)
    selected_policy_loader = (
        operational_policy_loader or load_operational_service_level_policy
    )
    operational_policy = selected_policy_loader(
        operational_policy_config_path,
        gate_policy.operational_policy_id,
    )
    selected_schedule_loader = schedule_loader or load_local_portfolio_schedule
    schedule = selected_schedule_loader(
        schedule_config_path,
        operational_policy.schedule_id,
    )
    if schedule.schedule_id != operational_policy.schedule_id:
        raise ValidationError("operational policy and schedule do not align")
    selected_calendar_loader = calendar_loader or load_market_calendar
    calendar = selected_calendar_loader(calendar_config_path, schedule.calendar_id)
    latest_expected_session = calendar.latest_expected_session(evaluated_at.date())
    selected_mandate_loader = mandate_loader or load_portfolio_mandate
    mandate = selected_mandate_loader(
        portfolio_config_path,
        schedule.portfolio_id,
        latest_expected_session,
    )
    selected_report_reader = report_reader or read_latest_operational_report
    report = selected_report_reader(
        dsn=dsn,
        operational_policy_id=operational_policy.policy_id,
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        schema_name=schema_name,
    )
    return evaluate_operational_readiness(
        gate_policy=gate_policy,
        evaluated_at=evaluated_at,
        latest_expected_session=latest_expected_session,
        operational_policy_fingerprint=operational_policy.fingerprint,
        schedule_id=schedule.schedule_id,
        schedule_fingerprint=schedule.fingerprint,
        calendar_id=calendar.calendar_id,
        portfolio_id=schedule.portfolio_id,
        risk_limit_policy_id=schedule.policy_id,
        mandate_fingerprint=mandate.fingerprint,
        report=report,
    )


def _timestamp(value: str) -> datetime:
    try:
        return _aware_utc(value, "evaluated_at")
    except ValidationError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(
            json.dumps(summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    except OSError:
        temporary.unlink(missing_ok=True)
        raise StorageError("Unable to write operational readiness evidence") from None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate one read-only operational readiness gate."
    )
    parser.add_argument("--gate-id", required=True)
    parser.add_argument("--evaluated-at", required=True, type=_timestamp)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("WAREHOUSE_POSTGRES_DSN", DEFAULT_POSTGRES_DSN),
    )
    parser.add_argument(
        "--gate-config",
        type=Path,
        default=Path("config/operational_readiness_gates.yaml"),
    )
    parser.add_argument(
        "--operational-policy-config",
        type=Path,
        default=Path("config/operational_service_levels.yaml"),
    )
    parser.add_argument(
        "--schedule-config",
        type=Path,
        default=Path("config/local_portfolio_schedules.yaml"),
    )
    parser.add_argument(
        "--calendar-config",
        type=Path,
        default=Path("config/market_calendars.yaml"),
    )
    parser.add_argument(
        "--portfolio-config",
        type=Path,
        default=Path("config/portfolios.yaml"),
    )
    parser.add_argument("--schema", default="risk_platform")
    parser.add_argument("--summary-json", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = run_operational_readiness_gate(
            gate_id=args.gate_id,
            evaluated_at=args.evaluated_at,
            dsn=args.dsn,
            gate_config_path=args.gate_config,
            operational_policy_config_path=args.operational_policy_config,
            schedule_config_path=args.schedule_config,
            calendar_config_path=args.calendar_config,
            portfolio_config_path=args.portfolio_config,
            schema_name=args.schema,
        )
        if args.summary_json is not None:
            _write_summary(args.summary_json, result)
    except ValidationError:
        print(
            "Operational readiness failed: configuration or retained evidence was invalid",
            file=sys.stderr,
        )
        return 1
    except StorageError:
        print(
            "Operational readiness failed: PostgreSQL or local evidence could not be read",
            file=sys.stderr,
        )
        return 1
    except Exception:
        print(
            "Operational readiness failed: unexpected local failure",
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0 if result["decision"] == "allow" else 2


if __name__ == "__main__":
    raise SystemExit(main())
