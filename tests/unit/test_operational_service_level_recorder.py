from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src.analytics.operational_service_levels import (
    evaluate_operational_service_levels,
    parse_operational_service_level_policy,
)
from src.common.exceptions import StorageError, ValidationError
from src.warehouse.operational_service_level_recorder import (
    canonical_report_bytes,
    read_report,
    validate_operational_service_level_report,
)


def _report() -> dict[str, object]:
    policy = parse_operational_service_level_policy(
        {
            "policies": {
                "us-tech-local": {
                    "schedule_id": "us-tech-local",
                    "metrics": {
                        "schedule_lag_sessions": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "market_freshness_exception_count": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "notification_retry_exhausted_count": {
                            "warning": 1,
                            "critical": 2,
                        },
                        "notification_oldest_dead_letter_age_seconds": {
                            "warning": 900,
                            "critical": 3600,
                        },
                    },
                }
            }
        },
        "us-tech-local",
    )
    output = evaluate_operational_service_levels(
        policy=policy,
        as_of=datetime(2026, 4, 1, 12, tzinfo=timezone.utc),
        schedule_fingerprint="local-portfolio-schedule-v1-example",
        latest_expected_session=date(2026, 3, 31),
        schedule_checkpoint=date(2026, 3, 31),
        schedule_lag_sessions=0,
        expected_constituents=(
            "alpha_vantage:AAPL",
            "alpha_vantage:MSFT",
        ),
        calendar_id="XNYS",
        freshness_records=[
            {
                "source": "alpha_vantage",
                "symbol": symbol,
                "calendar_id": "XNYS",
                "as_of_date": date(2026, 3, 31),
                "freshness_status": "current",
                "trailing_missing_session_count": 0,
            }
            for symbol in ("AAPL", "MSFT")
        ],
        notification_records=[],
        maximum_notification_attempts=3,
    )
    return {
        **dict(output.report),
        "portfolio_id": "us-tech-equal",
        "risk_limit_policy_id": "us-tech-standard",
        "mandate_id": "us-tech-2026",
        "mandate_fingerprint": "portfolio-mandate-v1-example",
        "provider_request_performed": False,
        "external_delivery_performed": False,
        "cloud_schedule_activated": False,
    }


def test_report_validation_is_canonical_and_deterministic() -> None:
    first = validate_operational_service_level_report(_report())
    second = validate_operational_service_level_report(dict(reversed(list(_report().items()))))

    assert first == second
    assert canonical_report_bytes(first) == canonical_report_bytes(second)
    assert first["overall_status"] == "ok"
    assert [metric["metric_name"] for metric in first["metrics"]] == [
        "schedule_lag_sessions",
        "market_freshness_exception_count",
        "notification_retry_exhausted_count",
        "notification_oldest_dead_letter_age_seconds",
    ]


def test_report_validation_rejects_status_and_side_effect_tampering() -> None:
    report = _report()
    report["overall_status"] = "critical"
    with pytest.raises(ValidationError, match="overall_status"):
        validate_operational_service_level_report(report)

    report = _report()
    report["external_delivery_performed"] = True
    with pytest.raises(ValidationError, match="must be false"):
        validate_operational_service_level_report(report)


def test_report_validation_rejects_noncanonical_metric_order() -> None:
    report = _report()
    metrics = list(report["metrics"])  # type: ignore[arg-type]
    report["metrics"] = list(reversed(metrics))
    with pytest.raises(ValidationError, match="canonical indicator order"):
        validate_operational_service_level_report(report)


def test_read_report_rejects_symbolic_link_and_accepts_regular_json(
    tmp_path: Path,
) -> None:
    path = tmp_path / "report.json"
    path.write_text(json.dumps(_report()), encoding="utf-8")
    assert read_report(path)["calculation_id"] == _report()["calculation_id"]

    link = tmp_path / "report-link.json"
    try:
        link.symlink_to(path)
    except OSError:
        pytest.skip("symbolic links are unavailable")
    with pytest.raises(StorageError, match="symbolic link"):
        read_report(link)
