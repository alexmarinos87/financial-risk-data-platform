from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from src.analytics.operational_service_level_objectives import (
    evaluate_operational_objectives,
    parse_operational_objective_policy,
)
from src.common.exceptions import ValidationError
from src.warehouse.operational_service_level_objective_recorder import (
    canonical_objective_report_bytes,
    validate_operational_objective_report,
)


def _policy():
    return parse_operational_objective_policy(
        {
            "objective_policies": {
                "demo": {
                    "operational_policy_id": "operations",
                    "window_sessions": 2,
                    "minimum_observations": 2,
                    "targets": {
                        "schedule_completion_attainment": {
                            "source_metric_name": "schedule_lag_sessions",
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "market_freshness_attainment": {
                            "source_metric_name": (
                                "market_freshness_exception_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "notification_retry_exhaustion_free_attainment": {
                            "source_metric_name": (
                                "notification_retry_exhausted_count"
                            ),
                            "success_threshold": 0,
                            "target_ratio": 1,
                        },
                        "notification_dead_letter_duration_attainment": {
                            "source_metric_name": (
                                "notification_oldest_dead_letter_age_seconds"
                            ),
                            "success_threshold": 900,
                            "target_ratio": 1,
                        },
                    },
                }
            }
        },
        "demo",
    )


def _source_report(day: int) -> dict[str, object]:
    names = (
        "schedule_lag_sessions",
        "market_freshness_exception_count",
        "notification_retry_exhausted_count",
        "notification_oldest_dead_letter_age_seconds",
    )
    units = ("sessions", "constituents", "events", "seconds")
    return {
        "calculation_id": (
            f"operational-service-levels-v1-report-{day:024x}"
        ),
        "policy_id": "operations",
        "policy_fingerprint": "operational-slo-policy-" + "a" * 24,
        "schedule_id": "schedule",
        "schedule_fingerprint": "schedule-fingerprint",
        "calendar_id": "XNYS",
        "portfolio_id": "portfolio",
        "risk_limit_policy_id": "risk-policy",
        "mandate_fingerprint": "mandate-fingerprint",
        "as_of": datetime(2026, 1, day, 12, tzinfo=timezone.utc),
        "latest_expected_session": date(2026, 1, day),
        "metrics_json": [
            {
                "metric_name": name,
                "observed_value": 0,
                "unit": unit,
                "warning_threshold": 1,
                "critical_threshold": 2,
                "status": "ok",
                "reason": None,
            }
            for name, unit in zip(names, units, strict=True)
        ],
        "document_sha256": f"{day:064x}",
    }


def _report() -> dict[str, object]:
    output = evaluate_operational_objectives(
        objective_policy=_policy(),
        through_session=date(2026, 1, 2),
        expected_sessions=(date(2026, 1, 1), date(2026, 1, 2)),
        operational_policy_fingerprint=(
            "operational-slo-policy-" + "a" * 24
        ),
        schedule_id="schedule",
        schedule_fingerprint="schedule-fingerprint",
        calendar_id="XNYS",
        portfolio_id="portfolio",
        risk_limit_policy_id="risk-policy",
        mandate_id="mandate",
        mandate_fingerprint="mandate-fingerprint",
        reports=[_source_report(1), _source_report(2)],
    )
    return dict(output.report)


def test_valid_report_is_canonical_and_deterministic() -> None:
    first = validate_operational_objective_report(_report())
    second = validate_operational_objective_report(dict(reversed(list(_report().items()))))

    assert first == second
    assert canonical_objective_report_bytes(first) == canonical_objective_report_bytes(
        second
    )
    assert first["history_status"] == "ready"
    assert first["overall_status"] == "met"
    assert len(first["objectives"]) == 4


def test_report_side_effect_flags_fail_closed() -> None:
    report = _report()
    report["automated_remediation_performed"] = True

    with pytest.raises(ValidationError, match="must be false"):
        validate_operational_objective_report(report)


def test_report_objective_ratio_must_reconcile() -> None:
    report = _report()
    objectives = [dict(row) for row in report["objectives"]]
    objectives[0]["attainment_ratio"] = 0.5
    report["objectives"] = objectives

    with pytest.raises(ValidationError, match="ratio does not reconcile"):
        validate_operational_objective_report(report)


def test_report_shape_and_input_evidence_are_strict() -> None:
    report = _report()
    report["unexpected"] = True
    with pytest.raises(ValidationError, match="invalid shape"):
        validate_operational_objective_report(report)

    report = _report()
    report["input_report_document_sha256"] = []
    with pytest.raises(ValidationError, match="input report evidence"):
        validate_operational_objective_report(report)
