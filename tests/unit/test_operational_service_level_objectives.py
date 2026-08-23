from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from src.analytics.operational_service_level_objectives import (
    OBJECTIVE_NAMES,
    evaluate_operational_objectives,
    load_operational_objective_policy,
    parse_operational_objective_policy,
)
from src.common.exceptions import ValidationError


def _policy_payload(
    *,
    window_sessions: int = 3,
    minimum_observations: int = 2,
    schedule_target_ratio: float = 2 / 3,
) -> dict[str, object]:
    return {
        "objective_policies": {
            "demo": {
                "operational_policy_id": "operations",
                "window_sessions": window_sessions,
                "minimum_observations": minimum_observations,
                "targets": {
                    "schedule_completion_attainment": {
                        "source_metric_name": "schedule_lag_sessions",
                        "success_threshold": 0,
                        "target_ratio": schedule_target_ratio,
                    },
                    "market_freshness_attainment": {
                        "source_metric_name": "market_freshness_exception_count",
                        "success_threshold": 0,
                        "target_ratio": 2 / 3,
                    },
                    "notification_retry_exhaustion_free_attainment": {
                        "source_metric_name": (
                            "notification_retry_exhausted_count"
                        ),
                        "success_threshold": 0,
                        "target_ratio": 1.0,
                    },
                    "notification_dead_letter_duration_attainment": {
                        "source_metric_name": (
                            "notification_oldest_dead_letter_age_seconds"
                        ),
                        "success_threshold": 900,
                        "target_ratio": 1.0,
                    },
                },
            }
        }
    }


def _report(
    day: int,
    *,
    schedule_lag: float = 0,
    freshness_exceptions: float = 0,
    retry_exhausted: float = 0,
    dead_letter_age: float = 0,
    calculation_id: str | None = None,
    as_of_hour: int = 1,
) -> dict[str, object]:
    metric_names = (
        "schedule_lag_sessions",
        "market_freshness_exception_count",
        "notification_retry_exhausted_count",
        "notification_oldest_dead_letter_age_seconds",
    )
    values = (
        schedule_lag,
        freshness_exceptions,
        retry_exhausted,
        dead_letter_age,
    )
    units = ("sessions", "constituents", "events", "seconds")
    metrics = [
        {
            "metric_name": name,
            "observed_value": value,
            "unit": unit,
            "warning_threshold": 1,
            "critical_threshold": 2,
            "status": "ok",
            "reason": None,
        }
        for name, value, unit in zip(metric_names, values, units, strict=True)
    ]
    return {
        "calculation_id": calculation_id
        or f"operational-service-levels-v1-report-{day:024x}",
        "policy_id": "operations",
        "policy_fingerprint": "operational-slo-policy-" + "a" * 24,
        "schedule_id": "schedule",
        "schedule_fingerprint": "schedule-fingerprint",
        "calendar_id": "XNYS",
        "portfolio_id": "portfolio",
        "risk_limit_policy_id": "risk-policy",
        "mandate_fingerprint": "mandate-fingerprint",
        "as_of": datetime(
            2026,
            1,
            day,
            as_of_hour,
            tzinfo=timezone.utc,
        ),
        "latest_expected_session": date(2026, 1, day),
        "metrics_json": metrics,
        "document_sha256": f"{day:064x}",
    }


def _evaluate(
    reports: list[dict[str, object]],
    *,
    window_sessions: int = 3,
    minimum_observations: int = 2,
    schedule_target_ratio: float = 2 / 3,
):
    policy = parse_operational_objective_policy(
        _policy_payload(
            window_sessions=window_sessions,
            minimum_observations=minimum_observations,
            schedule_target_ratio=schedule_target_ratio,
        ),
        "demo",
    )
    sessions = tuple(
        date(2026, 1, day) for day in range(1, window_sessions + 1)
    )
    return evaluate_operational_objectives(
        objective_policy=policy,
        through_session=sessions[-1],
        expected_sessions=sessions,
        operational_policy_fingerprint="operational-slo-policy-" + "a" * 24,
        schedule_id="schedule",
        schedule_fingerprint="schedule-fingerprint",
        calendar_id="XNYS",
        portfolio_id="portfolio",
        risk_limit_policy_id="risk-policy",
        mandate_id="mandate",
        mandate_fingerprint="mandate-fingerprint",
        reports=reports,
    )


def test_objective_policy_loads_the_reviewed_configuration() -> None:
    policy = load_operational_objective_policy(
        Path("config/operational_service_level_objectives.yaml"),
        "us-tech-local-20-session",
    )

    assert policy.operational_policy_id == "us-tech-local"
    assert policy.window_sessions == 20
    assert policy.minimum_observations == 10
    assert tuple(policy.objectives) == OBJECTIVE_NAMES
    assert policy.fingerprint.startswith("operational-slo-objective-policy-")


def test_ready_window_reports_met_and_missed_objectives() -> None:
    output = _evaluate(
        [
            _report(1),
            _report(2, freshness_exceptions=1),
            _report(3, dead_letter_age=901),
        ]
    )
    rows = {row["objective_name"]: row for row in output.objectives}

    assert output.report["history_status"] == "ready"
    assert output.report["window_complete"] is True
    assert output.report["overall_status"] == "missed"
    assert rows["schedule_completion_attainment"]["status"] == "met"
    assert rows["market_freshness_attainment"][
        "attainment_ratio"
    ] == pytest.approx(2 / 3)
    assert rows["notification_dead_letter_duration_attainment"][
        "status"
    ] == "missed"


def test_insufficient_history_is_not_reported_as_objective_failure() -> None:
    output = _evaluate(
        [_report(1)],
        window_sessions=3,
        minimum_observations=2,
    )

    assert output.report["history_status"] == "insufficient"
    assert output.report["overall_status"] == "insufficient"
    assert all(row["status"] == "insufficient" for row in output.objectives)


def test_missing_expected_session_counts_against_attainment() -> None:
    output = _evaluate(
        [_report(1), _report(3)],
        window_sessions=3,
        minimum_observations=2,
    )
    row = {
        item["objective_name"]: item for item in output.objectives
    }["schedule_completion_attainment"]

    assert output.report["history_status"] == "ready"
    assert output.report["window_complete"] is False
    assert output.report["missing_report_sessions"] == ["2026-01-02"]
    assert row["attainment_ratio"] == pytest.approx(2 / 3)
    assert row["missing_report_observations"] == 1


def test_latest_report_correction_wins_for_one_session() -> None:
    older = _report(
        2,
        schedule_lag=1,
        calculation_id=(
            "operational-service-levels-v1-report-" + "1" * 24
        ),
        as_of_hour=1,
    )
    newer = _report(
        2,
        schedule_lag=0,
        calculation_id=(
            "operational-service-levels-v1-report-" + "2" * 24
        ),
        as_of_hour=2,
    )

    output = _evaluate([_report(1), older, newer, _report(3)])

    input_ids = output.report["input_report_calculation_ids"]
    assert newer["calculation_id"] in input_ids
    assert older["calculation_id"] not in input_ids


def test_conflicting_report_calculation_id_fails_closed() -> None:
    first = _report(
        1,
        calculation_id=(
            "operational-service-levels-v1-report-" + "f" * 24
        ),
    )
    conflicting = dict(first)
    conflicting["document_sha256"] = "b" * 64

    with pytest.raises(ValidationError, match="conflicting content"):
        _evaluate([first, conflicting, _report(2), _report(3)])


def test_policy_change_produces_a_new_fingerprint() -> None:
    baseline = parse_operational_objective_policy(_policy_payload(), "demo")
    changed = parse_operational_objective_policy(
        _policy_payload(schedule_target_ratio=0.8),
        "demo",
    )

    assert baseline.fingerprint != changed.fingerprint
