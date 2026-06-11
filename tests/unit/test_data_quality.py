from src.analytics.data_quality import (
    late_rate,
    null_field_metrics,
    numeric_range_metrics,
    required_field_metrics,
)
from src.orchestration.run_pipeline import _evaluate_max, _evaluate_threshold


def test_late_rate_handles_empty_total() -> None:
    assert late_rate(3, 0) == 0.0


def test_late_rate_keeps_boundary_values_explicit() -> None:
    assert late_rate(0, 4) == 0.0
    assert late_rate(1, 4) == 0.25
    assert late_rate(4, 4) == 1.0


def test_data_quality_max_threshold_treats_equal_rate_as_ok() -> None:
    assert _evaluate_max(0.02, 0.02) == "ok"
    assert _evaluate_max(0.0201, 0.02) == "critical"
    assert _evaluate_max(0.25, None) == "ok"


def test_volatility_threshold_status_transitions_are_inclusive() -> None:
    assert _evaluate_threshold(0.029, warn=0.03, critical=0.07) == "ok"
    assert _evaluate_threshold(0.03, warn=0.03, critical=0.07) == "warn"
    assert _evaluate_threshold(0.069, warn=0.03, critical=0.07) == "warn"
    assert _evaluate_threshold(0.07, warn=0.03, critical=0.07) == "critical"


def test_required_field_metrics_counts_missing_fields_by_record() -> None:
    records = [
        {"event_id": "evt-1", "symbol": "AAPL", "price": 100.0},
        {"event_id": "evt-2", "price": 101.0},
        {"symbol": "MSFT", "price": 240.0},
    ]

    result = required_field_metrics(records, ["event_id", "symbol", "price"])

    assert result == {
        "required_fields_checked": 3,
        "missing_by_field": {
            "event_id": 1,
            "symbol": 1,
            "price": 0,
        },
        "missing_field_count": 2,
        "failed_record_count": 2,
        "status": "critical",
    }


def test_null_field_metrics_counts_null_values_by_field() -> None:
    records = [
        {"event_id": "evt-1", "symbol": "AAPL", "price": 100.0},
        {"event_id": "evt-2", "symbol": None, "price": 101.0},
        {"event_id": "evt-3", "symbol": "MSFT", "price": None},
    ]

    result = null_field_metrics(records, ["event_id", "symbol", "price"])

    assert result == {
        "fields_checked": 3,
        "nulls_by_field": {
            "event_id": 0,
            "symbol": 1,
            "price": 1,
        },
        "null_rates_by_field": {
            "event_id": 0.0,
            "symbol": 1 / 3,
            "price": 1 / 3,
        },
        "null_field_count": 2,
        "failed_record_count": 2,
        "max_null_rate": 1 / 3,
        "status": "critical",
    }


def test_numeric_range_metrics_counts_invalid_values_by_field_and_record() -> None:
    records = [
        {"event_id": "evt-1", "price": 100.0, "volume": 10},
        {"event_id": "evt-2", "price": 0.0, "volume": 5},
        {"event_id": "evt-3", "price": -1.0, "volume": -2},
        {"event_id": "evt-4", "price": "101.0", "volume": True},
        {"event_id": "evt-5", "price": None},
    ]

    result = numeric_range_metrics(
        records,
        {
            "price": {"min_exclusive": 0.0},
            "volume": {"min_inclusive": 0.0},
        },
    )

    assert result == {
        "fields_checked": 2,
        "invalid_by_field": {
            "price": 3,
            "volume": 2,
        },
        "invalid_field_count": 5,
        "failed_record_count": 3,
        "status": "critical",
    }
