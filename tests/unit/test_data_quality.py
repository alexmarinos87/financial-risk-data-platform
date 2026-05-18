from src.analytics.data_quality import late_rate, null_field_metrics, required_field_metrics


def test_late_rate_handles_empty_total() -> None:
    assert late_rate(3, 0) == 0.0


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
