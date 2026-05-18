from __future__ import annotations

from typing import Any


def late_rate(late_count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return late_count / total


def required_field_metrics(
    records: list[dict[str, Any]],
    required_fields: list[str],
) -> dict[str, Any]:
    missing_by_field = {
        field: sum(1 for record in records if field not in record) for field in required_fields
    }
    failed_record_count = sum(
        1 for record in records if any(field not in record for field in required_fields)
    )
    missing_field_count = sum(missing_by_field.values())

    return {
        "required_fields_checked": len(required_fields),
        "missing_by_field": missing_by_field,
        "missing_field_count": missing_field_count,
        "failed_record_count": failed_record_count,
        "status": "critical" if failed_record_count else "ok",
    }


def null_field_metrics(
    records: list[dict[str, Any]],
    fields: list[str],
) -> dict[str, Any]:
    total_records = len(records)
    nulls_by_field = {
        field: sum(1 for record in records if field in record and record[field] is None)
        for field in fields
    }
    null_rates_by_field = {
        field: 0.0 if total_records == 0 else count / total_records
        for field, count in nulls_by_field.items()
    }
    failed_record_count = sum(
        1 for record in records if any(field in record and record[field] is None for field in fields)
    )
    null_field_count = sum(nulls_by_field.values())

    return {
        "fields_checked": len(fields),
        "nulls_by_field": nulls_by_field,
        "null_rates_by_field": null_rates_by_field,
        "null_field_count": null_field_count,
        "failed_record_count": failed_record_count,
        "max_null_rate": max(null_rates_by_field.values(), default=0.0),
        "status": "critical" if failed_record_count else "ok",
    }
