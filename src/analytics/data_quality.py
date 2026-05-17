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
