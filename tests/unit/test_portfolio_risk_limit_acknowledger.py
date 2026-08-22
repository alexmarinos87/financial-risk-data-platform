from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.common.exceptions import ValidationError
from src.warehouse.portfolio_risk_limit_acknowledger import (
    MODEL_VERSION,
    _aware_utc,
    _bounded_text,
    _disposition,
    acknowledgement_id,
)


def test_acknowledgement_id_is_deterministic_and_request_scoped() -> None:
    first = acknowledgement_id("evaluation-1", "INC-2026-001")
    second = acknowledgement_id("evaluation-1", "INC-2026-001")
    other_request = acknowledgement_id("evaluation-1", "INC-2026-002")
    other_evaluation = acknowledgement_id("evaluation-2", "INC-2026-001")

    assert first == second
    assert first.startswith(f"{MODEL_VERSION}-")
    assert len(first.rsplit("-", maxsplit=1)[-1]) == 24
    assert len({first, other_request, other_evaluation}) == 3


def test_acknowledged_at_requires_timezone_and_normalises_to_utc() -> None:
    value = datetime(2026, 2, 1, 12, tzinfo=timezone(timedelta(hours=2)))

    assert _aware_utc(value) == datetime(2026, 2, 1, 10, tzinfo=timezone.utc)
    assert _aware_utc("2026-02-01T10:00:00Z") == datetime(
        2026, 2, 1, 10, tzinfo=timezone.utc
    )
    with pytest.raises(ValidationError, match="timezone-aware"):
        _aware_utc("2026-02-01T10:00:00")


def test_acknowledgement_text_and_disposition_are_bounded() -> None:
    assert _bounded_text("  reviewer@example.com  ", "actor", 128) == (
        "reviewer@example.com"
    )
    assert _disposition("INVESTIGATING") == "investigating"

    with pytest.raises(ValidationError, match="control characters"):
        _bounded_text("line\nbreak", "reason", 2_000)
    with pytest.raises(ValidationError, match="disposition"):
        _disposition("closed")
    with pytest.raises(ValidationError, match="invalid format"):
        acknowledgement_id("evaluation-1", "request id with spaces")
