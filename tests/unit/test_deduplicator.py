from copy import deepcopy
from typing import Any

import pytest

from src.common.exceptions import ValidationError
from src.processing.deduplicator import dedupe_events


def test_empty_input() -> None:
    assert dedupe_events([]) == []


def test_equal_duplicates_preserve_first_record_order_and_input() -> None:
    first = {"event_id": "b", "price": 10.0, "metadata": {"source": "demo"}}
    second = {"event_id": "a", "price": 11.0}
    events = [first, second, deepcopy(first), deepcopy(second)]
    original = deepcopy(events)
    result = dedupe_events(events)
    assert result == [first, second]
    assert result[0] is first
    assert result[1] is second
    assert events == original


@pytest.mark.parametrize(
    "change",
    [
        {"price": 12.0},
        {"symbol": "OTHER"},
        {"ts_ingest": "2026-01-01T00:01:00Z"},
        {"source": "another-source"},
        {"metadata": {"venue": "changed"}},
        {"unexpected": True},
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_conflicting_identity_rejected_in_either_input_order(
    change: dict[str, Any], reverse: bool
) -> None:
    first = {
        "event_id": "private-id",
        "symbol": "DEMO",
        "price": 10.0,
        "ts_ingest": "2026-01-01T00:00:00Z",
        "source": "demo",
        "metadata": {"venue": "original"},
    }
    events = [first, {**first, **change}]
    if reverse:
        events.reverse()
    original = deepcopy(events)
    with pytest.raises(ValidationError, match="Conflicting duplicate event identity") as exc:
        dedupe_events(events)
    assert "private-id" not in str(exc.value)
    assert "original" not in str(exc.value)
    assert events == original


def test_missing_payload_field_is_a_conflict() -> None:
    with pytest.raises(ValidationError, match="Conflicting duplicate"):
        dedupe_events([{"event_id": "a", "price": 1}, {"event_id": "a"}])


@pytest.mark.parametrize("event", [{}, {"event_id": None}, {"event_id": []}, {"event_id": {}}])
def test_invalid_identity_is_not_silently_deduplicated(event: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="deduplication identity"):
        dedupe_events([event, deepcopy(event)])


def test_custom_key_and_integer_identities_remain_supported() -> None:
    first = {"sequence": 2, "value": "b"}
    second = {"sequence": 1, "value": "a"}
    assert dedupe_events([first, second, dict(first)], key="sequence") == [first, second]
    with pytest.raises(ValidationError, match="Conflicting duplicate"):
        dedupe_events([first, {"sequence": 2, "value": "changed"}], key="sequence")


def test_repeated_duplicates_do_not_hide_a_later_conflict() -> None:
    with pytest.raises(ValidationError, match="Conflicting duplicate"):
        dedupe_events([
            {"event_id": "a", "price": 1},
            {"event_id": "a", "price": 1},
            {"event_id": "a", "price": 2},
        ])
