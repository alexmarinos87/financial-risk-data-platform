"""Model instances must obey the same canonical contract as event mappings."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.analytics import daily_risk
from src.common.exceptions import ValidationError
from src.ingestion.schemas import MarketEvent


OPTIONS = {"volatility_window": 2, "var_window": 2}
INPUT_ERROR = "Daily risk input contains an invalid market event"


def events() -> list[MarketEvent]:
    return [MarketEvent(
        event_id=f"snapshot-{day}", symbol="IBM", source="alpha_vantage",
        price=price, volume=1,
        ts_event=datetime(2026, 1, day, tzinfo=timezone.utc),
        ts_ingest=datetime(2026, 1, day, 1, tzinfo=timezone.utc),
    ) for day, price in enumerate([100.0, 110.0, 99.0, 108.9], 1)]


@pytest.mark.parametrize("field,value", [
    ("ts_event", datetime(2026, 1, 2)),
    ("ts_ingest", datetime(2026, 1, 2, 1)),
    ("volume", 1.5), ("volume", "private-invalid-volume"),
    ("symbol", None), ("price", "private-invalid-price"),
])
@pytest.mark.parametrize("mode", ["assignment", "copy", "construct"])
def test_invalid_model_state_rejects_like_its_mapping(
    field: str, value: Any, mode: str,
) -> None:
    source = events()
    if mode == "assignment":
        setattr(source[1], field, value)
    elif mode == "copy":
        source[1] = source[1].model_copy(update={field: value})
    else:
        source[1] = MarketEvent.model_construct(**{**dict(source[1]), field: value})
    before = [dict(event) for event in source]
    with pytest.raises(ValidationError) as mapping_error:
        daily_risk.build_daily_risk_outputs(before, **OPTIONS)
    with pytest.raises(ValidationError) as instance_error:
        daily_risk.build_daily_risk_outputs(source, **OPTIONS)
    assert str(instance_error.value) == str(mapping_error.value) == INPUT_ERROR
    assert instance_error.value.__suppress_context__ is True
    assert [dict(event) for event in source] == before


@pytest.mark.parametrize("field", ["ts_event", "ts_ingest"])
def test_all_naive_timestamps_cannot_be_silently_interpreted_as_local_time(field: str) -> None:
    source = events()
    for event in source:
        setattr(event, field, getattr(event, field).replace(tzinfo=None))
    with pytest.raises(ValidationError, match=f"^{INPUT_ERROR}$"):
        daily_risk.build_daily_risk_outputs(source, **OPTIONS)


def test_incomplete_constructed_model_uses_public_validation_error() -> None:
    source = events()
    fields = dict(source[1])
    fields.pop("ts_ingest")
    source[1] = MarketEvent.model_construct(**fields)
    with pytest.raises(ValidationError, match=f"^{INPUT_ERROR}$"):
        daily_risk.build_daily_risk_outputs(source, **OPTIONS)


def test_valid_offset_instances_are_normalised_without_mutating_the_caller() -> None:
    source = events()
    expected = daily_risk.build_daily_risk_outputs(source, **OPTIONS)
    zone = timezone(timedelta(hours=2))
    offset_source = [event.model_copy(update={
        "ts_event": event.ts_event.astimezone(zone),
        "ts_ingest": event.ts_ingest.astimezone(zone),
    }) for event in source]
    before = [dict(event) for event in offset_source]
    actual = daily_risk.build_daily_risk_outputs(offset_source, **OPTIONS)
    assert actual == expected
    assert all(row["ts_event"].tzinfo is timezone.utc for row in actual.returns)
    assert all(row["ts_ingest"].tzinfo is timezone.utc for row in actual.returns)
    assert [dict(event) for event in offset_source] == before
    assert all(event.ts_event.tzinfo is zone for event in offset_source)


def test_reused_mutable_event_is_snapshotted_before_requesting_the_next_item() -> None:
    source = events()
    expected = daily_risk.build_daily_risk_outputs(source, **OPTIONS)
    reusable = source[0].model_copy()

    def history() -> Iterator[MarketEvent]:
        for event in source:
            for name, value in dict(event).items():
                setattr(reusable, name, value)
            yield reusable

    actual = daily_risk.build_daily_risk_outputs(history(), **OPTIONS)
    assert actual == expected


def test_later_generator_mutation_cannot_change_an_already_validated_observation() -> None:
    source = events()
    expected = daily_risk.build_daily_risk_outputs(source, **OPTIONS)

    def history() -> Iterator[MarketEvent]:
        yield source[0]
        source[0].price = float("inf")
        yield from source[1:]

    assert daily_risk.build_daily_risk_outputs(history(), **OPTIONS) == expected


def test_valid_models_mappings_and_replay_remain_equivalent() -> None:
    source = events()
    before = [dict(event) for event in source]
    expected = daily_risk.build_daily_risk_outputs(before, **OPTIONS)
    assert daily_risk.build_daily_risk_outputs(source, **OPTIONS) == expected
    assert daily_risk.build_daily_risk_outputs(reversed(source), **OPTIONS) == expected
    assert [dict(event) for event in source] == before
