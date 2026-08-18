from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError as PydanticValidationError

from src.ingestion.market_data_loader import build_market_event_from_row
from src.ingestion.schemas import MarketEvent


def _event_payload() -> dict[str, object]:
    return {
        "event_id": "evt-1",
        "symbol": "AAPL",
        "price": 189.32,
        "volume": 1200,
        "ts_event": datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc),
        "ts_ingest": datetime(2025, 1, 20, 10, 1, 3, tzinfo=timezone.utc),
        "source": "stooq",
    }


@pytest.mark.parametrize("field_name", ["ts_event", "ts_ingest"])
@pytest.mark.parametrize(
    "naive_value",
    [datetime(2025, 1, 20, 10, 1), "2025-01-20T10:01:00"],
)
def test_market_event_rejects_timezone_naive_timestamps(
    field_name: str,
    naive_value: datetime | str,
) -> None:
    payload = _event_payload()
    payload[field_name] = naive_value

    with pytest.raises(PydanticValidationError) as error:
        MarketEvent.model_validate(payload)

    assert error.value.errors()[0]["loc"] == (field_name,)
    assert error.value.errors()[0]["type"] == "timezone_aware"


def test_market_event_normalises_aware_offsets_to_utc() -> None:
    offset = timezone(timedelta(hours=2))
    payload = _event_payload()
    payload["ts_event"] = datetime(2025, 1, 20, 12, 1, tzinfo=offset)
    payload["ts_ingest"] = datetime(2025, 1, 20, 12, 1, 3, tzinfo=offset)

    event = MarketEvent.model_validate(payload)

    assert event.ts_event == datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc)
    assert event.ts_ingest == datetime(2025, 1, 20, 10, 1, 3, tzinfo=timezone.utc)


def test_landed_naive_timestamps_are_normalised_before_schema_validation() -> None:
    event = build_market_event_from_row(
        {
            "event_id": "evt-1",
            "symbol": "AAPL",
            "price": 189.32,
            "volume": 1200,
            "ts_event": "2025-01-20T10:01:00",
            "ts_ingest": "2025-01-20T10:01:03",
            "source": "stooq",
        }
    )

    assert event.ts_event == datetime(2025, 1, 20, 10, 1, tzinfo=timezone.utc)
    assert event.ts_ingest == datetime(2025, 1, 20, 10, 1, 3, tzinfo=timezone.utc)
