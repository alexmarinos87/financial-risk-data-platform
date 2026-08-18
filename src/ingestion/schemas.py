from datetime import datetime, timezone

from pydantic import AwareDatetime, BaseModel, Field, field_validator


class MarketEvent(BaseModel):
    event_id: str
    symbol: str
    price: float
    volume: int
    ts_event: AwareDatetime
    ts_ingest: AwareDatetime
    source: str = Field(default="stooq")

    @field_validator("ts_event", "ts_ingest")
    @classmethod
    def _normalise_timestamp_to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)
