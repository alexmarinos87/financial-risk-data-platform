from datetime import datetime
from pydantic import BaseModel, Field


class MarketEvent(BaseModel):
    event_id: str
    symbol: str
    price: float
    volume: int
    ts_event: datetime
    ts_ingest: datetime
    source: str = Field(default="stooq")
