from .schemas import MarketEvent
from ..common.time import utc_now


def build_market_event(symbol: str, price: float, volume: int, event_id: str) -> MarketEvent:
    return MarketEvent(
        event_id=event_id,
        symbol=symbol,
        price=price,
        volume=volume,
        ts_event=utc_now(),
        ts_ingest=utc_now(),
        source="stooq",
    )
