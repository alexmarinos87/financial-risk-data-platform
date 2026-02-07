from src.ingestion.market_data_producer import build_market_event


def test_build_market_event():
    event = build_market_event("AAPL", 100.0, 10, "evt-1")
    assert event.symbol == "AAPL"
