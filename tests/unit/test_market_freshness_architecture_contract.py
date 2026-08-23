from pathlib import Path


def test_market_freshness_documentation_matches_runtime_contract() -> None:
    docs = Path("docs/market-freshness.md").read_text(encoding="utf-8")
    config = Path("config/market_calendars.yaml").read_text(
        encoding="utf-8"
    )
    runner = Path("src/orchestration/run_market_freshness.py").read_text(
        encoding="utf-8"
    )

    for required in (
        "current",
        "gap_detected",
        "stale",
        "weekend",
        "holiday",
        "half-open",
        "provider request",
        "daily_market_freshness",
    ):
        assert required in docs

    for required in (
        "America/New_York",
        "2026-07-03",
        "2026-11-27",
        '"13:00"',
    ):
        assert required in config

    assert "load_alpha_vantage_daily_events" in runner
    assert "provider_request_performed" in runner
