from pathlib import Path


def test_architecture_matches_current_runtime_contract() -> None:
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")

    for required in (
        "Python **3.11**",
        "Pydantic `AwareDatetime`",
        "daily-risk-v2",
        "risk_platform.daily_returns",
        "risk_platform.daily_volatility",
        "risk_platform.daily_risk_summary",
        "latest_daily_risk_summary",
        "daily_risk_semantic_model",
        "portfolio_daily_returns",
        "portfolio_daily_risk_summary",
        "issue #51",
    ):
        assert required in architecture

    assert "Python 3.10 or newer" not in architecture
    assert "direct schema input can currently remain timezone-naive" not in architecture
    assert (
        "Portfolio-level aggregation and correlation risk are not yet implemented"
        not in architecture
    )
