from pathlib import Path


def test_architecture_matches_current_runtime_contract() -> None:
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")
    attribution = Path("docs/portfolio-attribution.md").read_text(encoding="utf-8")

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
        "risk_platform.portfolio_daily_returns",
        "risk_platform.portfolio_daily_risk_summary",
        "latest_portfolio_daily_risk_summary",
        "portfolio_risk_semantic_model",
        "portfolio_daily_contribution_model",
        "portfolio_risk_attribution",
        "Euler component-volatility attribution",
        "issue #51",
    ):
        assert required in architecture

    for required in (
        "portfolio-attribution-v1",
        "sample_covariance",
        "component_contribution_i",
        "portfolio-attribution-demo",
        "undefined_zero_variance",
    ):
        assert required in attribution

    for stale in (
        "Python 3.10 or newer",
        "direct schema input can currently remain timezone-naive",
        "Portfolio-level aggregation and correlation risk are not yet implemented",
        "Portfolio outputs are curated Parquet only",
        "PostgreSQL portfolio serving is not yet implemented",
        "Marginal risk attribution, covariance matrices",
        "Marginal VaR, covariance matrices",
    ):
        assert stale not in architecture
