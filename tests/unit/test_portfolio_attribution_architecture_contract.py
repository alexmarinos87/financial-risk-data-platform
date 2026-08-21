from pathlib import Path


def test_architecture_documents_attribution_warehouse_serving() -> None:
    architecture = Path("docs/architecture.md").read_text(encoding="utf-8")
    attribution = Path("docs/portfolio-attribution.md").read_text(
        encoding="utf-8"
    )
    readme = Path("README.md").read_text(encoding="utf-8")

    for required in (
        "risk_platform.portfolio_risk_attribution",
        "latest_portfolio_risk_attribution",
        "portfolio_attribution_semantic_model",
        "portfolio_covariance_model",
        "portfolio_correlation_model",
        "portfolio_volatility_contribution_model",
        "portfolio-attribution-warehouse-load",
        "check-portfolio-attribution-consistency",
    ):
        assert required in architecture
        assert required in attribution or required in readme

    stale = (
        "Attribution remains curated Parquet in the current increment",
        "PostgreSQL attribution serving are not implemented",
        "does not yet add PostgreSQL attribution tables or views",
    )
    for statement in stale:
        assert statement not in architecture
        assert statement not in attribution
