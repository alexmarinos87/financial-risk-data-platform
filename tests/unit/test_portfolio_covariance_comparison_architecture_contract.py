from pathlib import Path


def test_covariance_comparison_documentation_matches_runtime_contract() -> None:
    documentation = Path("docs/portfolio-covariance-comparison.md").read_text(
        encoding="utf-8"
    )
    analytics = Path(
        "src/analytics/portfolio_attribution_ewma.py"
    ).read_text(encoding="utf-8")
    runner = Path(
        "src/orchestration/run_portfolio_covariance_comparison.py"
    ).read_text(encoding="utf-8")
    schema = Path("sql/portfolio_attribution_schema.sql").read_text(
        encoding="utf-8"
    )

    for required in (
        "portfolio-attribution-ewma-v1",
        "ewma_zero_mean_lambda_0_94_annualized",
        "implied_from_ewma_covariance",
    ):
        assert required in documentation
        assert required in analytics

    assert "EWMA decay         = 0.94" in documentation
    assert "EWMA_DECAY = 0.94" in analytics
    assert "run_portfolio_covariance_comparison" in documentation
    assert "run_portfolio_covariance_comparison" in runner
    assert "ordered input calculation IDs" in documentation
    assert "already_present" in documentation

    for required in (
        "input_calculation_ids_json",
        "ewma_minus_sample_volatility",
        "records_already_present",
    ):
        assert required in runner

    assert "model_version" in schema
    assert "covariance_method" in schema
    assert "correlation_method" in schema
    assert "PARTITION BY" in schema

    for prohibited in (
        "configurable decay",
        "rolling EWMA history",
        "provider request",
        "infrastructure action",
    ):
        assert prohibited in documentation
