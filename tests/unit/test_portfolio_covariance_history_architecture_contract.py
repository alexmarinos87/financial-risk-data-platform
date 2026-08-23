from pathlib import Path


def test_rolling_covariance_comparison_documentation_matches_runtime() -> None:
    documentation = Path(
        "docs/portfolio-covariance-history-comparison.md"
    ).read_text(encoding="utf-8")
    history = Path(
        "src/analytics/portfolio_attribution_ewma_history.py"
    ).read_text(encoding="utf-8")
    runner = Path(
        "src/orchestration/run_portfolio_covariance_history_comparison.py"
    ).read_text(encoding="utf-8")
    schema = Path("sql/portfolio_covariance_method_schema.sql").read_text(
        encoding="utf-8"
    )

    for required in (
        "build_portfolio_ewma_attribution_history",
        "MAX_HISTORY_SNAPSHOTS",
        "start_date",
        "max_snapshots",
    ):
        assert required in history

    for required in (
        "run_portfolio_covariance_history_comparison",
        "paired_snapshot_dates",
        "maximum_absolute_difference",
        "records_already_present",
    ):
        assert required in runner

    for required in (
        "run_portfolio_covariance_history_comparison",
        "2,500",
        "portfolio_covariance_method_comparison",
        "portfolio_covariance_method_consistency_checks.sql",
        "ordered input calculation IDs",
    ):
        assert required in documentation

    for required in (
        "portfolio-attribution-ewma-v1",
        "ewma_zero_mean_lambda_0_94_annualized",
        "implied_from_ewma_covariance",
        "input_calculation_ids_json = sample.input_calculation_ids_json",
    ):
        assert required in schema
