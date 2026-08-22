from pathlib import Path


def test_effective_dated_mandates_are_wired_to_governed_runtime() -> None:
    mandate_doc = Path("docs/portfolio-mandates.md").read_text(encoding="utf-8")
    cycle_doc = Path("docs/governed-portfolio-cycle.md").read_text(
        encoding="utf-8"
    )
    cycle_code = Path(
        "src/orchestration/run_governed_portfolio_cycle.py"
    ).read_text(encoding="utf-8")

    for required in (
        "run_governed_portfolio_cycle.py",
        "filters every input",
        "portfolio risk, rolling attribution and",
        "risk-limit evaluation",
    ):
        assert required in mandate_doc

    for required in (
        "load_portfolio_mandate",
        "validate_mandate_range",
        "filter_records_to_mandate",
        "governed-portfolio/",
        "analytics_and_evidence_only",
        "portfolio_attribution_history",
        "portfolio_risk_limits",
    ):
        assert required in cycle_code

    for required in (
        "A range crossing a mandate boundary fails before any stage writes data",
        "policy.covariance_window == requested covariance window",
        "whole cycle safe to rerun",
        "does not call Alpha Vantage",
    ):
        assert required in cycle_doc

    stale = (
        "Wiring the portfolio, attribution and risk-limit operator commands "
        "to this selector is deliberately delivered as a separate PR"
    )
    assert stale not in mandate_doc
