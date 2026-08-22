from pathlib import Path


def test_governed_cycle_documents_temporal_selection_and_boundaries() -> None:
    source = Path(
        "src/orchestration/run_governed_portfolio_cycle.py"
    ).read_text(encoding="utf-8")
    docs = Path("docs/governed-portfolio-cycle.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "load_effective_portfolio_risk_limit_policy",
        "validate_mandate_range",
        "validate_policy_range",
        "governed-portfolio/",
        "portfolio_risk",
        "portfolio_attribution_history",
        "portfolio_risk_limits",
        "policy_version",
        "analytics_and_evidence_only",
    ):
        assert required in source

    for required in (
        "inclusive `effective_from`",
        "exclusive nullable `effective_to`",
        "split a historical request",
        "one portfolio-scoped lock",
        "does not send notifications",
    ):
        assert required in docs
