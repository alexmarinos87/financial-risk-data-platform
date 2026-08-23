from pathlib import Path


def test_method_aware_risk_limit_contract_is_documented() -> None:
    documentation = Path("docs/portfolio-risk-limit-methods.md").read_text(
        encoding="utf-8"
    )
    method_config = Path(
        "config/portfolio_risk_limit_methods.yaml"
    ).read_text(encoding="utf-8")

    for required in (
        "portfolio-risk-limits-v2",
        "us-tech-sample",
        "us-tech-ewma",
        "portfolio-attribution-v1",
        "portfolio-attribution-ewma-v1",
        "portfolio_risk_limit_method_comparison",
        "run_method_aware_portfolio_risk_limits",
        "status disagreement",
    ):
        assert required in documentation or required in method_config

    assert "no threshold is silently reused" not in documentation.lower()
    assert "explicit method binding" in documentation.lower()
