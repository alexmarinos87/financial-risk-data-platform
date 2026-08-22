from pathlib import Path


def test_portfolio_risk_limit_contract_is_documented() -> None:
    document = Path("docs/portfolio-risk-limits.md").read_text(encoding="utf-8")
    policy = Path("config/portfolio_risk_limits.yaml").read_text(encoding="utf-8")
    schema = Path("sql/portfolio_risk_limits_schema.sql").read_text(
        encoding="utf-8"
    )

    for required in (
        "portfolio-risk-limits-v1",
        "portfolio_volatility_annualized",
        "largest_absolute_component_contribution_share",
        "policy fingerprint",
        "portfolio_risk_limit_evaluations",
        "latest_portfolio_risk_limit_evaluations",
        "portfolio_risk_limit_breaches",
        "portfolio_risk_limit_snapshot_status",
        "does not send alerts",
    ):
        assert required in document

    assert "us-tech-standard" in policy
    assert "warning: 0.30" in policy
    assert "critical: 0.45" in policy
    assert "policy_fingerprint" in schema
    assert "attribution_calculation_id" in schema
