from pathlib import Path

import yaml


def test_effective_dated_risk_limit_policy_contract_is_documented() -> None:
    documentation = Path(
        "docs/portfolio-risk-limit-policy-history.md"
    ).read_text(encoding="utf-8")
    normalized_documentation = " ".join(documentation.split())
    configuration = yaml.safe_load(
        Path("config/portfolio_risk_limits.yaml").read_text(encoding="utf-8")
    )
    policy = configuration["policies"]["us-tech-standard"]

    for required in (
        "effective_from",
        "effective_to",
        "policy_version_id",
        "limit_definition_fingerprint",
        "policy_fingerprint",
        "inclusive",
        "exclusive",
        "remain within one selected policy version",
    ):
        assert required in normalized_documentation

    assert policy["policy_version_id"] == "us-tech-standard-v1"
    assert policy["effective_from"].isoformat() == "2026-01-01"
    assert policy["effective_to"] is None
