from pathlib import Path


def test_model_approval_evidence_is_documented_and_executable() -> None:
    documentation = Path("docs/model-approval-evidence.md").read_text(
        encoding="utf-8"
    )
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    for required in (
        "model-contract-v1",
        "model-approval-v1",
        "model-approval-revocation-v1",
        "risk_platform.model_approvals",
        "risk_platform.model_approval_revocations",
        "current_model_approval_status",
        "current_model_approvals",
        "revoked_model_approvals",
        "model_approval_registry",
        "model_approval_consistency_checks.sql",
    ):
        assert required in documentation

    assert "model_approval_schema.sql" in compose
    assert "model_approval_contract_check" in workflow
    assert "model_approval_consistency_checks.sql" in workflow

    for prohibited_claim in (
        "external approval service is implemented",
        "automatically approves",
        "blocks trades",
    ):
        assert prohibited_claim not in documentation
