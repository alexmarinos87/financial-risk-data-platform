from pathlib import Path


def test_readiness_override_documentation_preserves_human_authority_boundary() -> None:
    documentation = Path("docs/operational-readiness-overrides.md").read_text(
        encoding="utf-8"
    )

    for required in (
        "mandatory expiry of no more than 24 hours",
        "An `allow` decision cannot be overridden",
        "operational_readiness_override_history",
        "current_operational_readiness_override_status",
        "active_operational_readiness_overrides",
        "explicit evaluation timestamp",
        "records authority but does not consume it",
    ):
        assert required in documentation
