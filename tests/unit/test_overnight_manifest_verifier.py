from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from scripts.overnight_manifest_verifier import (
    MAX_MANIFEST_BYTES,
    ManifestDenied,
    validate_manifest_blob,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 15, 20, tzinfo=UTC)
MANIFEST_ID = "frdp-analytics-daily-risk-20260815"


def _manifest(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "schema_version": 2,
        "manifest_id": MANIFEST_ID,
        "status": "approved for overnight development",
        "authorization_issued_at": "2026-08-15T19:00:00Z",
        "authorization_expires": "2026-08-15T22:00:00Z",
        "repository": "alexmarinos87/financial-risk-data-platform",
        "protected_base_branch": "main",
        "arc42_primary_block": "analytics",
        "context_goal": "Calculate daily risk from validated prices",
        "allowed_paths": ["src/analytics/daily_risk.py", "tests/unit/test_daily_risk.py"],
        "interfaces_crossed": [],
        "runtime_scenario": "Daily prices produce one deterministic risk result",
        "quality_scenario": "Golden vectors match within the documented tolerance",
        "recovery_scenario": "A failed check leaves no published candidate",
        "acceptance_criteria": ["Golden daily return vectors pass"],
        "validation_targets": [
            "security-check", "quality-check", "readiness-check", "git-diff-check"
        ],
        "maximum_changed_lines": 500,
        "maximum_changed_files": 2,
        "maximum_commits": 3,
        "maximum_pushes": 3,
        "maximum_runtime_minutes": 120,
        "retry_policy": "human-renewed-manifest-only",
        "risk": "low",
        "draft_pr_publication": "eligible-after-global-activation",
    }
    value.update(changes)
    return value


def _blob(**changes: object) -> bytes:
    return json.dumps(_manifest(**changes), separators=(",", ":")).encode()


def _reason(blob: bytes, expected_id: str = MANIFEST_ID) -> str:
    with pytest.raises(ManifestDenied) as raised:
        validate_manifest_blob(blob, expected_id, NOW)
    return raised.value.reason


def test_valid_manifest_contract_is_evidence_not_publication_authority() -> None:
    result = validate_manifest_blob(_blob(), MANIFEST_ID, NOW)
    assert result.arc42_primary_block == "analytics"
    assert result.allowed_paths == ("src/analytics/daily_risk.py", "tests/unit/test_daily_risk.py")
    assert result.maximum_changed_lines == 500
    assert "Calculate daily risk" not in repr(result)


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("schema_version", True, "INVALID_SCHEMA"),
        ("status", "enabled", "INVALID_AUTHORITY"),
        ("arc42_primary_block", "ingestion", "INVALID_BLOCK"),
        ("context_goal", "   ", "INVALID_TEXT"),
        ("quality_scenario", " padded", "INVALID_TEXT"),
        ("interfaces_crossed", ["analytics->storage"], "CROSS_BLOCK_NOT_ALLOWED"),
        ("validation_targets", ["make deploy"], "INVALID_VALIDATION_PROFILE"),
        ("maximum_changed_lines", True, "INVALID_BUDGET"),
        ("maximum_changed_lines", 1.5, "INVALID_BUDGET"),
        ("maximum_changed_lines", 501, "INVALID_BUDGET"),
        ("maximum_changed_files", 3, "INVALID_BUDGET"),
        ("maximum_pushes", 4, "INVALID_BUDGET"),
        ("risk", "high", "INVALID_AUTHORITY"),
        ("authorization_issued_at", "2026-08-15T19:00:00+00:00", "INVALID_TIME"),
        ("authorization_expires", "2026-08-15T20:00:00Z", "AUTHORIZATION_INACTIVE"),
        ("authorization_expires", "2026-08-16T20:00:01Z", "AUTHORIZATION_INACTIVE"),
    ],
)
def test_manifest_rejects_invalid_authority_and_budgets(
    field: str, value: object, reason: str
) -> None:
    assert _reason(_blob(**{field: value})) == reason


@pytest.mark.parametrize(
    "path",
    [
        "../src/analytics/risk.py",
        "/src/analytics/risk.py",
        "src\\analytics\\risk.py",
        "src/analytics/*.py",
        "src/processing/risk.py",
        "src/analytics/data_quality.py",
        "tests/integration/test_risk.py",
        "tests/unit/risk_test.py",
        ".github/workflows/ci.yml",
        "docs/overnight-development.md",
        "tests/.env",
    ],
)
def test_manifest_rejects_unsafe_sensitive_and_cross_block_paths(path: str) -> None:
    assert _reason(_blob(allowed_paths=[path], maximum_changed_files=1)) == "INVALID_PATH"


def test_manifest_requires_exact_schema_strict_json_and_matching_id() -> None:
    unknown = _manifest(extra=False)
    missing = _manifest()
    del missing["risk"]
    duplicate = _blob().replace(b'"status":', b'"status":"duplicate","status":', 1)
    escaped_duplicate = _blob().replace(b'"status":', b'"status":"duplicate","\\u0073tatus":', 1)

    assert _reason(json.dumps(unknown).encode()) == "INVALID_SCHEMA"
    assert _reason(json.dumps(missing).encode()) == "INVALID_SCHEMA"
    assert _reason(duplicate) == "INVALID_JSON"
    assert _reason(escaped_duplicate) == "INVALID_JSON"
    assert _reason(b'{"schema_version":NaN}') == "INVALID_JSON"
    assert _reason(b"\xff") == "INVALID_JSON"
    assert _reason(b"[" * 1_200 + b"0" + b"]" * 1_200) == "INVALID_JSON"
    assert _reason(_blob(), "different-valid-id") == "INVALID_MANIFEST_ID"
    assert _reason(b"x" * (MAX_MANIFEST_BYTES + 1)) == "MANIFEST_TOO_LARGE"


def test_manifest_requires_test_path_sorted_unique_paths_and_runtime_window() -> None:
    assert _reason(_blob(allowed_paths=["src/analytics/risk.py"], maximum_changed_files=1)) == "TEST_PATH_REQUIRED"
    paths = ["tests/unit/test_risk.py", "src/analytics/risk.py"]
    assert _reason(_blob(allowed_paths=paths)) == "INVALID_PATH"
    duplicate = ["src/analytics/risk.py", "tests/unit/test_risk.py", "tests/unit/test_risk.py"]
    assert _reason(_blob(allowed_paths=duplicate)) == "INVALID_LIST"
    assert _reason(_blob(maximum_runtime_minutes=121)) == "INVALID_BUDGET"
    assert _reason(_blob(authorization_expires="2026-08-15T21:00:00Z")) == (
        "RUNTIME_EXCEEDS_AUTHORIZATION"
    )
