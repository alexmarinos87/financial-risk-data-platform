from __future__ import annotations

import hashlib
import json
import subprocess
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from scripts.overnight_manifest_verifier import (
    MAX_MANIFEST_BYTES,
    ManifestDenied,
    VerifierFailure,
    main,
    validate_manifest_blob,
    verify_protected_base_manifest,
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


def _git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["/usr/bin/git", *arguments], cwd=repository, check=True, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path, content: bytes | None = None, *, executable: bool = False) -> tuple[Path, str]:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "-q", "-b", "main")
    _git(repository, "config", "user.email", "tests@example.invalid")
    _git(repository, "config", "user.name", "Tests")
    path = repository / "docs" / "overnight-manifests" / f"{MANIFEST_ID}.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(content if content is not None else _blob())
    if executable:
        path.chmod(0o755)
    _git(repository, "add", path.relative_to(repository).as_posix())
    _git(repository, "commit", "-q", "-m", "Add manifest")
    base = _git(repository, "rev-parse", "HEAD")
    _git(repository, "update-ref", "refs/remotes/origin/main", base)
    return repository, base


def test_git_binding_uses_exact_blob_without_mutating_worktree_or_trusting_git_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, base = _repository(tmp_path)
    path = repository / "docs" / "overnight-manifests" / f"{MANIFEST_ID}.json"
    path.write_text("dirty worktree content", encoding="utf-8")
    status_before = _git(repository, "status", "--porcelain=v1", "-uall")
    from scripts import overnight_manifest_verifier as verifier

    original_popen = verifier.subprocess.Popen
    child_environments: list[dict[str, str]] = []

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[bytes]:
        if "env" in kwargs:
            child_environments.append(dict(kwargs["env"]))
        return original_popen(*args, **kwargs)

    monkeypatch.setattr(verifier.subprocess, "Popen", recording_popen)
    for key in ("GIT_DIR", "LD_PRELOAD", "AWS_SECRET_ACCESS_KEY", "GH_TOKEN", "SSH_AUTH_SOCK"):
        monkeypatch.setenv(key, "sentinel-secret-invalid")

    binding = verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)

    evidence = binding.redacted_evidence()
    assert evidence["publication_authorized"] is False
    assert evidence["object_format"] == "sha1"
    assert evidence["manifest_sha256"] == hashlib.sha256(_blob()).hexdigest()
    assert "Calculate daily risk" not in json.dumps(evidence)
    assert child_environments
    assert all(
        not {"GIT_DIR", "LD_PRELOAD", "AWS_SECRET_ACCESS_KEY", "GH_TOKEN", "SSH_AUTH_SOCK"}
        & environment.keys()
        for environment in child_environments
    )
    assert all(environment["GIT_ALLOW_PROTOCOL"] == "" for environment in child_environments)
    for key in ("GIT_DIR", "LD_PRELOAD", "AWS_SECRET_ACCESS_KEY", "GH_TOKEN", "SSH_AUTH_SOCK"):
        monkeypatch.delenv(key)
    assert _git(repository, "status", "--porcelain=v1", "-uall") == status_before


@pytest.mark.parametrize(
    ("content", "executable", "reason"),
    [
        (_blob(), True, "MANIFEST_BLOB_INVALID"),
        (b"x" * (MAX_MANIFEST_BYTES + 1), False, "MANIFEST_TOO_LARGE"),
        (b"\xff", False, "INVALID_JSON"),
    ],
)
def test_git_binding_rejects_wrong_mode_oversize_and_invalid_utf8(
    tmp_path: Path, content: bytes, executable: bool, reason: str
) -> None:
    repository, base = _repository(tmp_path, content, executable=executable)
    with pytest.raises(ManifestDenied, match=reason):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)


def test_git_binding_rejects_base_movement_abbreviation_and_symlink_root(
    tmp_path: Path,
) -> None:
    repository, base = _repository(tmp_path)
    other = repository / "other.txt"
    other.write_text("move", encoding="utf-8")
    _git(repository, "add", "other.txt")
    _git(repository, "commit", "-q", "-m", "Move base")
    _git(repository, "update-ref", "refs/remotes/origin/main", "HEAD")
    with pytest.raises(ManifestDenied, match="BASE_NOT_CURRENT"):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)
    with pytest.raises(ManifestDenied, match="INVALID_BASE_SHA"):
        verify_protected_base_manifest(repository, base[:12], MANIFEST_ID, now=NOW)
    link = tmp_path / "repo-link"
    link.symlink_to(repository, target_is_directory=True)
    with pytest.raises(VerifierFailure):
        verify_protected_base_manifest(link, base, MANIFEST_ID, now=NOW)


@pytest.mark.parametrize(
    ("key", "value"),
    [("extensions.partialClone", "origin"), ("remote.origin.promisor", "true")],
)
def test_git_binding_rejects_partial_clone_configuration_before_object_reads(
    tmp_path: Path, key: str, value: str
) -> None:
    repository, base = _repository(tmp_path)
    _git(repository, "config", key, value)
    objects_before = sorted(
        path.relative_to(repository) for path in (repository / ".git/objects").rglob("*")
    )

    with pytest.raises(VerifierFailure):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)

    assert sorted(
        path.relative_to(repository) for path in (repository / ".git/objects").rglob("*")
    ) == objects_before


def test_git_binding_rejects_blob_bytes_that_do_not_match_tree_oid(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    manifest_path = f"docs/overnight-manifests/{MANIFEST_ID}.json"
    blob_oid = _git(repository, "rev-parse", f"HEAD:{manifest_path}")
    altered = _blob().replace(b"Calculate", b"Fabricate")
    assert len(altered) == len(_blob())
    loose_object = repository / ".git" / "objects" / blob_oid[:2] / blob_oid[2:]
    loose_object.chmod(0o600)
    loose_object.write_bytes(zlib.compress(f"blob {len(altered)}\0".encode() + altered))

    with pytest.raises(VerifierFailure):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)


def test_git_binding_treats_broken_tracking_ref_as_operational_failure(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    tracking_ref = repository / ".git" / "refs" / "remotes" / "origin" / "main"
    tracking_ref.write_text("not-an-object\n", encoding="ascii")

    with pytest.raises(VerifierFailure):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)


def test_git_reader_enforces_output_cap_and_falsey_clock_is_not_defaulted(
    tmp_path: Path,
) -> None:
    repository, base = _repository(tmp_path)
    from scripts import overnight_manifest_verifier as verifier

    blob_oid = _git(
        repository, "rev-parse", f"HEAD:docs/overnight-manifests/{MANIFEST_ID}.json"
    )
    with pytest.raises(ManifestDenied, match="MANIFEST_TOO_LARGE"):
        verifier._run_git(
            repository,
            "cat-file",
            "blob",
            blob_oid,
            maximum_output_bytes=8,
            excess_reason="MANIFEST_TOO_LARGE",
        )
    with pytest.raises(VerifierFailure):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=False)  # type: ignore[arg-type]


def test_git_binding_detects_remote_ref_moving_during_verification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, base = _repository(tmp_path)
    from scripts import overnight_manifest_verifier as verifier

    original = verifier._run_git
    base_reads = 0

    def moving_ref(repo: Path, *arguments: str, **options: Any) -> bytes:
        nonlocal base_reads
        if arguments[-1:] == ("refs/remotes/origin/main",):
            base_reads += 1
            if base_reads == 2:
                return ("0" * 40 + "\n").encode()
        return original(repo, *arguments, **options)

    monkeypatch.setattr(verifier, "_run_git", moving_ref)
    with pytest.raises(ManifestDenied, match="BASE_MOVED"):
        verify_protected_base_manifest(repository, base, MANIFEST_ID, now=NOW)


def test_cli_denial_is_fixed_and_redacted(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    repository, _ = _repository(tmp_path)
    result = main([
        "--repository", str(repository), "--base-sha", "sentinel-secret",
        "--manifest-id", MANIFEST_ID,
    ])
    output = capsys.readouterr()

    assert result == 2
    assert output.out == ""
    assert json.loads(output.err) == {"status": "denied", "reason": "INVALID_BASE_SHA"}
    assert "sentinel-secret" not in output.err


@pytest.mark.parametrize("arguments", [[], ["--unknown", "sentinel-secret-value"]])
def test_cli_argument_errors_are_redacted_operational_failures(
    arguments: list[str], capsys: pytest.CaptureFixture[str]
) -> None:
    result = main(arguments)
    output = capsys.readouterr()

    assert result == 1
    assert output.out == ""
    assert json.loads(output.err) == {"status": "error", "reason": "VERIFIER_FAILURE"}
    assert "sentinel-secret-value" not in output.err


def test_cli_success_and_operational_failure_are_distinct(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    current = datetime.now(UTC).replace(microsecond=0)
    repository, base = _repository(
        tmp_path,
        _blob(
            authorization_issued_at=(current - timedelta(hours=1)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            authorization_expires=(current + timedelta(hours=3)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        ),
    )
    result = main([
        "--repository", str(repository), "--base-sha", base, "--manifest-id", MANIFEST_ID,
    ])
    success = capsys.readouterr()
    assert result == 0
    assert json.loads(success.out)["publication_authorized"] is False
    assert success.err == ""

    _git(repository, "config", "remote.origin.promisor", "true")
    result = main([
        "--repository", str(repository), "--base-sha", base, "--manifest-id", MANIFEST_ID,
    ])
    failure = capsys.readouterr()
    assert result == 1
    assert failure.out == ""
    assert json.loads(failure.err) == {"status": "error", "reason": "VERIFIER_FAILURE"}
