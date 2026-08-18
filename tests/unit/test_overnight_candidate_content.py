from __future__ import annotations

import itertools
import json
import os
import subprocess
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import pytest

from scripts import overnight_candidate_content as content
from scripts.overnight_candidate_content import verify_committed_candidate_content
from scripts.overnight_manifest_verifier import ManifestDenied, VerifierFailure


UTC = timezone.utc
NOW = datetime(2026, 8, 15, 20, tzinfo=UTC)
MANIFEST_ID = "frdp-analytics-daily-risk-20260815"
SOURCE_PATH = "src/analytics/daily_risk.py"
TEST_PATH = "tests/unit/test_daily_risk.py"


def _manifest(maximum_changed_lines: int = 20) -> bytes:
    value = {
        "schema_version": 2,
        "manifest_id": MANIFEST_ID,
        "status": "approved for overnight development",
        "authorization_issued_at": "2026-08-15T19:00:00Z",
        "authorization_expires": "2026-08-15T22:00:00Z",
        "repository": "alexmarinos87/financial-risk-data-platform",
        "protected_base_branch": "main",
        "arc42_primary_block": "analytics",
        "context_goal": "Calculate daily risk from validated prices",
        "allowed_paths": [SOURCE_PATH, TEST_PATH],
        "interfaces_crossed": [],
        "runtime_scenario": "Daily prices produce one deterministic risk result",
        "quality_scenario": "Golden vectors match within the documented tolerance",
        "recovery_scenario": "A failed check leaves no published candidate",
        "acceptance_criteria": ["Golden daily return vectors pass"],
        "validation_targets": [
            "security-check", "quality-check", "readiness-check", "git-diff-check"
        ],
        "maximum_changed_lines": maximum_changed_lines,
        "maximum_changed_files": 2,
        "maximum_commits": 3,
        "maximum_pushes": 3,
        "maximum_runtime_minutes": 120,
        "retry_policy": "human-renewed-manifest-only",
        "risk": "low",
        "draft_pr_publication": "eligible-after-global-activation",
    }
    return json.dumps(value, separators=(",", ":")).encode()


def _git(repository: Path, *arguments: str) -> str:
    environment = dict(os.environ)
    environment.update({
        "GIT_AUTHOR_DATE": "2000-01-01T00:00:00Z",
        "GIT_COMMITTER_DATE": "2000-01-01T00:00:00Z",
    })
    result = subprocess.run(
        ["/usr/bin/git", *arguments], cwd=repository, env=environment, check=True,
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    return result.stdout.strip()


def _repository(tmp_path: Path, maximum_changed_lines: int = 20) -> tuple[Path, str]:
    repository = tmp_path / "repo"
    repository.mkdir()
    _git(repository, "init", "-q", "-b", "main")
    _git(repository, "config", "user.email", "tests@example.invalid")
    _git(repository, "config", "user.name", "Tests")
    manifest = repository / "docs/overnight-manifests" / f"{MANIFEST_ID}.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(_manifest(maximum_changed_lines))
    _git(repository, "add", "--", manifest.relative_to(repository).as_posix())
    _git(repository, "commit", "-q", "-m", "Add manifest")
    base = _git(repository, "rev-parse", "HEAD")
    _git(repository, "update-ref", "refs/remotes/origin/main", base)
    return repository, base


def _commit(repository: Path, path: str, value: bytes, message: str) -> str:
    target = repository / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(value)
    _git(repository, "add", "--", path)
    _git(repository, "commit", "-q", "-m", message)
    return _git(repository, "rev-parse", "HEAD")


def _two_commit_candidate(repository: Path) -> str:
    _commit(repository, SOURCE_PATH, b"alpha\nsentinel-source\n", "Add source")
    return _commit(repository, SOURCE_PATH, b"alpha\nchanged", "Change source")


def test_line_records_are_lf_exact_and_myers_matches_small_oracle() -> None:
    assert content._line_records(b"") == ()
    assert content._line_records(b"\n") == (b"\n",)
    assert content._line_records(b"a\n") == (b"a\n",)
    assert content._line_records(b"a") == (b"a",)
    assert content._line_records(b"a\r\nb\xe2\x80\xa8c") == (b"a\r\n", b"b\xe2\x80\xa8c")

    def oracle(old: tuple[bytes, ...], new: tuple[bytes, ...]) -> int:
        distances = list(range(len(new) + 1))
        for old_index, old_line in enumerate(old, 1):
            updated = [old_index]
            for new_index, new_line in enumerate(new, 1):
                updated.append(
                    distances[new_index - 1]
                    if old_line == new_line
                    else min(distances[new_index] + 1, updated[-1] + 1)
                )
            distances = updated
        return distances[-1]

    alphabet = (b"a\n", b"b\n")
    for old_size, new_size in itertools.product(range(4), repeat=2):
        for old in itertools.product(alphabet, repeat=old_size):
            for new in itertools.product(alphabet, repeat=new_size):
                assert content._bounded_edit_distance(
                    old, new, 20, content._EditWork()
                ) == oracle(old, new)


def test_content_observation_counts_every_edge_once_and_redacts_source(
    tmp_path: Path,
) -> None:
    repository, base = _repository(tmp_path, maximum_changed_lines=4)
    head = _two_commit_candidate(repository)

    result = verify_committed_candidate_content(repository, base, MANIFEST_ID, head, now=NOW)
    evidence = result.redacted_evidence()

    assert [(item.additions, item.deletions) for item in result.edge_line_counts] == [
        (2, 0), (1, 1)
    ]
    assert (result.cumulative_additions, result.cumulative_deletions) == (3, 1)
    assert result.cumulative_changed_lines == 4
    assert evidence["status"] == "candidate-content-observed"
    assert evidence["changed_line_budget_verified"] is True
    assert evidence["content_fingerprint_verified"] is True
    assert evidence["publication_authorized"] is False
    assert evidence["object_store_isolation_verified"] is False
    assert evidence["candidate_fingerprint"].startswith("sha256:")
    assert result.candidate_fingerprint_sha256 == (
        "68c228def5697e095e59e5730253660c1f44b38740dc79ca0baa64139766c821"
    )
    assert "sentinel-source" not in json.dumps(evidence)
    assert "sentinel-source" not in repr(result)

    (repository / SOURCE_PATH).write_text("dirty sentinel-source", encoding="utf-8")
    (repository / ".gitattributes").write_text("* diff=sentinel-source\n", encoding="utf-8")
    repeated = verify_committed_candidate_content(
        repository, base, MANIFEST_ID, head, now=NOW
    )
    assert repeated.candidate_fingerprint_sha256 == result.candidate_fingerprint_sha256


def test_content_observation_denies_one_line_over_budget(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path, maximum_changed_lines=3)
    head = _two_commit_candidate(repository)

    with pytest.raises(ManifestDenied, match="CANDIDATE_BUDGET_EXCEEDED"):
        verify_committed_candidate_content(repository, base, MANIFEST_ID, head, now=NOW)


def test_reverted_history_consumes_budget_and_changes_fingerprint(tmp_path: Path) -> None:
    repository, base = _repository(tmp_path)
    _git(repository, "checkout", "-q", "-b", "direct", base)
    _commit(repository, SOURCE_PATH, b"alpha\n", "Add final source")
    direct = _commit(repository, TEST_PATH, b"def test_value():\n    assert True\n", "Add test")
    direct_result = verify_committed_candidate_content(
        repository, base, MANIFEST_ID, direct, now=NOW
    )

    _git(repository, "checkout", "-q", "-b", "history", base)
    _commit(repository, SOURCE_PATH, b"alpha\n", "Add source")
    _commit(repository, SOURCE_PATH, b"temporary\n", "Temporary edit")
    _commit(repository, SOURCE_PATH, b"alpha\n", "Restore source")
    history = _git(repository, "rev-parse", "HEAD")
    # Put the same final test blob into the third commit without adding a fourth commit.
    (repository / TEST_PATH).parent.mkdir(parents=True, exist_ok=True)
    (repository / TEST_PATH).write_bytes(b"def test_value():\n    assert True\n")
    _git(repository, "add", "--", TEST_PATH)
    _git(repository, "commit", "--amend", "-q", "--no-edit")
    history = _git(repository, "rev-parse", "HEAD")
    history_result = verify_committed_candidate_content(
        repository, base, MANIFEST_ID, history, now=NOW
    )

    assert direct_result.history.candidate_tree_sha == history_result.history.candidate_tree_sha
    assert direct_result.cumulative_changed_lines == 3
    assert history_result.cumulative_changed_lines == 7
    assert direct_result.candidate_fingerprint_sha256 != (
        history_result.candidate_fingerprint_sha256
    )
    assert history_result.fingerprinted_blob_count == 3


def test_content_observation_rejects_oversized_line_and_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, base = _repository(tmp_path)
    head = _commit(
        repository, SOURCE_PATH, b"x" * (content.MAX_LINE_RECORD_BYTES + 1), "Long line"
    )
    with pytest.raises(ManifestDenied, match="CANDIDATE_DIFF_INVALID"):
        verify_committed_candidate_content(repository, base, MANIFEST_ID, head, now=NOW)

    monkeypatch.setattr(content, "MAX_EDIT_WORK_UNITS", 1)
    with pytest.raises(ManifestDenied, match="CANDIDATE_DIFF_INVALID"):
        content._bounded_edit_distance(
            (b"a\n", b"b\n"), (b"b\n", b"a\n"), 4, content._EditWork()
        )


def test_fingerprint_framing_is_unambiguous() -> None:
    first = content._Transcript()
    first.field("a", b"bc")
    second = content._Transcript()
    second.field("ab", b"c")

    assert first.hexdigest() != second.hexdigest()


def test_content_observation_rejects_malformed_history_capsule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, base = _repository(tmp_path)
    head = _two_commit_candidate(repository)
    valid = verify_committed_candidate_content(
        repository, base, MANIFEST_ID, head, now=NOW
    ).history
    first_edge = valid.edges[0]
    second_edge = valid.edges[1]
    first_commit = valid.commit_shas[0]
    first_digest = valid.commit_object_sha256s[0]
    conflicting_trees = tuple(sorted((
        *valid.tree_object_sha256s,
        (valid.tree_object_sha256s[0][0], "f" * 64),
    )))
    malformed = (
        replace(
            valid,
            edges=valid.edges[:1],
            commit_object_sha256s=(first_digest, "f" * 64),
        ),
        replace(
            valid,
            edges=(replace(first_edge, parent_commit_sha="0" * 40), *valid.edges[1:]),
        ),
        replace(
            valid,
            edges=(replace(first_edge, changes=()), *valid.edges[1:]),
        ),
        replace(
            valid,
            edges=(
                first_edge,
                replace(
                    second_edge,
                    changes=(replace(second_edge.changes[0], old=None),),
                ),
            ),
        ),
        replace(valid, tree_object_sha256s=conflicting_trees),
        replace(
            valid,
            manifest=replace(
                valid.manifest,
                contract=replace(valid.manifest.contract, maximum_commits=1),
            ),
        ),
        replace(
            valid,
            manifest=replace(
                valid.manifest,
                contract=replace(valid.manifest.contract, maximum_changed_files=0),
            ),
        ),
        replace(
            valid,
            candidate_commit_sha=first_commit,
            commit_shas=(first_commit, first_commit),
            commit_object_sha256s=(first_digest, first_digest),
            edges=(
                first_edge,
                replace(
                    second_edge,
                    parent_commit_sha=first_commit,
                    commit_sha=first_commit,
                ),
            ),
        ),
    )
    for invalid_history in malformed:
        monkeypatch.setattr(
            content,
            "verify_committed_candidate_history",
            lambda *_args, _history=invalid_history, **_kwargs: _history,
        )
        with pytest.raises(VerifierFailure):
            verify_committed_candidate_content(
                repository, base, MANIFEST_ID, head, now=NOW
            )


def test_content_observation_binds_requested_identifiers_and_blob_bytes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository, base = _repository(tmp_path)
    head = _commit(repository, SOURCE_PATH, b"alpha\n", "Add source")
    valid = verify_committed_candidate_content(
        repository, base, MANIFEST_ID, head, now=NOW
    ).history
    monkeypatch.setattr(
        content, "verify_committed_candidate_history", lambda *_args, **_kwargs: valid
    )
    for requested_base, requested_manifest, requested_head in (
        ("f" * 40, MANIFEST_ID, head),
        (base, "frdp-analytics-other-risk-20260815", head),
        (base, MANIFEST_ID, "e" * 40),
    ):
        with pytest.raises(VerifierFailure):
            verify_committed_candidate_content(
                repository,
                requested_base,
                requested_manifest,
                requested_head,
                now=NOW,
            )

    original_change = valid.edges[0].changes[0]
    assert original_change.new is not None
    changed_endpoint = replace(original_change.new, content=b"tampered\n")
    changed = replace(original_change, new=changed_endpoint)
    malformed = replace(
        valid,
        edges=(replace(valid.edges[0], changes=(changed,)),),
        final_changes=(changed,),
    )
    monkeypatch.setattr(
        content,
        "verify_committed_candidate_history",
        lambda *_args, **_kwargs: malformed,
    )
    with pytest.raises(VerifierFailure):
        verify_committed_candidate_content(
            repository, base, MANIFEST_ID, head, now=NOW
        )
