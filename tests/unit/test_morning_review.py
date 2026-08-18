from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.morning_review import (
    ReviewError,
    classify_path,
    code_span,
    collect_overnight_evidence,
    decode_git,
    generate_package,
    parse_name_status,
    prepare_output_dir,
)


NOW = datetime(2026, 8, 14, 9, 0, tzinfo=timezone.utc)


def git(root: Path, *args: str) -> bytes:
    return subprocess.run(
        ["git", *args],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout


def write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def initialise_repo(root: Path) -> str:
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Review Test")
    git(root, "config", "user.email", "review@example.com")
    write(root / ".gitignore", ".sandbox/\n")
    write(root / "tracked.txt", "one\ntwo\n")
    git(root, "add", ".gitignore", "tracked.txt")
    git(root, "commit", "-m", "baseline")
    return decode_git(git(root, "rev-parse", "HEAD")).strip()


def overnight_payload(head: str, status_hash: str, finished_at: datetime) -> dict[str, object]:
    commands = [
        {"name": "git-status", "command": ["git", "status", "--short", "--branch"], "returncode": 0},
        {"name": "security-check", "command": ["make", "security-check"], "returncode": 0},
        {"name": "readiness-check", "command": ["make", "readiness-check"], "returncode": 0},
    ]
    return {
        "run_id": "run-1",
        "started_at": (finished_at - timedelta(minutes=5)).isoformat(),
        "finished_at": finished_at.isoformat(),
        "status": "passed",
        "git_head": head,
        "git_status_sha256": status_hash,
        "guardrails": {
            "push": False,
            "merge": False,
            "deploy": False,
            "terraform_apply": False,
            "cloud_environment_allowed": False,
        },
        "cycles": [{"status": "passed", "commands": commands}],
    }


def test_generate_package_captures_separate_git_states_and_preserves_status(
    tmp_path: Path,
) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    merge_base = initialise_repo(root)
    git(root, "checkout", "-b", "feature")
    write(root / "tracked.txt", "committed\ntwo\n")
    git(root, "add", "tracked.txt")
    git(root, "commit", "-m", "feature change")
    feature_head = decode_git(git(root, "rev-parse", "HEAD")).strip()

    git(root, "checkout", "main")
    write(root / "main-only.txt", "main\n")
    git(root, "add", "main-only.txt")
    git(root, "commit", "-m", "diverge main")
    main_head = decode_git(git(root, "rev-parse", "HEAD")).strip()
    git(root, "checkout", "feature")

    write(root / "staged.txt", "staged\n")
    git(root, "add", "staged.txt")
    write(root / "tracked.txt", "committed\nunstaged\n")
    write(root / "infra" / "terraform" / "new.tf", "resource {}\n")
    before = git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")

    report_path, evidence_path, evidence = generate_package(
        root=root,
        base_ref="main",
        requested_output=None,
        now=NOW,
        stale_hours=24,
    )

    after = git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    assert before == after
    assert evidence["decision"] == "PENDING"
    assert evidence["git"]["path_status_unchanged"] is True
    assert evidence["git"]["base"]["base_sha"] == main_head
    assert evidence["git"]["base"]["merge_base_sha"] == merge_base
    assert evidence["git"]["start"]["head_sha"] == feature_head
    assert evidence["git"]["inventories"]["committed"]
    assert evidence["git"]["inventories"]["staged"]
    assert evidence["git"]["inventories"]["unstaged"]
    assert "infra/terraform/new.tf" in evidence["git"]["inventories"]["untracked"]
    untracked_metadata = {
        item["path"]: item for item in evidence["git"]["inventories"]["untracked_metadata"]
    }
    assert untracked_metadata["infra/terraform/new.tf"]["line_count"] == 1
    assert evidence["risk_flags"][0]["path"] == "infra/terraform/new.tf"
    assert report_path.is_file()
    assert evidence_path.is_file()
    assert "PENDING — human acceptance required" in report_path.read_text(encoding="utf-8")
    assert json.loads(evidence_path.read_text(encoding="utf-8"))["decision"] == "PENDING"
    assert report_path.stat().st_mode & 0o777 == 0o600
    assert report_path.parent.stat().st_mode & 0o777 == 0o700


def test_invalid_base_still_reports_worktree_without_claiming_clean(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    initialise_repo(root)
    write(root / "untracked.txt", "candidate\n")

    _, _, evidence = generate_package(root, "missing-ref", None, NOW, 24)

    assert evidence["git"]["base"]["status"] == "MISSING"
    assert evidence["git"]["inventories"]["untracked"] == ["untracked.txt"]


def test_origin_main_base_exposes_commits_ahead_on_local_main(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    baseline = initialise_repo(root)
    git(root, "update-ref", "refs/remotes/origin/main", baseline)
    write(root / "ahead.txt", "local commit\n")
    git(root, "add", "ahead.txt")
    git(root, "commit", "-m", "local main ahead")

    _, _, evidence = generate_package(root, "origin/main", None, NOW, 24)

    committed_paths = {
        path
        for entry in evidence["git"]["inventories"]["committed"]
        for path in entry["paths"]
    }
    assert committed_paths == {"ahead.txt"}


@pytest.mark.parametrize(
    ("payload_change", "expected_status"),
    [
        ({}, "STALE"),
        ({"git_head": "different"}, "STALE"),
        ({"cycles": []}, "FAIL"),
        ({"status": "failed"}, "FAIL"),
    ],
)
def test_overnight_evidence_is_fail_closed(
    tmp_path: Path,
    payload_change: dict[str, object],
    expected_status: str,
) -> None:
    root = tmp_path
    summary_path = root / ".sandbox" / "overnight" / "20260814-080000" / "summary.json"
    summary_path.parent.mkdir(parents=True)
    payload = overnight_payload("head", "status", NOW - timedelta(hours=1))
    payload.update(payload_change)
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = collect_overnight_evidence(root, NOW, stale_hours=24)

    assert evidence["status"] == expected_status


def test_overnight_evidence_rejects_wrong_commands_unsafe_guardrails_and_future_time(
    tmp_path: Path,
) -> None:
    summary_path = tmp_path / ".sandbox" / "overnight" / "run" / "summary.json"
    summary_path.parent.mkdir(parents=True)

    wrong_command = overnight_payload("head", "status", NOW - timedelta(hours=1))
    wrong_command["cycles"][0]["commands"][1]["command"] = ["true"]  # type: ignore[index]
    summary_path.write_text(json.dumps(wrong_command), encoding="utf-8")
    assert collect_overnight_evidence(tmp_path, NOW, 24)["status"] == "FAIL"

    unsafe = overnight_payload("head", "status", NOW - timedelta(hours=1))
    unsafe["guardrails"]["deploy"] = True  # type: ignore[index]
    summary_path.write_text(json.dumps(unsafe), encoding="utf-8")
    assert collect_overnight_evidence(tmp_path, NOW, 24)["status"] == "FAIL"

    future = overnight_payload("head", "status", NOW + timedelta(minutes=1))
    summary_path.write_text(json.dumps(future), encoding="utf-8")
    evidence = collect_overnight_evidence(tmp_path, NOW, 24)
    assert evidence["status"] == "FAIL"
    assert "future" in evidence["reason"]


def test_old_or_unattributed_overnight_evidence_is_stale(tmp_path: Path) -> None:
    summary_path = tmp_path / ".sandbox" / "overnight" / "20260814-080000" / "summary.json"
    summary_path.parent.mkdir(parents=True)
    payload = overnight_payload("head", "status", NOW - timedelta(days=2))
    del payload["git_status_sha256"]
    summary_path.write_text(json.dumps(payload), encoding="utf-8")

    evidence = collect_overnight_evidence(tmp_path, NOW, stale_hours=24)

    assert evidence["status"] == "STALE"
    assert evidence["age_hours"] == 48.0


def test_missing_malformed_and_incomplete_overnight_runs_are_not_passes(tmp_path: Path) -> None:
    assert collect_overnight_evidence(tmp_path, NOW, 24)["status"] == "MISSING"

    run_dir = tmp_path / ".sandbox" / "overnight" / "20260814-080000"
    run_dir.mkdir(parents=True)
    assert collect_overnight_evidence(tmp_path, NOW, 24)["status"] == "FAIL"

    (run_dir / "summary.json").write_text("{not-json", encoding="utf-8")
    assert collect_overnight_evidence(tmp_path, NOW, 24)["status"] == "FAIL"


def test_overnight_evidence_rejects_symlinked_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / ".sandbox" / "overnight" / "run"
    run_dir.mkdir(parents=True)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(overnight_payload("head", "status", NOW)), encoding="utf-8")
    os.symlink(outside, run_dir / "summary.json")

    evidence = collect_overnight_evidence(tmp_path, NOW, 24)

    assert evidence["status"] == "FAIL"


def test_overnight_evidence_rejects_symlinked_sandbox_ancestor(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    outside = tmp_path / "outside"
    run_dir = outside / "overnight" / "run"
    run_dir.mkdir(parents=True)
    (run_dir / "summary.json").write_text(
        json.dumps(overnight_payload("head", "status", NOW)),
        encoding="utf-8",
    )
    os.symlink(outside, root / ".sandbox")

    evidence = collect_overnight_evidence(root, NOW, 24)

    assert evidence["status"] == "FAIL"
    assert ".sandbox escapes" in evidence["reason"]


def test_output_boundary_rejects_escape_collision_and_symlink(tmp_path: Path) -> None:
    root = tmp_path / "repo"
    root.mkdir()
    initialise_repo(root)

    with pytest.raises(ReviewError, match="stay beneath"):
        prepare_output_dir(root, Path("../escape"), NOW)

    output = prepare_output_dir(root, Path("fixed"), NOW)
    assert output == root / ".sandbox" / "review-packages" / "fixed"
    with pytest.raises(ReviewError, match="already exists"):
        prepare_output_dir(root, Path("fixed"), NOW)

    other_root = tmp_path / "symlink-repo"
    other_root.mkdir()
    initialise_repo(other_root)
    (other_root / ".sandbox").mkdir()
    target = tmp_path / "outside"
    target.mkdir()
    os.symlink(target, other_root / ".sandbox" / "review-packages")
    with pytest.raises(ReviewError, match="escapes"):
        prepare_output_dir(other_root, None, NOW)


def test_git_parsing_and_markdown_rendering_handle_hostile_names() -> None:
    entries = parse_name_status(b"R100\0old.tf\0new`name\n.tf\0D\0gone.pem\0")

    assert entries == [
        {"status": "R100", "paths": ["old.tf", "new`name\n.tf"]},
        {"status": "D", "paths": ["gone.pem"]},
    ]
    rendered = code_span("new`name\n.tf")
    assert "\n" not in rendered
    assert "\\x0a" in rendered
    assert rendered.startswith("``")
    assert "\\xff" in decode_git(b"bad\xffname")


def test_sensitive_path_matching_uses_file_and_directory_boundaries() -> None:
    assert classify_path("infra/new.tf") is not None
    assert classify_path("infrastructure.txt") is None
    assert classify_path("nested/.env.production") is not None
    assert classify_path("scripts/morning_review.py") is not None
    assert classify_path("config/environments.yaml") is not None
    assert classify_path("Dockerfile") is not None
    assert classify_path("credentials.json") is not None
    assert classify_path("policies/iam.json") is not None
    assert classify_path("src/aws/client.py") is not None
