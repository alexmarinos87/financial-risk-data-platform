from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import select
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Any, NoReturn


MAX_MANIFEST_BYTES = 32 * 1024
MAX_GIT_METADATA_BYTES = 4 * 1024
MAX_CANDIDATE_BLOB_BYTES = 1024 * 1024
MAX_CANDIDATE_TOTAL_BLOB_BYTES = 4 * 1024 * 1024
MAX_COMMIT_OBJECT_BYTES = 64 * 1024
MAX_TREE_OBJECT_BYTES = 1024 * 1024
MAX_TOTAL_TREE_BYTES = 16 * 1024 * 1024
MAX_TREE_OBJECTS = 4096
MAX_EXPANDED_TREE_ENTRIES = 100_000
MAX_EXPANDED_TREE_PATH_BYTES = 64 * 1024 * 1024
MAX_TREE_PATH_BYTES = 4096
MAX_TREE_DEPTH = 128
GIT_TIMEOUT_SECONDS = 10.0
MAX_AUTHORIZATION_WINDOW = timedelta(hours=24)
UTC = timezone.utc
MANIFEST_ID = re.compile(r"[a-z0-9][a-z0-9-]{7,63}")
RFC3339_UTC = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
PATH_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,239}")

PRIMARY_ROOTS = MappingProxyType({
    "processing": "src/processing/", "analytics": "src/analytics/",
    "benchmarking": "src/benchmarks/",
})
VALIDATION_TARGETS = ("security-check", "quality-check", "readiness-check", "git-diff-check")
MANIFEST_KEYS = frozenset({
    "schema_version", "manifest_id", "status", "authorization_issued_at",
    "authorization_expires", "repository", "protected_base_branch",
    "arc42_primary_block", "context_goal", "allowed_paths", "interfaces_crossed",
    "runtime_scenario", "quality_scenario", "recovery_scenario", "acceptance_criteria",
    "validation_targets", "maximum_changed_lines", "maximum_changed_files",
    "maximum_commits", "maximum_pushes", "maximum_runtime_minutes", "retry_policy",
    "risk", "draft_pr_publication",
})
DENIED_EXACT_PATHS = frozenset({
    "AGENTS.md", "Dockerfile", "Makefile", "docker-compose.yml", "pyproject.toml",
    "docs/agent-roles.md", "docs/agentic-workflows.md", "docs/architecture.md",
    "docs/engineering-delivery-workflow.md", "docs/iteration-backlog.md",
    "docs/iteration-loop.md", "docs/overnight-development.md",
    "docs/security-protocols.md", "src/analytics/data_quality.py",
    "src/orchestration/backfill.py", "src/orchestration/locks.py",
})
DENIED_PREFIXES = (
    ".git", ".github/", "config/", "deploy/", "infra/", "mongo/", "scripts/",
    "sql/", "src/warehouse/", "docs/overnight-manifests/",
)
DENIAL_REASONS = frozenset({
    "INVALID_JSON", "MANIFEST_TOO_LARGE", "INVALID_TEXT", "INVALID_BUDGET",
    "INVALID_LIST", "INVALID_TIME", "INVALID_SCHEMA", "INVALID_MANIFEST_ID",
    "INVALID_AUTHORITY", "INVALID_BLOCK", "CROSS_BLOCK_NOT_ALLOWED", "INVALID_PATH",
    "PRIMARY_BLOCK_PATH_REQUIRED", "TEST_PATH_REQUIRED", "INVALID_VALIDATION_PROFILE",
    "AUTHORIZATION_INACTIVE", "RUNTIME_EXCEEDS_AUTHORIZATION", "INVALID_BASE_SHA",
    "BASE_NOT_CURRENT", "BASE_MOVED", "MANIFEST_BLOB_INVALID",
    "INVALID_CANDIDATE_SHA", "CANDIDATE_HISTORY_INVALID", "CANDIDATE_DIFF_INVALID",
    "CANDIDATE_BUDGET_EXCEEDED",
})
GIT_ENVIRONMENT = MappingProxyType({
    "GIT_ALLOW_PROTOCOL": "",
    "GIT_ATTR_NOSYSTEM": "1",
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_CONFIG_SYSTEM": "/dev/null",
    "GIT_NO_LAZY_FETCH": "1",
    "GIT_NO_REPLACE_OBJECTS": "1",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_PROTOCOL_FROM_USER": "0",
    "GIT_TERMINAL_PROMPT": "0",
    "HOME": "/nonexistent",
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
    "XDG_CONFIG_HOME": "/nonexistent",
})


class ManifestDenied(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class VerifierFailure(Exception):
    pass


@dataclass(frozen=True, slots=True, repr=False)
class ValidatedManifestContract:
    manifest_id: str
    authorization_issued_at: datetime
    authorization_expires: datetime
    arc42_primary_block: str
    context_goal: str
    allowed_paths: tuple[str, ...]
    runtime_scenario: str
    quality_scenario: str
    recovery_scenario: str
    acceptance_criteria: tuple[str, ...]
    validation_targets: tuple[str, ...]
    maximum_changed_lines: int
    maximum_changed_files: int
    maximum_commits: int
    maximum_pushes: int
    maximum_runtime_minutes: int


@dataclass(frozen=True, slots=True, repr=False)
class ProtectedBaseManifest:
    contract: ValidatedManifestContract
    base_commit_sha: str
    base_tree_sha: str
    manifest_path: str
    git_blob_oid: str
    manifest_sha256: str
    record_key: str
    object_format: str
    verified_at: datetime

    def redacted_evidence(self) -> dict[str, Any]:
        return {
            "status": "manifest-contract-valid",
            "publication_authorized": False,
            "manifest_repository": "alexmarinos87/financial-risk-data-platform",
            "declared_base_branch": "main",
            "local_tracking_ref": "refs/remotes/origin/main",
            "base_commit_sha": self.base_commit_sha,
            "base_tree_sha": self.base_tree_sha,
            "object_format": self.object_format,
            "manifest_path": self.manifest_path,
            "git_blob_oid": self.git_blob_oid,
            "manifest_sha256": self.manifest_sha256,
            "record_key": self.record_key,
            "manifest_id": self.contract.manifest_id,
            "arc42_primary_block": self.contract.arc42_primary_block,
            "allowed_paths": list(self.contract.allowed_paths),
            "maximum_changed_lines": self.contract.maximum_changed_lines,
            "maximum_changed_files": self.contract.maximum_changed_files,
            "maximum_commits": self.contract.maximum_commits,
            "maximum_pushes": self.contract.maximum_pushes,
            "maximum_runtime_minutes": self.contract.maximum_runtime_minutes,
            "verified_at": self.verified_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "authorization_expires": self.contract.authorization_expires.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


@dataclass(frozen=True, slots=True, repr=False)
class VerifiedFileContent:
    mode: str
    git_oid: str
    sha256: str
    content: bytes


@dataclass(frozen=True, slots=True, repr=False)
class CommittedPathChange:
    path: str
    old: VerifiedFileContent | None
    new: VerifiedFileContent | None


@dataclass(frozen=True, slots=True, repr=False)
class CommittedHistoryEdge:
    parent_commit_sha: str
    parent_tree_sha: str
    commit_sha: str
    commit_tree_sha: str
    changes: tuple[CommittedPathChange, ...]


@dataclass(frozen=True, slots=True, repr=False)
class CommittedCandidateHistory:
    manifest: ProtectedBaseManifest
    candidate_commit_sha: str
    candidate_tree_sha: str
    commit_shas: tuple[str, ...]
    base_commit_sha256: str
    commit_object_sha256s: tuple[str, ...]
    tree_object_sha256s: tuple[tuple[str, str], ...]
    edges: tuple[CommittedHistoryEdge, ...]
    final_changes: tuple[CommittedPathChange, ...]
    touched_paths: tuple[str, ...]
    final_paths: tuple[str, ...]
    verified_at: datetime

    def redacted_evidence(self) -> dict[str, Any]:
        return {
            "status": "candidate-history-scope-observed",
            "publication_authorized": False,
            "object_store_isolation_verified": False,
            "worktree_cleanliness_verified": False,
            "validation_verified": False,
            "changed_line_budget_verified": False,
            "content_fingerprint_verified": False,
            "push_budget_verified": False,
            "manifest_id": self.manifest.contract.manifest_id,
            "manifest_sha256": self.manifest.manifest_sha256,
            "record_key": self.manifest.record_key,
            "base_commit_sha": self.manifest.base_commit_sha,
            "base_tree_sha": self.manifest.base_tree_sha,
            "candidate_commit_sha": self.candidate_commit_sha,
            "candidate_tree_sha": self.candidate_tree_sha,
            "commit_shas": list(self.commit_shas),
            "commit_count": len(self.commit_shas),
            "touched_paths": list(self.touched_paths),
            "changed_file_count": len(self.touched_paths),
            "final_paths": list(self.final_paths),
            "maximum_changed_lines": self.manifest.contract.maximum_changed_lines,
            "maximum_changed_files": self.manifest.contract.maximum_changed_files,
            "maximum_commits": self.manifest.contract.maximum_commits,
            "verified_at": self.verified_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "authorization_expires": self.manifest.contract.authorization_expires.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        }


def _duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestDenied("INVALID_JSON")
        result[key] = value
    return result


def _reject_constant(_value: str) -> None:
    raise ManifestDenied("INVALID_JSON")


def _text(value: Any, maximum: int = 1_000) -> str:
    if type(value) is not str or not 1 <= len(value) <= maximum:
        raise ManifestDenied("INVALID_TEXT")
    if value != value.strip() or not value.isprintable():
        raise ManifestDenied("INVALID_TEXT")
    try:
        value.encode("utf-8")
    except UnicodeError:
        raise ManifestDenied("INVALID_TEXT") from None
    return value


def _integer(value: Any, minimum: int, maximum: int) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ManifestDenied("INVALID_BUDGET")
    return value


def _texts(value: Any, minimum: int, maximum: int, text_limit: int = 1_000) -> list[str]:
    if type(value) is not list or not minimum <= len(value) <= maximum:
        raise ManifestDenied("INVALID_LIST")
    result = [_text(item, text_limit) for item in value]
    if len(set(result)) != len(result):
        raise ManifestDenied("INVALID_LIST")
    return result


def _timestamp(value: Any) -> datetime:
    if type(value) is not str or RFC3339_UTC.fullmatch(value) is None:
        raise ManifestDenied("INVALID_TIME")
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        raise ManifestDenied("INVALID_TIME") from None


def _path_allowed(path: str, primary_root: str) -> bool:
    if (
        PATH_PATTERN.fullmatch(path) is None
        or path != path.lower()
        or path.endswith("/")
        or "//" in path
        or any(part in {"", ".", ".."} or part.startswith("-") for part in path.split("/"))
    ):
        return False
    lower = path.lower()
    if path in DENIED_EXACT_PATHS or any(path.startswith(prefix) for prefix in DENIED_PREFIXES):
        return False
    if any(
        part.startswith(".env")
        or part in {"credential", "credentials", "secret", "secrets", "id_rsa", "id_ed25519"}
        or part.endswith((".key", ".pem", ".p12"))
        for part in lower.split("/")
    ):
        return False
    if path.startswith(primary_root):
        return path.endswith(".py")
    if path.startswith("tests/unit/"):
        return path.rsplit("/", maxsplit=1)[-1].startswith("test_") and path.endswith(".py")
    return path.startswith("docs/") and path.endswith(".md")


def validate_manifest_blob(blob: bytes, expected_id: str, now: datetime) -> ValidatedManifestContract:
    if type(blob) is not bytes:
        raise ManifestDenied("INVALID_JSON")
    if len(blob) > MAX_MANIFEST_BYTES:
        raise ManifestDenied("MANIFEST_TOO_LARGE")
    if type(expected_id) is not str or MANIFEST_ID.fullmatch(expected_id) is None:
        raise ManifestDenied("INVALID_MANIFEST_ID")
    if type(now) is not datetime:
        raise VerifierFailure
    try:
        raw = json.loads(
            blob.decode("utf-8"),
            object_pairs_hook=_duplicate_keys,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError, RecursionError, TypeError, ValueError):
        raise ManifestDenied("INVALID_JSON") from None
    if type(raw) is not dict or raw.keys() != MANIFEST_KEYS:
        raise ManifestDenied("INVALID_SCHEMA")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != 2:
        raise ManifestDenied("INVALID_SCHEMA")
    if raw["manifest_id"] != expected_id:
        raise ManifestDenied("INVALID_MANIFEST_ID")

    fixed = {
        "status": "approved for overnight development",
        "repository": "alexmarinos87/financial-risk-data-platform",
        "protected_base_branch": "main",
        "retry_policy": "human-renewed-manifest-only",
        "risk": "low",
        "draft_pr_publication": "eligible-after-global-activation",
    }
    if any(type(raw[key]) is not str or raw[key] != value for key, value in fixed.items()):
        raise ManifestDenied("INVALID_AUTHORITY")
    block = raw["arc42_primary_block"]
    if type(block) is not str or block not in PRIMARY_ROOTS:
        raise ManifestDenied("INVALID_BLOCK")
    if type(raw["interfaces_crossed"]) is not list or raw["interfaces_crossed"] != []:
        raise ManifestDenied("CROSS_BLOCK_NOT_ALLOWED")

    context = _text(raw["context_goal"])
    runtime_scenario = _text(raw["runtime_scenario"])
    quality_scenario = _text(raw["quality_scenario"])
    recovery_scenario = _text(raw["recovery_scenario"])
    criteria = _texts(raw["acceptance_criteria"], 1, 10)
    paths = _texts(raw["allowed_paths"], 1, 12, 240)
    if paths != sorted(paths) or any(not _path_allowed(path, PRIMARY_ROOTS[block]) for path in paths):
        raise ManifestDenied("INVALID_PATH")
    if not any(path.startswith(PRIMARY_ROOTS[block]) for path in paths):
        raise ManifestDenied("PRIMARY_BLOCK_PATH_REQUIRED")
    if not any(path.startswith("tests/unit/") for path in paths):
        raise ManifestDenied("TEST_PATH_REQUIRED")
    if raw["validation_targets"] != list(VALIDATION_TARGETS) or any(
        type(item) is not str for item in raw["validation_targets"]
    ):
        raise ManifestDenied("INVALID_VALIDATION_PROFILE")

    lines = _integer(raw["maximum_changed_lines"], 1, 500)
    files = _integer(raw["maximum_changed_files"], 1, min(12, len(paths)))
    commits = _integer(raw["maximum_commits"], 1, 3)
    pushes = _integer(raw["maximum_pushes"], 1, commits)
    runtime = _integer(raw["maximum_runtime_minutes"], 1, 120)

    try:
        clock_is_aware = now.tzinfo is not None and now.utcoffset() is not None
        verified_at = now.astimezone(UTC)
    except Exception:
        raise VerifierFailure from None
    if not clock_is_aware:
        raise VerifierFailure
    issued = _timestamp(raw["authorization_issued_at"])
    expires = _timestamp(raw["authorization_expires"])
    if not issued <= verified_at < expires or expires - issued > MAX_AUTHORIZATION_WINDOW:
        raise ManifestDenied("AUTHORIZATION_INACTIVE")
    if verified_at + timedelta(minutes=runtime) > expires:
        raise ManifestDenied("RUNTIME_EXCEEDS_AUTHORIZATION")

    return ValidatedManifestContract(
        manifest_id=expected_id,
        authorization_issued_at=issued,
        authorization_expires=expires,
        arc42_primary_block=block,
        context_goal=context,
        allowed_paths=tuple(paths),
        runtime_scenario=runtime_scenario,
        quality_scenario=quality_scenario,
        recovery_scenario=recovery_scenario,
        acceptance_criteria=tuple(criteria),
        validation_targets=VALIDATION_TARGETS,
        maximum_changed_lines=lines,
        maximum_changed_files=files,
        maximum_commits=commits,
        maximum_pushes=pushes,
        maximum_runtime_minutes=runtime,
    )


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    try:
        process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=1)
    except (OSError, subprocess.TimeoutExpired):
        pass


def _run_git(
    repository: Path,
    *arguments: str,
    maximum_output_bytes: int = MAX_GIT_METADATA_BYTES,
    allowed_returncodes: tuple[int, ...] = (0,),
    excess_reason: str | None = None,
) -> bytes:
    process: subprocess.Popen[bytes] | None = None
    try:
        process = subprocess.Popen(
            ["/usr/bin/git", *arguments],
            cwd=repository,
            env=dict(GIT_ENVIRONMENT),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        if process.stdout is None:
            raise VerifierFailure
        deadline = time.monotonic() + GIT_TIMEOUT_SECONDS
        output = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise VerifierFailure
            readable, _, _ = select.select((process.stdout,), (), (), remaining)
            if not readable:
                raise VerifierFailure
            chunk = os.read(
                process.stdout.fileno(),
                min(8 * 1024, maximum_output_bytes + 1 - len(output)),
            )
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > maximum_output_bytes:
                if excess_reason is not None:
                    raise ManifestDenied(excess_reason)
                raise VerifierFailure
        returncode = process.wait(timeout=max(0.001, deadline - time.monotonic()))
    except ManifestDenied:
        if process is not None:
            _stop_process(process)
        raise
    except VerifierFailure:
        if process is not None:
            _stop_process(process)
        raise
    except (OSError, ValueError, subprocess.SubprocessError):
        if process is not None:
            _stop_process(process)
        raise VerifierFailure from None
    finally:
        if process is not None and process.stdout is not None:
            process.stdout.close()
    if returncode not in allowed_returncodes:
        raise VerifierFailure
    return bytes(output)


def _git_object_oid(object_format: str, kind: str, content: bytes) -> str:
    hasher = hashlib.new(object_format, usedforsecurity=False)
    hasher.update(f"{kind} {len(content)}\0".encode("ascii"))
    hasher.update(content)
    return hasher.hexdigest()


def _ascii(value: bytes) -> str:
    try:
        decoded = value.decode("ascii")
    except UnicodeError:
        raise VerifierFailure from None
    if not decoded.endswith("\n") or "\n" in decoded[:-1]:
        raise VerifierFailure
    return decoded[:-1]


def verify_protected_base_manifest(
    repository: Path, base_sha: str, manifest_id: str, *, now: datetime | None = None
) -> ProtectedBaseManifest:
    if now is not None and type(now) is not datetime:
        raise VerifierFailure
    try:
        if not isinstance(repository, Path):
            raise VerifierFailure
        absolute_repository = Path(os.path.abspath(repository))
        if any(component.is_symlink() for component in (absolute_repository, *absolute_repository.parents)):
            raise VerifierFailure
        trusted_repository = absolute_repository.resolve(strict=True)
        if trusted_repository != absolute_repository:
            raise VerifierFailure
    except (OSError, RuntimeError):
        raise VerifierFailure from None
    if not trusted_repository.is_dir():
        raise VerifierFailure

    top_level_output = _run_git(
        trusted_repository, "rev-parse", "--path-format=absolute", "--show-toplevel"
    )
    if (
        not top_level_output.endswith(b"\n")
        or b"\n" in top_level_output[:-1]
        or b"\0" in top_level_output
        or Path(os.fsdecode(top_level_output[:-1])) != trusted_repository
    ):
        raise VerifierFailure
    partial_clone_keys = _run_git(
        trusted_repository,
        "config",
        "--name-only",
        "--get-regexp",
        r"^(extensions\.partialclone|remote\..*\.promisor)$",
        allowed_returncodes=(0, 1),
    )
    if partial_clone_keys:
        raise VerifierFailure
    object_format = _ascii(_run_git(trusted_repository, "rev-parse", "--show-object-format"))
    sha_length = {"sha1": 40, "sha256": 64}.get(object_format)
    if sha_length is None:
        raise VerifierFailure
    if type(base_sha) is not str or re.fullmatch(f"[0-9a-f]{{{sha_length}}}", base_sha) is None:
        raise ManifestDenied("INVALID_BASE_SHA")
    if type(manifest_id) is not str or MANIFEST_ID.fullmatch(manifest_id) is None:
        raise ManifestDenied("INVALID_MANIFEST_ID")

    def current_base() -> str:
        ref_sha = _ascii(
            _run_git(
                trusted_repository,
                "rev-parse",
                "--verify",
                "refs/remotes/origin/main",
            )
        )
        if re.fullmatch(f"[0-9a-f]{{{sha_length}}}", ref_sha) is None:
            raise VerifierFailure
        return ref_sha

    if current_base() != base_sha:
        raise ManifestDenied("BASE_NOT_CURRENT")
    resolved = _ascii(
        _run_git(
            trusted_repository,
            "rev-parse",
            "--verify",
            f"{base_sha}^{{commit}}",
        )
    )
    if resolved != base_sha:
        raise ManifestDenied("INVALID_BASE_SHA")
    tree_sha = _ascii(_run_git(trusted_repository, "rev-parse", f"{base_sha}^{{tree}}"))
    if re.fullmatch(f"[0-9a-f]{{{sha_length}}}", tree_sha) is None:
        raise VerifierFailure
    manifest_path = f"docs/overnight-manifests/{manifest_id}.json"
    entry = _run_git(
        trusted_repository, "ls-tree", "-z", "--full-tree", base_sha, "--", manifest_path
    )
    records = [record for record in entry.split(b"\0") if record]
    try:
        metadata, encoded_path = records[0].split(b"\t", maxsplit=1)
        mode, kind, blob_oid = metadata.decode("ascii").split()
    except (IndexError, UnicodeError, ValueError):
        raise ManifestDenied("MANIFEST_BLOB_INVALID") from None
    if (
        len(records) != 1
        or mode != "100644"
        or kind != "blob"
        or re.fullmatch(f"[0-9a-f]{{{sha_length}}}", blob_oid) is None
        or encoded_path != manifest_path.encode("ascii")
    ):
        raise ManifestDenied("MANIFEST_BLOB_INVALID")
    size_text = _ascii(
        _run_git(trusted_repository, "cat-file", "-s", blob_oid, maximum_output_bytes=16)
    )
    if len(size_text) > 10 or re.fullmatch(r"[0-9]+", size_text) is None:
        raise VerifierFailure
    size = int(size_text)
    if size > MAX_MANIFEST_BYTES:
        raise ManifestDenied("MANIFEST_TOO_LARGE")
    blob = _run_git(
        trusted_repository,
        "cat-file",
        "blob",
        blob_oid,
        maximum_output_bytes=MAX_MANIFEST_BYTES,
        excess_reason="MANIFEST_TOO_LARGE",
    )
    if len(blob) != size:
        raise VerifierFailure
    if _git_object_oid(object_format, "blob", blob) != blob_oid:
        raise VerifierFailure
    verified_at = datetime.now(UTC) if now is None else now
    contract = validate_manifest_blob(blob, manifest_id, verified_at)
    if current_base() != base_sha:
        raise ManifestDenied("BASE_MOVED")
    return ProtectedBaseManifest(
        contract=contract,
        base_commit_sha=base_sha,
        base_tree_sha=tree_sha,
        manifest_path=manifest_path,
        git_blob_oid=blob_oid,
        manifest_sha256=hashlib.sha256(blob).hexdigest(),
        record_key=hashlib.sha256(manifest_id.encode("ascii")).hexdigest(),
        object_format=object_format,
        verified_at=verified_at.astimezone(UTC),
    )


def _verified_object(
    repository: Path, object_format: str, kind: str, oid: str, maximum_bytes: int
) -> tuple[str, int, bytes]:
    size_text = _ascii(
        _run_git(repository, "cat-file", "-s", oid, maximum_output_bytes=16)
    )
    if len(size_text) > 10 or not size_text.isdigit():
        raise VerifierFailure
    size = int(size_text)
    if size > maximum_bytes:
        raise ManifestDenied("CANDIDATE_DIFF_INVALID")
    content = _run_git(
        repository,
        "cat-file",
        kind,
        oid,
        maximum_output_bytes=maximum_bytes,
        excess_reason="CANDIDATE_DIFF_INVALID",
    )
    if len(content) != size or _git_object_oid(object_format, kind, content) != oid:
        raise VerifierFailure
    return hashlib.sha256(content).hexdigest(), size, content


def _commit_tree_and_parents(content: bytes, sha_length: int) -> tuple[str, tuple[str, ...]]:
    header, separator, _ = content.partition(b"\n\n")
    headers = header.split(b"\n")
    if not separator or not headers or not headers[0].startswith(b"tree "):
        raise ManifestDenied("CANDIDATE_HISTORY_INVALID")
    tree = headers[0].removeprefix(b"tree ")
    parent_index = 1
    parents: list[bytes] = []
    while parent_index < len(headers) and headers[parent_index].startswith(b"parent "):
        parents.append(headers[parent_index].removeprefix(b"parent "))
        parent_index += 1
    pattern = re.compile(f"[0-9a-f]{{{sha_length}}}".encode("ascii"))
    if (
        pattern.fullmatch(tree) is None
        or tree == b"0" * sha_length
        or any(line.startswith((b"tree ", b"parent ")) for line in headers[parent_index:])
        or any(
            pattern.fullmatch(parent) is None or parent == b"0" * sha_length
            for parent in parents
        )
    ):
        raise ManifestDenied("CANDIDATE_HISTORY_INVALID")
    return tree.decode("ascii"), tuple(parent.decode("ascii") for parent in parents)


def _tree_entries(content: bytes, oid_bytes: int) -> tuple[tuple[bytes, str, str], ...]:
    if not content:
        raise ManifestDenied("CANDIDATE_DIFF_INVALID")
    offset = 0
    names: set[bytes] = set()
    previous_key: bytes | None = None
    result: list[tuple[bytes, str, str]] = []
    allowed_modes = {b"40000", b"100644", b"100755", b"120000", b"160000"}
    while offset < len(content):
        space = content.find(b" ", offset)
        nul = content.find(b"\0", space + 1)
        oid_end = nul + 1 + oid_bytes
        if space <= offset or nul <= space + 1 or oid_end > len(content):
            raise VerifierFailure
        mode = content[offset:space]
        name = content[space + 1:nul]
        raw_oid = content[nul + 1:oid_end]
        order_key = name + (b"/" if mode == b"40000" else b"\0")
        if (
            mode not in allowed_modes
            or not name
            or b"/" in name
            or name in {b".", b".."}
            or name.lower() == b".git"
            or name in names
            or raw_oid == b"\0" * oid_bytes
            or (previous_key is not None and order_key <= previous_key)
        ):
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        names.add(name)
        previous_key = order_key
        result.append((name, mode.decode("ascii"), raw_oid.hex()))
        offset = oid_end
    return tuple(result)


def _verified_tree_snapshots(
    repository: Path, object_format: str, roots: tuple[str, ...]
) -> tuple[dict[str, dict[bytes, tuple[str, str]]], tuple[tuple[str, str], ...]]:
    oid_bytes = {"sha1": 20, "sha256": 32}.get(object_format)
    if oid_bytes is None:
        raise VerifierFailure
    parsed: dict[str, tuple[tuple[bytes, str, str], ...]] = {}
    digests: dict[str, str] = {}
    total_bytes = 0
    expanded_entries = 0
    expanded_path_bytes = 0
    snapshots: dict[str, dict[bytes, tuple[str, str]]] = {}

    def entries_for(oid: str) -> tuple[tuple[bytes, str, str], ...]:
        nonlocal total_bytes
        if oid not in parsed:
            if len(parsed) >= MAX_TREE_OBJECTS:
                raise ManifestDenied("CANDIDATE_DIFF_INVALID")
            digest, size, content = _verified_object(
                repository, object_format, "tree", oid, MAX_TREE_OBJECT_BYTES
            )
            total_bytes += size
            if total_bytes > MAX_TOTAL_TREE_BYTES:
                raise ManifestDenied("CANDIDATE_DIFF_INVALID")
            parsed[oid] = _tree_entries(content, oid_bytes)
            digests[oid] = digest
        return parsed[oid]

    for root in roots:
        if root in snapshots:
            continue
        snapshot: dict[bytes, tuple[str, str]] = {}
        pending: list[tuple[str, bytes, tuple[str, ...], int]] = [(root, b"", (), 0)]
        while pending:
            oid, prefix, ancestors, depth = pending.pop()
            if oid in ancestors or depth > MAX_TREE_DEPTH:
                raise ManifestDenied("CANDIDATE_DIFF_INVALID")
            next_ancestors = (*ancestors, oid)
            for name, mode, child_oid in reversed(entries_for(oid)):
                full_path = prefix + name
                expanded_entries += 1
                expanded_path_bytes += len(full_path)
                if (
                    expanded_entries > MAX_EXPANDED_TREE_ENTRIES
                    or expanded_path_bytes > MAX_EXPANDED_TREE_PATH_BYTES
                    or len(full_path) > MAX_TREE_PATH_BYTES
                ):
                    raise ManifestDenied("CANDIDATE_DIFF_INVALID")
                if mode == "40000":
                    pending.append((child_oid, full_path + b"/", next_ancestors, depth + 1))
                elif full_path in snapshot:
                    raise ManifestDenied("CANDIDATE_DIFF_INVALID")
                else:
                    snapshot[full_path] = (mode, child_oid)
        snapshots[root] = snapshot
    return snapshots, tuple(sorted(digests.items()))


def _verified_commit_history(
    repository: Path, binding: ProtectedBaseManifest, candidate_sha: str
) -> tuple[tuple[str, ...], dict[str, str], dict[str, str]]:
    current = candidate_sha
    reverse_commits: list[str] = []
    trees: dict[str, str] = {}
    digests: dict[str, str] = {}
    while current != binding.base_commit_sha:
        if len(reverse_commits) >= binding.contract.maximum_commits:
            raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")
        digest, _, content = _verified_object(
            repository, binding.object_format, "commit", current, MAX_COMMIT_OBJECT_BYTES
        )
        tree, parents = _commit_tree_and_parents(content, len(binding.base_commit_sha))
        if len(parents) != 1:
            raise ManifestDenied("CANDIDATE_HISTORY_INVALID")
        reverse_commits.append(current)
        trees[current] = tree
        digests[current] = digest
        current = parents[0]
    if not reverse_commits:
        raise ManifestDenied("CANDIDATE_HISTORY_INVALID")
    base_digest, _, base_content = _verified_object(
        repository, binding.object_format, "commit", binding.base_commit_sha,
        MAX_COMMIT_OBJECT_BYTES,
    )
    base_tree, _ = _commit_tree_and_parents(base_content, len(binding.base_commit_sha))
    if base_tree != binding.base_tree_sha:
        raise VerifierFailure
    digests[binding.base_commit_sha] = base_digest
    return tuple(reversed(reverse_commits)), trees, digests


def _is_lfs_pointer(content: bytes) -> bool:
    if len(content) > 1024:
        return False
    lines = content.splitlines()
    return (
        len(lines) >= 3
        and re.fullmatch(rb"version https://[^\s]+/spec/v1", lines[0]) is not None
        and any(re.fullmatch(rb"oid sha256:[0-9a-f]{64}", line) for line in lines[1:])
        and any(re.fullmatch(rb"size [0-9]+", line) for line in lines[1:])
    )


def _inspect_edge(
    repository: Path,
    old_snapshot: dict[bytes, tuple[str, str]],
    new_snapshot: dict[bytes, tuple[str, str]],
    binding: ProtectedBaseManifest,
    blob_cache: dict[str, VerifiedFileContent],
) -> tuple[CommittedPathChange, ...]:
    allowed_paths = {path.encode("ascii"): path for path in binding.contract.allowed_paths}
    changes: list[CommittedPathChange] = []
    for raw_path in sorted(old_snapshot.keys() | new_snapshot.keys()):
        old = old_snapshot.get(raw_path)
        new = new_snapshot.get(raw_path)
        if old == new:
            continue
        path = allowed_paths.get(raw_path)
        if path is None or any(
            endpoint is not None and endpoint[0] != "100644" for endpoint in (old, new)
        ):
            raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        for endpoint in (old, new):
            if endpoint is None or endpoint[1] in blob_cache:
                continue
            oid = endpoint[1]
            digest, size, content = _verified_object(
                repository, binding.object_format, "blob", oid, MAX_CANDIDATE_BLOB_BYTES
            )
            try:
                content.decode("utf-8")
            except UnicodeError:
                raise ManifestDenied("CANDIDATE_DIFF_INVALID") from None
            if b"\0" in content or _is_lfs_pointer(content):
                raise ManifestDenied("CANDIDATE_DIFF_INVALID")
            blob_cache[oid] = VerifiedFileContent(endpoint[0], oid, digest, content)
            if sum(len(item.content) for item in blob_cache.values()) > MAX_CANDIDATE_TOTAL_BLOB_BYTES:
                raise ManifestDenied("CANDIDATE_DIFF_INVALID")
        changes.append(CommittedPathChange(
            path,
            None if old is None else blob_cache[old[1]],
            None if new is None else blob_cache[new[1]],
        ))
    return tuple(changes)


def verify_committed_candidate_history(
    repository: Path,
    base_sha: str,
    manifest_id: str,
    candidate_sha: str,
    *,
    now: datetime | None = None,
) -> CommittedCandidateHistory:
    binding = verify_protected_base_manifest(repository, base_sha, manifest_id, now=now)
    repository = Path(os.path.abspath(repository))
    sha_length = len(binding.base_commit_sha)
    if type(candidate_sha) is not str or re.fullmatch(
        f"[0-9a-f]{{{sha_length}}}", candidate_sha
    ) is None:
        raise ManifestDenied("INVALID_CANDIDATE_SHA")
    shallow = _ascii(_run_git(repository, "rev-parse", "--is-shallow-repository"))
    if shallow != "false":
        raise VerifierFailure
    grafts_path = Path(_ascii(_run_git(
        repository, "rev-parse", "--path-format=absolute", "--git-path", "info/grafts"
    )))
    if grafts_path.exists() or grafts_path.is_symlink():
        raise VerifierFailure
    commits, commit_trees, commit_digests = _verified_commit_history(
        repository, binding, candidate_sha
    )
    candidate_tree = commit_trees[candidate_sha]
    tree_roots = (binding.base_tree_sha, *(commit_trees[commit] for commit in commits))
    snapshots, tree_digests = _verified_tree_snapshots(
        repository, binding.object_format, tree_roots
    )

    touched_paths: set[str] = set()
    blob_cache: dict[str, VerifiedFileContent] = {}
    edges: list[CommittedHistoryEdge] = []
    previous_commit = binding.base_commit_sha
    previous_tree = binding.base_tree_sha
    for commit in commits:
        current_tree = commit_trees[commit]
        changes = _inspect_edge(
            repository, snapshots[previous_tree], snapshots[current_tree], binding, blob_cache
        )
        if not changes:
            raise ManifestDenied("CANDIDATE_HISTORY_INVALID")
        edges.append(CommittedHistoryEdge(
            previous_commit, previous_tree, commit, current_tree, changes
        ))
        touched_paths.update(change.path for change in changes)
        if len(touched_paths) > binding.contract.maximum_changed_files:
            raise ManifestDenied("CANDIDATE_BUDGET_EXCEEDED")
        previous_commit = commit
        previous_tree = current_tree
    final_changes = _inspect_edge(
        repository,
        snapshots[binding.base_tree_sha],
        snapshots[candidate_tree],
        binding,
        blob_cache,
    )
    if not final_changes:
        raise ManifestDenied("CANDIDATE_DIFF_INVALID")
    closing_commits, closing_trees, closing_commit_digests = _verified_commit_history(
        repository, binding, candidate_sha
    )
    if (
        closing_commits != commits
        or closing_trees != commit_trees
        or closing_commit_digests != commit_digests
    ):
        raise VerifierFailure
    closing_snapshots, closing_tree_digests = _verified_tree_snapshots(
        repository, binding.object_format, tree_roots
    )
    if closing_snapshots != snapshots or closing_tree_digests != tree_digests:
        raise VerifierFailure
    try:
        closing = verify_protected_base_manifest(repository, base_sha, manifest_id, now=now)
    except ManifestDenied as error:
        if error.reason == "BASE_NOT_CURRENT":
            raise ManifestDenied("BASE_MOVED") from None
        raise
    if (
        closing.base_tree_sha != binding.base_tree_sha
        or closing.manifest_sha256 != binding.manifest_sha256
        or closing.contract != binding.contract
    ):
        raise VerifierFailure
    return CommittedCandidateHistory(
        manifest=closing,
        candidate_commit_sha=candidate_sha,
        candidate_tree_sha=candidate_tree,
        commit_shas=commits,
        base_commit_sha256=commit_digests[binding.base_commit_sha],
        commit_object_sha256s=tuple(commit_digests[commit] for commit in commits),
        tree_object_sha256s=tree_digests,
        edges=tuple(edges),
        final_changes=final_changes,
        touched_paths=tuple(sorted(touched_paths)),
        final_paths=tuple(change.path for change in final_changes),
        verified_at=closing.verified_at,
    )


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, _message: str) -> NoReturn:
        raise VerifierFailure


def main(argv: list[str] | None = None) -> int:
    parser = _SafeArgumentParser(description="Verify one current origin/main manifest")
    parser.add_argument("--repository", required=True, type=Path)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--manifest-id", required=True)
    try:
        arguments = parser.parse_args(argv)
        binding = verify_protected_base_manifest(
            arguments.repository, arguments.base_sha, arguments.manifest_id
        )
    except ManifestDenied as error:
        reason = error.reason if error.reason in DENIAL_REASONS else "MANIFEST_DENIED"
        print(json.dumps({"status": "denied", "reason": reason}), file=sys.stderr)
        return 2
    except Exception:
        print('{"status":"error","reason":"VERIFIER_FAILURE"}', file=sys.stderr)
        return 1
    print(json.dumps(binding.redacted_evidence(), sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
