from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Any


MAX_MANIFEST_BYTES = 32 * 1024
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
