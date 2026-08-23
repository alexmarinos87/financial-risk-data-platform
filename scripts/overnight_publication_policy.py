from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


POLICY_PATH = Path(".github/overnight-publication-policy.json")
CI_PATH = Path(".github/workflows/ci.yml")
MAX_POLICY_BYTES = 16 * 1024
MAX_CI_BYTES = 128 * 1024

REQUIRED_STATUS_CHECKS = (
    "Security guardrails",
    "Python readiness",
    "PostgreSQL contract",
    "Infrastructure validation",
)

EXPECTED_POLICY: dict[str, Any] = {
    "schema_version": 1,
    "publication_status": "disabled",
    "authorization_expires": None,
    "protected_base": {"repository": "alexmarinos87/financial-risk-data-platform", "branch": "main"},
    "required_status_checks": list(REQUIRED_STATUS_CHECKS),
    "adapters": {
        "candidate_isolation": {"path": None, "sha256": None},
        "policy_verifier": {"path": None, "sha256": None},
        "publisher": {"path": None, "sha256": None},
    },
    "scheduled_run_capabilities": {
        "create_local_branch": False, "edit_worktree": False,
        "create_commit": False, "create_remote_branch": False,
        "update_remote_branch": False, "open_draft_pull_request": False,
    },
}

EXPECTED_TOP_LEVEL_BLOCKS = {
    "name": "name: ci",
    "on": """\
"on":
  push:
    branches:
      - main
  pull_request:""",
    "concurrency": """\
concurrency:
  group: ci-${{ github.event.pull_request.number || github.ref }}
  cancel-in-progress: true""",
    "permissions": """\
permissions:
  contents: read""",
}
EXPECTED_TOP_LEVEL_KEYS = ("name", "on", "concurrency", "permissions", "jobs")
EXPECTED_CI_SHA256 = "71ad86f5c3e418ba20a35a3d80b3966f64c1d079ba438b8a6997dd76868353c0"
EXPECTED_JOBS_SHA256 = "acad1af9966f536249ee37ac30d3c0a37cb9bcd36f4ea32034f97da8c05496f4"

ALLOWED_ACTIONS = {
    "actions/checkout": "11d5960a326750d5838078e36cf38b85af677262",
    "actions/setup-python": "a26af69be951a213d495a4c3e4e4022e16d87065",
    "azure/setup-kubectl": "776406bce94f63e41d621b960d78ee25c8b76ede",
    "hashicorp/setup-terraform": "b9cd54a3c349d3f38e8881555d616ced269862dd",
}


class _DuplicateKeyError(ValueError):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _DuplicateKeyError
        result[key] = value
    return result


def _reject_nonstandard_constant(_value: str) -> None:
    raise ValueError


def _read_control_file(path: Path, maximum_bytes: int, label: str) -> tuple[str | None, list[str]]:
    try:
        if path.is_symlink() or not path.is_file():
            return None, [f"{label} must be a regular non-symlink file"]
        if path.stat().st_size > maximum_bytes:
            return None, [f"{label} exceeds its size limit"]
        data = path.read_bytes()
        if len(data) > maximum_bytes:
            return None, [f"{label} exceeds its size limit"]
        return data.decode("utf-8"), []
    except (OSError, UnicodeError, ValueError):
        return None, [f"{label} could not be read safely"]


def _matches_exact_types(actual: Any, expected: Any) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        return actual.keys() == expected.keys() and all(
            _matches_exact_types(actual[key], value) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return len(actual) == len(expected) and all(
            _matches_exact_types(item, expected[index]) for index, item in enumerate(actual)
        )
    return actual == expected


def validate_disabled_policy_text(text: str) -> list[str]:
    try:
        policy = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_constant,
        )
    except (json.JSONDecodeError, _DuplicateKeyError, ValueError, TypeError):
        return ["overnight publication policy is not strict JSON"]

    if not _matches_exact_types(policy, EXPECTED_POLICY):
        return ["overnight publication policy must match disabled schema version 1"]
    return []


def _top_level_blocks(text: str) -> tuple[dict[str, str], bool]:
    lines = text.splitlines()
    starts: list[tuple[int, str]] = []
    pattern = re.compile(r'^(?:"(?P<quoted>[^"]+)"|(?P<plain>[A-Za-z][A-Za-z0-9_-]*)):(?:\s.*)?$')
    for index, line in enumerate(lines):
        match = pattern.fullmatch(line)
        if match:
            starts.append((index, match.group("quoted") or match.group("plain")))

    blocks: dict[str, str] = {}
    duplicate = False
    for position, (start, name) in enumerate(starts):
        end = starts[position + 1][0] if position + 1 < len(starts) else len(lines)
        if name in blocks:
            duplicate = True
        blocks[name] = "\n".join(lines[start:end]).rstrip()
    return blocks, duplicate


def validate_ci_text(text: str) -> list[str]:
    failures: list[str] = []
    if hashlib.sha256(text.encode()).hexdigest() != EXPECTED_CI_SHA256:
        failures.append("CI workflow bytes do not match the reviewed contract")
    blocks, duplicate = _top_level_blocks(text)
    if duplicate:
        failures.append("CI workflow has duplicate top-level keys")
    if tuple(blocks) != EXPECTED_TOP_LEVEL_KEYS:
        failures.append("CI workflow top-level keys are not exact")
    for name, expected in EXPECTED_TOP_LEVEL_BLOCKS.items():
        if blocks.get(name) != expected:
            failures.append(f"CI workflow {name} contract is not exact")

    jobs = blocks.get("jobs", "")
    if hashlib.sha256(jobs.encode()).hexdigest() != EXPECTED_JOBS_SHA256:
        failures.append("CI workflow job contract is not exact")
    job_names = re.findall(r"(?m)^    name: (.+)$", jobs)
    if tuple(job_names) != REQUIRED_STATUS_CHECKS or len(set(job_names)) != len(job_names):
        failures.append("CI workflow required status-check names are not exact and unique")

    action_pattern = re.compile(r"(?m)^\s*- uses:\s+([^#\s]+)(?:\s+#.*)?$")
    action_matches = list(action_pattern.finditer(text))
    if len(action_matches) != text.count("uses:"):
        failures.append("CI workflow contains an unparseable action reference")
    for match in action_matches:
        reference = match.group(1)
        if "@" not in reference:
            failures.append("CI workflow action reference is missing an immutable revision")
            continue
        action, revision = reference.rsplit("@", maxsplit=1)
        if not re.fullmatch(r"[0-9a-f]{40}", revision):
            failures.append("CI workflow action reference is not a full commit SHA")
        if ALLOWED_ACTIONS.get(action) != revision:
            failures.append("CI workflow action is not on the approved immutable allowlist")

        if action == "actions/checkout":
            next_step = re.search(r"(?m)^      - ", text[match.end() :])
            end = match.end() + next_step.start() if next_step else len(text)
            step = text[match.end() : end]
            if "\n        with:\n          persist-credentials: false" not in step:
                failures.append("CI checkout must disable persisted credentials")

    return failures


def validate_repository_controls(root: Path) -> list[str]:
    policy_text, failures = _read_control_file(
        root / POLICY_PATH, MAX_POLICY_BYTES, "overnight publication policy"
    )
    if policy_text is not None:
        failures.extend(validate_disabled_policy_text(policy_text))

    ci_text, ci_failures = _read_control_file(root / CI_PATH, MAX_CI_BYTES, "CI workflow")
    failures.extend(ci_failures)
    if ci_text is not None:
        failures.extend(validate_ci_text(ci_text))
    return failures
