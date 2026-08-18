from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.overnight_publication_policy import (
    CI_PATH,
    EXPECTED_POLICY,
    MAX_POLICY_BYTES,
    POLICY_PATH,
    validate_ci_text,
    validate_disabled_policy_text,
    validate_repository_controls,
)


ROOT = Path(__file__).resolve().parents[2]


def _policy_text(policy: object = EXPECTED_POLICY) -> str:
    return json.dumps(policy)


def _changed_policy(*path: str, value: object) -> dict[str, object]:
    policy = json.loads(_policy_text())
    target = policy
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return policy


def _copy_controls(tmp_path: Path) -> None:
    for relative_path in (POLICY_PATH, CI_PATH):
        destination = tmp_path / relative_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes((ROOT / relative_path).read_bytes())


def test_repository_controls_accept_canonical_disabled_state() -> None:
    assert validate_repository_controls(ROOT) == []


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("publication_status",), "enabled"),
        (("publication_status",), "DISABLED"),
        (("publication_status",), None),
        (("publication_status",), False),
        (("authorization_expires",), "2026-08-16T07:00:00Z"),
        (("schema_version",), True),
        (("scheduled_run_capabilities", "create_commit"), 0),
        (("scheduled_run_capabilities", "create_commit"), 0.0),
        (("adapters", "candidate_isolation", "path"), "scripts/isolate.py"),
        (("adapters", "candidate_isolation", "sha256"), "0" * 64),
        (("adapters", "policy_verifier", "path"), "scripts/verify.py"),
        (("adapters", "policy_verifier", "sha256"), "0" * 64),
        (("adapters", "publisher", "path"), "scripts/publisher.py"),
        (("adapters", "publisher", "sha256"), "0" * 64),
        (("scheduled_run_capabilities", "create_local_branch"), True),
        (("scheduled_run_capabilities", "edit_worktree"), True),
        (("scheduled_run_capabilities", "create_commit"), True),
        (("scheduled_run_capabilities", "create_remote_branch"), True),
        (("scheduled_run_capabilities", "update_remote_branch"), True),
        (("scheduled_run_capabilities", "open_draft_pull_request"), True),
    ],
)
def test_disabled_policy_rejects_activation_data(path: tuple[str, ...], value: object) -> None:
    assert validate_disabled_policy_text(_policy_text(_changed_policy(*path, value=value)))


@pytest.mark.parametrize(
    "text",
    [
        "",
        "not json",
        "[]",
        '{"publication_status":"disabled","publication_status":"disabled"}',
        '{"schema_version": NaN}',
    ],
)
def test_disabled_policy_rejects_malformed_or_wrong_shaped_json(text: str) -> None:
    assert validate_disabled_policy_text(text)


def test_disabled_policy_rejects_unknown_and_missing_fields() -> None:
    unknown = json.loads(_policy_text())
    unknown["override"] = False
    missing = json.loads(_policy_text())
    del missing["authorization_expires"]

    assert validate_disabled_policy_text(_policy_text(unknown))
    assert validate_disabled_policy_text(_policy_text(missing))


def test_repository_controls_reject_missing_symlinked_and_oversized_policy(
    tmp_path: Path,
) -> None:
    _copy_controls(tmp_path)
    policy_path = tmp_path / POLICY_PATH

    policy_path.unlink()
    assert validate_repository_controls(tmp_path)

    policy_path.symlink_to(ROOT / POLICY_PATH)
    assert validate_repository_controls(tmp_path)

    policy_path.unlink()
    policy_path.write_text(" " * (MAX_POLICY_BYTES + 1), encoding="utf-8")
    assert validate_repository_controls(tmp_path)

    policy_path.write_bytes((ROOT / POLICY_PATH).read_bytes())
    ci_path = tmp_path / CI_PATH
    ci_path.write_bytes(ci_path.read_bytes().replace(b"\n", b"\r\n"))
    assert validate_repository_controls(tmp_path)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("      - main", "      - '*'"),
        ("  pull_request:", "  pull_request_target:"),
        ("  cancel-in-progress: true", "  cancel-in-progress: false"),
        ("@11d5960a326750d5838078e36cf38b85af677262", "@v4"),
        (
            "@11d5960a326750d5838078e36cf38b85af677262",
            "@${{ github.event.after }}",
        ),
        (
            "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
            "example/checkout@11d5960a326750d5838078e36cf38b85af677262",
        ),
        ("          persist-credentials: false", "          persist-credentials: true"),
        ("  contents: read", "  contents: write"),
        ("    name: Python readiness", "    name: Security guardrails"),
        (
            "      - run: PYTHON=python make security-check",
            "      - run: true",
        ),
        (
            "      - run: PYTHON=python make security-check",
            "      - run: PYTHON=python make security-check\n        continue-on-error: true",
        ),
        (
            "    runs-on: ubuntu-latest",
            "    permissions: contents: write\n    runs-on: ubuntu-latest",
        ),
        (
            "      - run: PYTHON=python make readiness-check",
            "      - if: false\n      - run: PYTHON=python make readiness-check",
        ),
        ('"on":', '"on":\n"o\\u006e":\n  pull_request_target:'),
        ("permissions:", 'permissions:\n"per\\u006dissions": write-all'),
        ("      - uses:", '      - "uses":'),
    ],
)
def test_ci_contract_rejects_security_regressions(old: str, new: str) -> None:
    canonical = (ROOT / CI_PATH).read_text(encoding="utf-8")
    assert old in canonical

    assert validate_ci_text(canonical.replace(old, new, 1))
