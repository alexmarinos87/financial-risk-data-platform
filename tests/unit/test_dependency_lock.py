from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
LOCK_PATH = ROOT / "requirements.lock"
CI_PATH = ROOT / ".github" / "workflows" / "ci.yml"
PYPROJECT_PATH = ROOT / "pyproject.toml"
PIN_PATTERN = re.compile(r"(?P<name>[A-Za-z0-9_.-]+)==(?P<version>[A-Za-z0-9_.+!-]+)")


def _locked_packages() -> dict[str, str]:
    packages: dict[str, str] = {}
    for raw_line in LOCK_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = PIN_PATTERN.fullmatch(line)
        assert match is not None, f"dependency lock entry is not an exact pin: {line}"
        name = match.group("name").lower().replace("_", "-")
        assert name not in packages, f"dependency lock contains a duplicate package: {name}"
        packages[name] = match.group("version")
    return packages


def test_dependency_lock_contains_exact_runtime_and_validation_pins() -> None:
    packages = _locked_packages()

    assert {
        "duckdb",
        "mypy",
        "numpy",
        "pandas",
        "psycopg",
        "pydantic",
        "pytest",
        "pyyaml",
        "ruff",
        "types-pyyaml",
    }.issubset(packages)


def test_ci_local_setup_and_container_use_the_same_constraints_file() -> None:
    ci = CI_PATH.read_text(encoding="utf-8")
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

    assert "PIP_CONSTRAINT=requirements.lock python -m pip install -e '.[dev]'" in ci
    assert "LOCK_FILE ?= requirements.lock" in makefile
    assert "PIP_CONSTRAINT=$(LOCK_FILE) $(PIP) install -e '.[dev]'" in makefile
    assert "PIP_CONSTRAINT=requirements.lock python -m pip install ." in dockerfile


def test_python_version_contract_matches_ci() -> None:
    ci = CI_PATH.read_text(encoding="utf-8")
    pyproject = PYPROJECT_PATH.read_text(encoding="utf-8")

    assert 'python-version: "3.11"' in ci
    assert 'requires-python = ">=3.11"' in pyproject
    assert 'python_version = "3.11"' in pyproject
