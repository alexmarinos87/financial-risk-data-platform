from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SRC_COMMAND = ["-m", "mypy", "--package", "src"]
REVIEW_COMMAND = [
    "-m", "mypy", "--module", "scripts.pr_check_evidence",
    "--module", "scripts.pr_stack_review",
]


@pytest.mark.parametrize("failure", ["none", "src", "review"])
def test_make_type_gate_runs_both_commands_and_propagates_failure(
    tmp_path: Path, failure: str,
) -> None:
    """Exercise real Make control flow, not the actual type checker."""
    trace = tmp_path / "invocations.jsonl"
    shim = tmp_path / "python_shim.py"
    shim.write_text(
        "import json, os, sys\n"
        "from pathlib import Path\n"
        "with Path(os.environ['TYPE_GATE_TRACE']).open('a') as output:\n"
        "    output.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "stage = 'src' if '--package' in sys.argv else 'review'\n"
        "sys.exit(17 if os.environ['TYPE_GATE_FAIL'] == stage else 0)\n",
        encoding="utf-8",
    )
    environment = {**os.environ, "TYPE_GATE_TRACE": str(trace), "TYPE_GATE_FAIL": failure}
    for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEOVERRIDES"):
        environment.pop(name, None)
    before = (ROOT / "Makefile").read_bytes()
    result = subprocess.run(
        ["make", "--no-print-directory", "type-check",
         f"PYTHON={shlex.quote(sys.executable)} {shlex.quote(str(shim))}"],
        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=15, check=False,
    )
    calls = [json.loads(line) for line in trace.read_text().splitlines()]
    assert calls == ([SRC_COMMAND] if failure == "src" else [SRC_COMMAND, REVIEW_COMMAND])
    assert (result.returncode == 0) is (failure == "none")
    assert (ROOT / "Makefile").read_bytes() == before


@pytest.mark.parametrize("target", ["quality-check", "readiness-check"])
def test_existing_validation_targets_include_direct_review_script_typing(target: str) -> None:
    environment = dict(os.environ)
    for name in ("MAKEFLAGS", "MFLAGS", "GNUMAKEFLAGS", "MAKEOVERRIDES"):
        environment.pop(name, None)
    result = subprocess.run(
        ["make", "--no-print-directory", "--just-print", target, "PYTHON=python"],
        cwd=ROOT, env=environment, capture_output=True, text=True, timeout=15, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "python " + " ".join(SRC_COMMAND) in result.stdout
    assert "python " + " ".join(REVIEW_COMMAND) in result.stdout
    assert "python -m ruff check ." in result.stdout
    assert "python -m pytest -q" in result.stdout
    assert "python -m pip check" in result.stdout
